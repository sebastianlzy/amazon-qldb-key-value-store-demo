const QLDBKVS = require("amazon-qldb-kvs-nodejs").QLDBKVS;
const LEDGER_NAME = "Assets";
const TABLE_NAME = "CarInventories";
const DOC_OBJECT_KEY1 = "Toyota Wish";
const DOC_OBJECT_VALUE1 = {
    text: "quantity",
    number: 3
};
const DOC_OBJECT_KEY2 = "Toyota Harrier";
const DOC_OBJECT_VALUE2 = {
    text: "quantity",
    number: 2
};

const createQLDBLedger = async (checkForTable = true) => {
    const qldbKVS = new QLDBKVS(LEDGER_NAME, TABLE_NAME, checkForTable);

    const setValue = async (key, value) => {
        const response = await qldbKVS.setValue(key, value);
        if (response) {
            console.log(`Internal document Id from the Ledger, returned by setValue: ${JSON.stringify(response)}`);
            return response
        } else {
            console.log(`Could not set value for key "${key}"`);
        }
    }

    const setValues = async (keys, values) => {
        const responses = await qldbKVS.setValues(keys, values);

        if (responses) {
            console.log(`Internal document Id from the Ledger, returned by setValues: ${JSON.stringify(responses)}`);
            return responses
        } else {
            console.log(`Could not set value for keys "[${keys}, ${values}]"`);
        }
    }

    const getValue = async (key) => {
        const valueFromLedger = await qldbKVS.getValue(key);

        if (valueFromLedger) {
            console.log(`Value from Ledger: ${JSON.stringify(valueFromLedger)}`);
            return valueFromLedger
        } else {
            console.log(`Value for key "${key}" is not found.`);
        }
    }

    const getValues = async (keys) => {
        const valuesFromLedger = await qldbKVS.getValues(keys);

        if (valuesFromLedger) {
            console.log(`Value from Ledger: ${JSON.stringify(valuesFromLedger)}`);
            return valuesFromLedger
        } else {
            console.log(`Values for keys "${keys}" is not found.`);
        }
    }

    const getMetadata = async (key) => {
        const metadata = await qldbKVS.getMetadata(key);
        if (metadata) {
            console.log(`Metadata for verifying document with Key "${key}": ${JSON.stringify(metadata)}`);
            return metadata
        } else {
            console.log(`Metadata for key "${key}" not found.`);
        }
    }

    const getMetaDataByDocumentIdAndTxId = async (documentId, txId) => {
        // Alternatively, you can get the metadata for a specific version of the document by document Id and Transaction Id that // you get from the response object when creating or updating it:
        const metadataFromIds = await qldbKVS.getMetadataByDocIdAndTxId(documentId, txId);
        if (metadataFromIds) {
            console.log(`Metadata for verifying document with Document ID "${documentId}" and transaction Id ${txId} : ${JSON.stringify(metadataFromIds)}`);
            return metadataFromIds
        } else {
            console.log(`Metadata for key "${DOC_OBJECT_KEY1}" not found.`);
        }
    }

    const verifyLedgerMetadata = async (metadata) => {
        const isValid = await qldbKVS.verifyLedgerMetadata(metadata);
        if (isValid) {
            console.log(`Metadata for document with is valid.`);
            return isValid
        } else {
            console.log(`Metadata is not valid.`);
        }
    }

    const getHistory = async (key) => {
        const histories = await qldbKVS.getHistory(key);

        if (histories) {
            histories.forEach((history) => {
                console.log("--------------------index.js-getHistory-94-histories----------------------------")
                console.log(`History for document with Key "${key}": ${JSON.stringify(history)}`);
                console.log("--------------------index.js-getHistory-94-histories---------------------------")
            })

            return histories
        } else {
            console.log(`History for document with Key "${key}" is not found.`);
        }
    }

    const getDocumentRevisionByLedgerMetadata = async (metadata) => {
        const documentRevision = await qldbKVS.getDocumentRevisionByLedgerMetadata(metadata);
        if (documentRevision) {
            console.log(`Document revision for metadata "${JSON.stringify(metadata)}": ${JSON.stringify(documentRevision)}`);
            return documentRevision
        } else {
            console.log(`Document revision for metadata "${JSON.stringify(metadata)} is not found.`);

        }
    }
    const verifyDocumentRevisionHash = async (documentRevision) => {
        const isValid = await qldbKVS.verifyDocumentRevisionHash(documentRevision);
        if (isValid) {
            console.log(`Document revision hash is valid`);
            return isValid
        } else {
            console.log(`Document revision hash is not valid`);
        }
    }

    return {
        setValue,
        setValues,
        getValue,
        getValues,
        getMetadata,
        getMetaDataByDocumentIdAndTxId,
        verifyLedgerMetadata,
        getHistory,
        getDocumentRevisionByLedgerMetadata,
        verifyDocumentRevisionHash
    }
}




const main = async () => {
    try {
        // Creating table
        const qldbKVS = await createQLDBLedger(true)

        //Insert records
        // const response = await qldbKVS.setValue(DOC_OBJECT_KEY1, DOC_OBJECT_VALUE1)
        // const responses = await qldbKVS.setValues([DOC_OBJECT_KEY1, DOC_OBJECT_KEY2], [DOC_OBJECT_VALUE1, DOC_OBJECT_VALUE2])

        //Get records
        // await qldbKVS.getValue(DOC_OBJECT_KEY1);
        // await qldbKVS.getValues([DOC_OBJECT_KEY1, DOC_OBJECT_KEY2]);

        //Get meta data
        // const metadata = await qldbKVS.getMetadata(DOC_OBJECT_KEY2);
        // await qldbKVS.getMetaDataByDocumentIdAndTxId(responses[0].documentId, responses[0].txId)

        //Verify metadata
        // await qldbKVS.verifyLedgerMetadata(metadata)

        //Get history
        await qldbKVS.getHistory(DOC_OBJECT_KEY1)

    } catch (e) {
        console.error(e)
    }

}

(async () => await main())()