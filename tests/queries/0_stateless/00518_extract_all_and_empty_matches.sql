SELECT
    '{"a":"1","b":"2","c":"","d":"4"}' AS json,
    extractAll(json, '"([^"]*)":') AS keys,
    extractAll(json, ':"([^"]*)"') AS values;
