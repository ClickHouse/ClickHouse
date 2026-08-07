-- Tags: no-fasttest
-- Tag no-fasttest: Depends on base64Decode

SELECT quantileDDMerge(0.01)(state) != 0 FROM format(
    RowBinary,
    'state AggregateFunction(quantileDD(0.01), Float64)',
    base64Decode('AgAAAAAAAABAAAAAAAAAAAABDAEPAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAAAs07f2aeoPY/AAAAAAAAAAABDAEjAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA')
)
