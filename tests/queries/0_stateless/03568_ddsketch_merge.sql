SELECT quantileDDMerge(0.01)(state) != 0 FROM (
  SELECT * FROM format(RowBinary, 'state AggregateFunction(quantileDD(0.01), Float64)', base64Decode('AgAAAAAAAABAAAAAAAAAAAABDAEPAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA'))
  UNION ALL
  SELECT * FROM format(RowBinary, 'state AggregateFunction(quantileDD(0.01), Float64)', base64Decode('As07f2aeoPY/AAAAAAAAAAABDAEjAgAAAAAAAPA/AwwA/v///w8CBAAAAAAAAAAA'))
)
