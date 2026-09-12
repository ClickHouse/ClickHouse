--https://github.com/ClickHouse/ClickHouse/issues/92582
SELECT tokens('mX.\fk', groupBitXor(NULL)), []; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }