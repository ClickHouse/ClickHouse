-- https://github.com/ClickHouse/ClickHouse/issues/43247
SELECT finalizeAggregation(CAST('AggregateFunction(categoricalInformationValue, Nullable(UInt8), UInt8)AggregateFunction(categoricalInformationValue, Nullable(UInt8), UInt8)',
                           'AggregateFunction(min, String)')); -- { serverError 131 }
