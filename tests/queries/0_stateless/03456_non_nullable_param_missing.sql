-- Missing non-nullable parameter must raise UNKNOWN_QUERY_PARAMETER
SELECT {p:UInt8} -- { serverError UNKNOWN_QUERY_PARAMETER }
