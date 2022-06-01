SELECT JSONExtractKeysAndValuesRaw(arrayJoin([])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT JSONHas(arrayJoin([]));  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT isValidJSON(arrayJoin([]));  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
