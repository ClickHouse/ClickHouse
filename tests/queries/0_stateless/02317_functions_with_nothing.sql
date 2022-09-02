SELECT JSONExtractKeysAndValuesRaw(arrayJoin([])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT JSONHas(arrayJoin([]));  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT isValidJSON(arrayJoin([]));  -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT concat(arrayJoin([]), arrayJoin([NULL, ''])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT plus(arrayJoin([]), arrayJoin([NULL, 1])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT sipHash64(arrayJoin([]), [NULL], arrayJoin(['', NULL, '', NULL])); -- {serverError ILLEGAL_TYPE_OF_ARGUMENT}
SELECT [concat(NULL, arrayJoin([]))];
