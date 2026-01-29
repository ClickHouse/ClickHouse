SELECT timeSeriesTagsGroupToTags(toUInt64(0)); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesTagsGroupToTags(toUInt64(1)); -- { serverError BAD_ARGUMENTS }
