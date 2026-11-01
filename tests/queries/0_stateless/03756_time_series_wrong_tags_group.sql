SELECT timeSeriesTagsGroupToTags(toUInt64(1)); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesTagsGroupToTags(toUInt64(-1)); -- { serverError BAD_ARGUMENTS }

-- Group 0 is actually valid - it means an empty set of tags.
SELECT timeSeriesTagsGroupToTags(toUInt64(0));
