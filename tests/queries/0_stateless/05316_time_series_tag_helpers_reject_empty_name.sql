-- Public time series tag helpers reject an empty tag name.

SELECT timeSeriesExtractTag(toUInt64(0), ''); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRemoveTag(toUInt64(0), ''); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesRemoveTags(toUInt64(0), ['']); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesReplaceTag(toUInt64(0), '', 'value', 'job', '.*'); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesJoinTags(toUInt64(0), '', '-', ['job']); -- { serverError BAD_ARGUMENTS }
SELECT timeSeriesCopyTag(toUInt64(0), toUInt64(0), ''); -- { serverError BAD_ARGUMENTS }
