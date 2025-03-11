SET send_logs_level='fatal';

-- max_bytes_ratio_before_external_group_by = nan - regarding fix in Aggregator.cpp, 
-- max_bytes_ratio_before_external_sort = nan - regarding fix in SortingStep.cpp
SELECT uniqExact(CAST(number, 'String'))
FROM numbers(1000.)
GROUP BY CAST(number % 2, 'String')
ORDER BY uniqExact(CAST(number, 'String')) ASC NULLS FIRST
SETTINGS max_bytes_ratio_before_external_group_by = nan, max_bytes_ratio_before_external_sort = nan;
