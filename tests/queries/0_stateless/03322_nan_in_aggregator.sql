SET send_logs_level='error';

SELECT uniqExact(CAST(number, 'String'))
FROM numbers(1000.)
GROUP BY CAST(number % 2, 'String')
SETTINGS max_bytes_ratio_before_external_group_by = nan;
