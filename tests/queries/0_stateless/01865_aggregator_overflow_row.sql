SELECT uniqCombined(number)
FROM numbers(10000)
GROUP BY number
    WITH TOTALS
ORDER BY number DESC
LIMIT 10
SETTINGS
    /* force aggregates serialization to trigger the issue with */
    max_bytes_before_external_group_by=1,
    max_bytes_ratio_before_external_group_by=0,
    /* overflow row: */
    max_rows_to_group_by=10000000000,
    group_by_overflow_mode='any',
    totals_mode='before_having',
    /* this is to account memory under 4MB (for max_bytes_before_external_group_by) to use less rows */
    max_untracked_memory=0,
    group_by_two_level_threshold=10000,
    /* explicitly */
    max_block_size=1000,
    max_threads=1
;
