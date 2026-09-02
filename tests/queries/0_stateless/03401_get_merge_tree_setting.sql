SELECT
    toBool(t1.val = t2.val) AS should_be_equal
FROM
    (SELECT toString(value) AS val FROM system.merge_tree_settings WHERE name = 'index_granularity') AS t1,
    (SELECT toString(getMergeTreeSetting('index_granularity')) AS val) AS t2;

SELECT
    toBool(t1.val = t2.val) AS should_be_equal
FROM
    (SELECT toString(value) AS val FROM system.merge_tree_settings WHERE name = 'max_merge_selecting_sleep_ms') AS t1,
    (SELECT toString(getMergeTreeSetting('max_merge_selecting_sleep_ms')) AS val) AS t2;

SELECT ('TEST INVALID ARGUMENTS');

SELECT getMergeTreeSetting(); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }

SELECT getMergeTreeSetting(10); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

SELECT getMergeTreeSetting('index_granularity')(4096); -- { serverError FUNCTION_CANNOT_HAVE_PARAMETERS }

SELECT getMergeTreeSetting('keeper_multiread_batch_size'); -- { serverError UNKNOWN_SETTING }

SELECT getMergeTreeSetting('index_granularity', 'marks_compression_codec'); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
