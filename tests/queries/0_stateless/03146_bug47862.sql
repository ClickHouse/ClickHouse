SELECT toInt64(lookup_res) AS cast_res
FROM (
    SELECT
        indexOf(field_id, 10) AS val_idx,
        ['110'][val_idx] AS lookup_res
    FROM (
        SELECT arrayJoin([[10], [15]]) AS field_id
    )
    WHERE val_idx != 0
)
WHERE cast_res > 0
SETTINGS enable_analyzer = 1;
