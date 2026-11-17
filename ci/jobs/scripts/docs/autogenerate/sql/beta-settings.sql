-- First write the header
SELECT '## Beta settings {#beta-settings}\n\n| Name | Default |\n|------|--------|'
INTO OUTFILE 'experimental-beta-settings.md' APPEND
FORMAT TSVRaw;

-- Then append the table content
WITH
    beta_session_settings AS
        (
            SELECT
                format('[{}](/operations/settings/settings#{})', name, name) AS Name,
                format('`{}`', ifNull(default, ' ')) AS Default
FROM system.settings
WHERE tier = 'Beta'
AND alias_for=''
AND NOT (name LIKE 'input_format_parquet_use_native_reader_v3')),
    beta_mergetree_settings AS
    (
SELECT
    format('[{}](/operations/settings/merge-tree-settings#{})', name, name) AS Name,
    format('`{}`', default) AS Default
FROM system.merge_tree_settings
WHERE tier = 'Beta'
    ),
    combined AS
    (
SELECT *
FROM beta_session_settings
UNION ALL
SELECT *
FROM beta_mergetree_settings
ORDER BY Name ASC
    )
SELECT concat('| ', Name, ' | ', Default, ' |')
FROM combined
    INTO OUTFILE 'experimental-beta-settings.md' APPEND
FORMAT TSVRaw;
