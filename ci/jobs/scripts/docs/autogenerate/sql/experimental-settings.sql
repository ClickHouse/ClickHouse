-- First write the header
SELECT '\n\n## Experimental settings {#experimental-settings}\n\n| Name | Default |\n|------|--------|'
INTO OUTFILE 'experimental-beta-settings.md' APPEND
FORMAT TSVRaw;

-- Then append the table content
WITH
    experimental_session_settings AS
        (
            SELECT
                format('[{}](/operations/settings/settings#{})', name, name) AS Name,
                format('`{}`', default) AS Default
FROM system.settings
WHERE tier = 'Experimental' AND alias_for='' AND NOT name LIKE 'input_format_parquet_use_native_reader_v3'
    ),
    experimental_mergetree_settings AS
    (
SELECT
    format('[{}](/operations/settings/merge-tree-settings#{})', name, name) AS Name,
    format('`{}`', default) AS Default
FROM system.merge_tree_settings
WHERE tier = 'Experimental'
    ),
    combined AS
    (
SELECT *
FROM experimental_session_settings
UNION ALL
SELECT *
FROM experimental_mergetree_settings
ORDER BY Name ASC
    )
SELECT concat('| ', Name, ' | ', Default, ' |')
FROM combined
    INTO OUTFILE 'experimental-beta-settings.md' APPEND
FORMAT TSVRaw;
