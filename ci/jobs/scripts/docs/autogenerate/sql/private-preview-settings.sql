-- First write the header
SELECT '\n\n## Private preview settings {#private-preview-settings}\n\n| Name | Default |\n|------|--------|'
INTO OUTFILE 'experimental-beta-settings.md' APPEND
FORMAT TSVRaw;

-- Then append the table content
WITH
    private_preview_session_settings AS
        (
            SELECT
                format('[{}](/operations/settings/settings#{})', name, name) AS Name,
                format('`{}`', default) AS Default
FROM system.settings
WHERE tier = 'PrivatePreview' AND alias_for=''
    ),
    private_preview_mergetree_settings AS
    (
SELECT
    format('[{}](/operations/settings/merge-tree-settings#{})', name, name) AS Name,
    format('`{}`', default) AS Default
FROM system.merge_tree_settings
WHERE tier = 'PrivatePreview'
    ),
    combined AS
    (
SELECT *
FROM private_preview_session_settings
UNION ALL
SELECT *
FROM private_preview_mergetree_settings
ORDER BY Name ASC
    )
SELECT concat('| ', Name, ' | ', Default, ' |')
FROM combined
    INTO OUTFILE 'experimental-beta-settings.md' APPEND
FORMAT TSVRaw;
