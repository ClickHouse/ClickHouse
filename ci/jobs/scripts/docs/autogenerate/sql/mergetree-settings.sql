WITH
    settings_changes AS
    (
        SELECT
            setting_name AS name,
            '[' ||
            arrayStringConcat(
                arrayMap(
                    (i, x) -> '{' ||
                        '"id": "row-' || toString(i) || '",' ||
                        '"items": [' ||
                            '{"label": ' || toJSONString(toString(x.1)) || '},' ||
                            '{"label": ' || toJSONString(toString(x.2)) || '},' ||
                            '{"label": ' || toJSONString(toString(x.3)) || '}' ||
                        ']' ||
                    '}',
                    arrayEnumerate(groupArray((version, default_value, comment))),
                    groupArray((version, default_value, comment))
                ),
            ', '
        ) ||
        ']' AS rows
        FROM
        (
            SELECT
                (arrayJoin(changes) AS change).1 AS setting_name,
                version,
                change.3 AS default_value,
                change.4 AS comment
        FROM system.settings_changes
        WHERE type = 'MergeTree'
        ORDER BY setting_name, version DESC
        )
        GROUP BY setting_name
    ),
    combined_changes_and_merge_tree_settings AS
    (
        SELECT
            a.*,
            b.*
        FROM system.merge_tree_settings AS a
        LEFT OUTER JOIN settings_changes as b
        ON a.name = b.name
    ),
    merge_tree_settings AS
    (
        SELECT format(
           '## {} {} \n{}{}\n{}{}',
           name,
           '{#'||name||'}',
           multiIf(tier == 'Experimental', '\n<ExperimentalBadge/>\n', tier == 'Beta', '\n<BetaBadge/>\n', ''),
           if(type != '' AND default != '', format('<SettingsInfoBlock type="{}" default_value="{}" />', type, default), ''),
           if(rows != '', printf('<VersionHistory rows={%s}/>\n\n', rows), ''),
           replaceRegexpAll(description, '(?m)(^[ \t]+|[ \t]+$)', '')
        )
        FROM combined_changes_and_merge_tree_settings ORDER BY name
    )
SELECT * FROM merge_tree_settings
INTO OUTFILE 'generated_merge_tree_settings.md' TRUNCATE FORMAT LineAsString
