WITH
    server_settings_outside_source AS
    (
        SELECT
            -- `\n\\s*` (not `\\s+`) so the blank line after a `## heading` is
            -- optional: Docusaurus source had one, migrated Mintlify pages do not.
            arrayJoin(extractAllGroups(raw_blob, '## (\\w+)(?:\\s[^\n]+)?\n\\s*((?:[^#]|#[^#]|##[^ ])+)')) AS g,
            g[1] AS name,
            replaceRegexpAll(
                    replaceRegexpAll(g[2], '\n(Type|Default( value)?): [^\n]+\n', ''), '^\n+|\n+$','') AS doc
        FROM file('_server_settings_outside_source.md', RawBLOB)),
    server_settings_in_source AS
    (
        SELECT
            name,
            replaceRegexpAll(description, '(?m)^[ \t]+', '') AS description,
            type,
            default,
            multiIf(
                changeable_without_restart = 'IncreaseOnly', 'Increase only',
                changeable_without_restart = 'DecreaseOnly', 'Decrease only',
                CAST(changeable_without_restart, 'String')
            ) AS changeable_without_restart
        FROM system.server_settings
    ),
    combined_server_settings AS
    (
        SELECT
            name,
            description,
            type,
            default,
            changeable_without_restart
        FROM server_settings_in_source
        UNION ALL
        SELECT
            name,
            doc AS description,
            ''  AS type,
            '' AS default,
            '' AS changeable_without_restart
        FROM server_settings_outside_source
    ),
    formatted_settings AS
    (
        SELECT
        format(
            '## {}{}{}{}\n\n',
            name,
            lcase(' {#'|| name ||'} \n\n'),
            if(type != '' AND default != '',
                format('<SettingsInfoBlock type="{}" default_value="{}"{} />',
                    type,
                    default,
                    if(changeable_without_restart != '', format(' changeable_without_restart="{}"', changeable_without_restart), '')
                ),
                ''
            ),
            description
        ) AS formatted_text
        FROM combined_server_settings
        ORDER BY name ASC
    ),
'---
description: ''This section contains descriptions of server settings i.e settings
which cannot be changed at the session or query level.''
keywords: [''global server settings'']
sidebar_label: ''Server Settings''
sidebar_position: 57
slug: /operations/server-configuration-parameters/settings
title: ''Server Settings''
doc_type: ''reference''
---

import Tabs from ''@theme/Tabs'';
import TabItem from ''@theme/TabItem'';
import SystemLogParameters from ''@site/docs/operations/server-configuration-parameters/_snippets/_system-log-parameters.md'';
import SettingsInfoBlock from ''@theme/SettingsInfoBlock/SettingsInfoBlock'';

# Server Settings

This section contains descriptions of server settings. These are settings which
cannot be changed at the session or query level.

For more information on configuration files in ClickHouse see [""Configuration Files""](/operations/configuration-files).

Other settings are described in the ""[Settings](/operations/settings/overview)"" section.
Before studying the settings, we recommend reading the [Configuration files](/operations/configuration-files)
section and note the use of substitutions (the `incl` and `optional` attributes).

' AS prefix_content
SELECT
    arrayStringConcat(
        [
            prefix_content,
            arrayStringConcat(groupArray(formatted_text),'')
        ],
        ''
    )
FROM formatted_settings
INTO OUTFILE 'server_settings.md'
TRUNCATE FORMAT LineAsString
