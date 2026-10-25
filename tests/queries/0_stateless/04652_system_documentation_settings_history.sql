-- The documentation of a setting in `system.documentation` (the source of the built-in `/docs` Web UI and of the
-- `help` command) carries the history of the changes of its default value: the version in which the setting was
-- introduced and every later change of the default, with the reason for the change. The history is the same data
-- that backs the `compatibility` setting and `system.settings_changes`.

-- Every recorded change, with the name it is recorded under resolved to the setting it belongs to, the way
-- `compatibility` resolves it: a change recorded under an alias of a setting belongs to that setting. The
-- exception is a record written under an alias for the sole purpose of registering that alias: it is the history
-- of the alias alone, because it neither introduces the setting it aliases nor changes its default.
-- Every recorded change, with the name it is recorded under and the setting that name resolves to.
CREATE VIEW session_records AS
SELECT
    ch.recorded_name AS recorded_name,
    if(s.alias_for != '', s.alias_for, ch.recorded_name) AS setting,
    ch.version AS version,
    ch.previous_value AS previous_value,
    ch.new_value AS new_value,
    ch.reason AS reason
FROM
(
    SELECT
        version,
        tupleElement(arrayJoin(changes) AS c, 'name') AS recorded_name,
        tupleElement(c, 'previous_value') AS previous_value,
        tupleElement(c, 'new_value') AS new_value,
        tupleElement(c, 'reason') AS reason
    FROM system.settings_changes WHERE type = 'Session'
) AS ch
INNER JOIN system.settings AS s ON s.name = ch.recorded_name;

-- A rename keeps the old name as an alias only sometimes; when it does not, the alias of a setting no longer leads
-- to the name it used to have, and the only account of the rename is the reason of a record — written either under
-- the new name ("Rename of `x`") or under the old one ("Obsolete setting, renamed to `x`"). The pairs of names of
-- such a rename, with the ones that the aliases already cover excluded. A setting renamed twice would need this
-- applied transitively; there is no such case today.
CREATE VIEW session_renames AS
SELECT DISTINCT previous_name, current_name FROM
(
    SELECT
        p.previous_name AS previous_name,
        if(cur.alias_for != '', cur.alias_for, p.current_name) AS current_name,
        if(prev.alias_for != '', prev.alias_for, p.previous_name) AS previous_setting
    FROM
    (
        SELECT
            extract(reason, '(?i)(?:rename of(?: setting)?|renamed from|new name of|previous name is)[^0-9A-Za-z_]*([0-9A-Za-z_]+)') AS previous_name,
            setting AS current_name
        FROM session_records
        UNION ALL
        SELECT
            recorded_name AS previous_name,
            extract(reason, '(?i)renamed to[^0-9A-Za-z_]*([0-9A-Za-z_]+)') AS current_name
        FROM session_records
    ) AS p
    LEFT JOIN system.settings AS cur ON cur.name = p.current_name
    LEFT JOIN system.settings AS prev ON prev.name = p.previous_name
    WHERE p.previous_name != '' AND p.current_name != ''
)
WHERE previous_setting != current_name;

-- The history of a setting is the history of every name it has, including the names it no longer has: a record
-- written under a name the setting was renamed from is history of the setting under the name it has today as well.
-- It stays under the old name too, for a name that is still a setting of its own — an obsolete setting, say.
CREATE VIEW own_name_changes AS
SELECT setting AS name, recorded_name, version, previous_value, new_value, reason
FROM session_records
WHERE NOT (recorded_name != setting AND previous_value = new_value AND match(reason,
    '(?i)\\b(?:add\\w*|new|introduc\\w*)\\b[^.]{0,20}\\balias\\b|(?:^|[.;]\\s+)(?:an?\\s+)?alias\\s+(?:for|of|to)\\b'));

CREATE VIEW session_changes AS
SELECT name, recorded_name, version, previous_value, new_value, reason FROM own_name_changes
UNION ALL
SELECT r.current_name AS name, recorded_name, version, previous_value, new_value, reason
FROM own_name_changes AS c INNER JOIN session_renames AS r ON r.previous_name = c.name;

-- The history of an alias is the history of that name as opposed to the history of the setting it resolves to:
-- every record written under the alias itself, plus the records written under another name of the same setting
-- that register this one as an alias — a record that names the alias and says either that an alias is being added
-- or that the setting is being renamed (which is how the file words keeping the old name). Such a record may
-- change the default value in the same breath, so it is not required to leave it as it was.
CREATE VIEW alias_registrations AS
SELECT a.name AS name
FROM system.settings AS a
INNER JOIN session_records AS r ON r.setting = a.alias_for
WHERE a.alias_for != '' AND r.recorded_name != a.name
  AND match(r.reason, '(?i)alias|renam')
  AND match(r.reason, '(?:^|[^0-9A-Za-z_])' || a.name || '(?:$|[^0-9A-Za-z_])');

-- A setting has a history section exactly when it has recorded changes, and never otherwise.
WITH
    changed AS (SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name FROM system.settings_changes WHERE type = 'Session'),
    documented AS
    (
        SELECT DISTINCT name FROM session_changes
        UNION DISTINCT
        SELECT name FROM changed WHERE name IN (SELECT name FROM system.settings WHERE alias_for != '')
        UNION DISTINCT
        SELECT DISTINCT name FROM alias_registrations
    )
SELECT count() FROM system.documentation
WHERE type = 'Setting' AND (name IN (SELECT name FROM documented)) != (position(description, '**History**') > 0);

WITH changed AS (SELECT DISTINCT arrayJoin(tupleElement(changes, 'name')) AS name FROM system.settings_changes WHERE type = 'MergeTree')
SELECT count() FROM system.documentation
WHERE type = 'MergeTree Setting' AND (name IN (SELECT name FROM changed)) != (position(description, '**History**') > 0);

-- Server settings are not covered by the `compatibility` setting, so no history of their changes is recorded.
SELECT count() FROM system.documentation WHERE type = 'Server Setting' AND position(description, '**History**') > 0;

-- The history lists exactly one item per recorded change of the setting. A change that concerns both a setting and
-- an alias of it is recorded twice, once under each name, with a reason authored separately for each, and is
-- listed once — so the identity of a change is the version and the values together with the recorded name. Two
-- records written under the same name are separate entries of the history file, not two accounts of one change,
-- and both are listed.
SELECT count() FROM system.documentation AS d
INNER JOIN
(
    SELECT name, sum(bullets) AS recorded
    FROM
    (
        SELECT name, version, previous_value, new_value, max(cnt) AS bullets
        FROM
        (
            SELECT name, version, previous_value, new_value, recorded_name, count() AS cnt
            FROM session_changes
            GROUP BY name, version, previous_value, new_value, recorded_name
        )
        GROUP BY name, version, previous_value, new_value
    )
    GROUP BY name
) AS c USING (name)
WHERE d.type = 'Setting'
  AND length(splitByString('\n- **', substring(d.description, position(d.description, '**History**')))) - 1 != c.recorded;

-- The changes are listed newest first, so the most recent one comes right after the section header.
SELECT count() FROM system.documentation AS d
INNER JOIN
(
    SELECT name, argMax(version, arrayMap(x -> toUInt32(x), splitByChar('.', version))) AS newest
    FROM session_changes GROUP BY name
) AS c USING (name)
WHERE d.type = 'Setting' AND position(d.description, '**History**\n\n- **' || c.newest || '** ') = 0;

-- The version in which the setting was introduced is called out separately, above the history.
SELECT count() > 100 FROM system.documentation WHERE type = 'Setting' AND position(description, '**Introduced in:** v') > 0;
SELECT count() > 0 FROM system.documentation WHERE type = 'MergeTree Setting' AND position(description, '**Introduced in:** v') > 0;

-- When an introducing version is claimed, it is the oldest recorded version of the setting, and that record does
-- not change the default value: a record of a change of the default is a change, not an introduction.
SELECT count() FROM system.documentation AS d
INNER JOIN
(
    SELECT
        name,
        argMin(version, arrayMap(x -> toUInt32(x), splitByChar('.', version))) AS oldest,
        argMin(has_unchanged_default, arrayMap(x -> toUInt32(x), splitByChar('.', version))) AS oldest_has_unchanged_default
    FROM
    (
        SELECT name, version, max(previous_value = new_value) AS has_unchanged_default
        FROM session_changes GROUP BY name, version
    )
    GROUP BY name
) AS c USING (name)
WHERE d.type = 'Setting' AND position(d.description, '**Introduced in:** v') > 0
  AND (extract(d.description, '\\*\\*Introduced in:\\*\\* v([0-9.]+)') != c.oldest OR NOT c.oldest_has_unchanged_default);

-- A record that does not change the default value is not necessarily an introduction: it is also how the history
-- notes something else about a setting that already exists. The oldest record of `page_cache_block_size` says that
-- the setting became adjustable per query, so no introducing version is claimed for it.
SELECT position(description, '**Introduced in:**') = 0 FROM system.documentation WHERE type = 'Setting' AND name = 'page_cache_block_size';

-- Conversely, the reason of a record that registers a new setting is free-form — it usually describes what the
-- setting does, and only sometimes says that the setting is new — so the introducing version is reported for such
-- a record whatever its reason says. `apply_row_policy_after_final` and `parallel_replicas_mode` are settings whose
-- introduction is recorded with a reason that never uses the word "new".
SELECT extract(description, '\\*\\*Introduced in:\\*\\* v([0-9.]+)') FROM system.documentation WHERE type = 'Setting' AND name = 'apply_row_policy_after_final';
SELECT extract(description, '\\*\\*Introduced in:\\*\\* v([0-9.]+)') FROM system.documentation WHERE type = 'Setting' AND name = 'parallel_replicas_mode';

-- The full documentation of a setting: the description, the type, the default value, and the history.
-- `async_insert_max_data_size` is a long-standing setting whose default value was raised in 24.2.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'async_insert_max_data_size';

-- An alias is introduced in a particular version and has a history of its own, distinct from the history of the
-- setting it resolves to.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'enable_analyzer';

-- A record that only registers an alias does not become the history of the setting it aliases, and in particular
-- does not claim to introduce it: `max_insert_block_size` is older than the change history and has no recorded
-- change of its own, so it has no history at all, while its alias `max_insert_block_size_rows` — registered in
-- 26.1 — has one.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'max_insert_block_size_rows';
SELECT position(description, '**History**') = 0, position(description, '**Introduced in:**') = 0
FROM system.documentation WHERE type = 'Setting' AND name = 'max_insert_block_size';

-- The history of a setting that was renamed is not cut at the rename: `enable_full_text_index` was called
-- `allow_experimental_full_text_index` when it appeared in 24.6, and that change is recorded under the old name.
SELECT position(description, '\n- **24.6** — the default value changed from `1` to `0`. Enable experimental text index') > 0
FROM system.documentation WHERE type = 'Setting' AND name = 'enable_full_text_index';

-- The history file is inconsistent about where the appearance of an alias is recorded, and an alias that is not
-- recorded under its own name has a history all the same.
-- The alias `async_insert_busy_timeout_ms` was registered by a record written under the canonical name
-- `async_insert_busy_timeout_max_ms`, and it is the history of the alias — its appearance in 24.2.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'async_insert_busy_timeout_ms';

-- A setting that was renamed with its old name kept as an alias has, under the old name, both the history of that
-- name from before the rename and the rename itself, which is recorded under the new name:
-- `text_index_density_threshold` appeared in 26.6 and became an alias of
-- `text_index_lazy_intersection_density_threshold` in 26.7.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'text_index_density_threshold';

-- The record that renames a setting does not claim to introduce the old name when that name is older than it:
-- `evaluation_time` changed its default in 25.8 and became an alias of `promql_evaluation_time` in 25.9.
SELECT position(description, '**Introduced in:**') = 0,
       position(description, '\n- **25.9** — the default value remained `auto`. The setting was renamed.') > 0
FROM system.documentation WHERE type = 'Setting' AND name = 'evaluation_time';

-- An alias can appear in the same version in which the default value of the setting changes, and the record that
-- registers it is then a record of a change of the default as well: `enable_lightweight_update` was made the
-- canonical name of `allow_experimental_lightweight_update` in 25.8, in the version in which the default of both
-- was flipped to `true`. The alias has that version in its history, and the setting lists that one change once,
-- even though the history file records it under both names with a differently worded reason for each.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'allow_experimental_lightweight_update';
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'enable_lightweight_update';

-- Two records written under the same name are separate entries of the history file, not two accounts of one
-- change, and each is listed with its own reason: `enable_max_bytes_limit_for_min_age_to_force_merge` has two
-- 25.1 records with the same values but differently authored reasons, and neither overwrites the other.
SELECT description FROM system.documentation WHERE type = 'MergeTree Setting' AND name = 'enable_max_bytes_limit_for_min_age_to_force_merge';

-- A rename does not always keep the old name as an alias, and then the history recorded under the old name is not
-- reachable through the aliases of the setting; it is followed by the name the reason of the rename record gives.
-- `distributed_cache_alignment` was called `distributed_cache_read_alignment` when it appeared in 24.10 and was
-- renamed in 25.7, with the old name made obsolete rather than kept as an alias, so its introducing version is the
-- one in which it appeared under the old name and not the one in which it was renamed.
SELECT description FROM system.documentation WHERE type = 'Setting' AND name = 'distributed_cache_alignment';

-- When the history recorded under the old name cannot be recovered at all — the old name of
-- `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` has no record of its own — the version of
-- the rename is not claimed as the introducing one either: the setting demonstrably existed before it.
SELECT position(description, '**Introduced in:**') = 0,
       position(description, '\n- **24.11** — the default value remained `1`. Rename of setting') > 0
FROM system.documentation WHERE type = 'Setting' AND name = 'filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit';

-- The rename can also be recorded under the old name ("Obsolete setting, renamed to ..."), and the two records of
-- one rename are listed once: `use_projection_index_in_read_pools` became `use_indexes_refiner_in_read_pools` in
-- 26.8, the version in which it appeared, and the new name lists that version once, as its introduction.
SELECT position(description, '**Introduced in:** v26.8') > 0,
       length(splitByString('\n- **', substring(description, position(description, '**History**')))) - 1
FROM system.documentation WHERE type = 'Setting' AND name = 'use_indexes_refiner_in_read_pools';

DROP VIEW alias_registrations;
DROP VIEW session_changes;
DROP VIEW own_name_changes;
DROP VIEW session_renames;
DROP VIEW session_records;
