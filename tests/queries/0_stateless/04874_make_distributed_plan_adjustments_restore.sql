-- Tags: no-old-analyzer
-- no-old-analyzer: make_distributed_plan requires the analyzer.

-- A derived distributed plan force-adjusts the settings it does not support. Turning the
-- workers num back to zero must undo those adjustments: a session that briefly asked for
-- Workers must not keep degraded settings for its later ordinary queries. The assertions
-- compare snapshots instead of fixed values, so they hold under randomized harness settings
-- and under an ambient const pin on `make_distributed_plan` (ClickHouse Cloud), where the
-- derivation never fires and nothing moves at all.

SET compile_expressions = 1;

CREATE TEMPORARY TABLE settings_before AS SELECT name, value, changed FROM system.settings;

SET distributed_plan_workers_num = 3;

SELECT 'the derived plan adjusts an explicitly set value, unless pinned const';
SELECT getSetting('compile_expressions') = (SELECT readonly != 0 FROM system.settings WHERE name = 'make_distributed_plan');

SET distributed_plan_workers_num = 0;

SELECT 'the explicitly set value returns when the workers num goes back to zero';
SELECT getSetting('compile_expressions');

SELECT 'and no other setting keeps a trace of the derived window';
-- Both snapshots go through the same statement shape: the analyzer mutates some per-query
-- settings (e.g. `parallel_replicas_for_cluster_engines`) on a plain SELECT context but not
-- on the inner context of CREATE ... AS SELECT, and a shape mismatch would show that noise.
CREATE TEMPORARY TABLE settings_after AS SELECT name, value, changed FROM system.settings;
SELECT count()
FROM settings_after a
JOIN settings_before b USING (name)
WHERE (a.value != b.value OR a.changed != b.changed) AND name != 'distributed_plan_workers_num';

SELECT 'a value set explicitly during the window survives the restore';
-- Pinned explicitly: the harness randomizes this setting, and a window only remembers a setting
-- it actually overrides.
SET use_skip_indexes_on_data_read = 1;
SET distributed_plan_workers_num = 3;
SET use_skip_indexes_on_data_read = 0;
SET distributed_plan_workers_num = 0;
SELECT getSetting('use_skip_indexes_on_data_read');
