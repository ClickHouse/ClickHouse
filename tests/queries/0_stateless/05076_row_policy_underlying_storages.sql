-- Row policies of the table an `Alias` reads are applied to reads through the alias,
-- including when the alias is still a lazily loaded proxy.
DROP ROW POLICY IF EXISTS rp_target_policy ON rp_target;
DROP ROW POLICY IF EXISTS rp_alias_policy ON rp_alias;
DROP TABLE IF EXISTS rp_merge;
DROP TABLE IF EXISTS rp_alias;
DROP TABLE IF EXISTS rp_target;

SET allow_experimental_alias_table_engine = 1;
SET optimize_trivial_count_query = 1;
SET make_distributed_plan = 0;
SET serialize_query_plan = 0;
SET allow_experimental_parallel_reading_from_replicas = 0;

CREATE TABLE rp_target (id UInt32, tenant_id UInt32, active UInt8) ENGINE = MergeTree ORDER BY id;
INSERT INTO rp_target VALUES (1, 1, 1), (2, 1, 0), (3, 2, 1), (4, 2, 0);
CREATE TABLE rp_alias ENGINE = Alias(currentDatabase(), 'rp_target');
CREATE TABLE rp_merge (id UInt32, tenant_id UInt32, active UInt8) ENGINE = Merge(currentDatabase(), '^rp_alias$');

CREATE ROW POLICY rp_target_policy ON rp_target FOR SELECT USING tenant_id = 1 TO CURRENT_USER;

SELECT 'Target policy through Alias, old analyzer', arraySort(groupArray(id)) FROM rp_alias SETTINGS enable_analyzer = 0;
SELECT 'Target policy through Alias, analyzer', arraySort(groupArray(id)) FROM rp_alias SETTINGS enable_analyzer = 1;
SELECT 'Target policy through Merge over Alias', arraySort(groupArray(id)) FROM rp_merge;

-- The trivial count optimization must not bypass the target policy.
SELECT 'Count through Alias, trivial count disabled', count() FROM rp_alias SETTINGS optimize_trivial_count_query = 0;
SELECT 'Count through Alias, trivial count enabled', count() FROM rp_alias SETTINGS optimize_trivial_count_query = 1;

-- Policies of the alias and of its target are combined with a logical AND.
CREATE ROW POLICY rp_alias_policy ON rp_alias FOR SELECT USING active = 1 TO CURRENT_USER;
SELECT 'Combined policies, old analyzer', arraySort(groupArray(id)) FROM rp_alias SETTINGS enable_analyzer = 0;
SELECT 'Combined policies, analyzer', arraySort(groupArray(id)) FROM rp_alias SETTINGS enable_analyzer = 1;

DROP ROW POLICY rp_target_policy ON rp_target;
SELECT 'Alias policy only', arraySort(groupArray(id)) FROM rp_alias;

DROP ROW POLICY rp_alias_policy ON rp_alias;
DROP TABLE rp_merge;
DROP TABLE rp_alias;
DROP TABLE rp_target;

-- A lazily loaded table is a proxy until first access; the target policy must apply either way.
DROP DATABASE IF EXISTS 05076_lazy SYNC;
CREATE DATABASE 05076_lazy ENGINE = Atomic SETTINGS lazy_load_tables = 1;
CREATE TABLE 05076_lazy.base (id UInt32, tenant_id UInt32) ENGINE = MergeTree ORDER BY id;
INSERT INTO 05076_lazy.base VALUES (1, 1), (2, 2), (3, 1);
CREATE TABLE 05076_lazy.al ENGINE = Alias('05076_lazy', 'base');
CREATE ROW POLICY rp_lazy ON 05076_lazy.base FOR SELECT USING tenant_id = 1 TO CURRENT_USER;

DETACH DATABASE 05076_lazy SYNC;
ATTACH DATABASE 05076_lazy;
-- Read the alias before anything materializes the proxy of its target.
SELECT 'Target policy through a lazily loaded Alias', arraySort(groupArray(id)) FROM 05076_lazy.al;

DROP ROW POLICY rp_lazy ON 05076_lazy.base;
DROP DATABASE 05076_lazy SYNC;
