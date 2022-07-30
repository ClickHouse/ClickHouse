/* we will `use system` to bypass style check,
because `show create table` statement
cannot fit the requirement in check-sytle, which is as

"# Queries to:
tables_with_database_column=(
    system.tables
    system.parts
    system.detached_parts
    system.parts_columns
    system.columns
    system.projection_parts
    system.mutations
)
# should have database = currentDatabase() condition"

 */
use system;
show create table aggregate_function_combinators format TSVRaw;
show create table asynchronous_inserts format TSVRaw;
show create table asynchronous_metrics format TSVRaw;
show create table build_options format TSVRaw;
show create table clusters format TSVRaw;
show create table collations format TSVRaw;
show create table columns format TSVRaw;
show create table contributors format TSVRaw;
show create table current_roles format TSVRaw;
show create table data_skipping_indices format TSVRaw;
show create table data_type_families format TSVRaw;
show create table databases format TSVRaw;
show create table detached_parts format TSVRaw;
show create table dictionaries format TSVRaw;
show create table disks format TSVRaw;
show create table distributed_ddl_queue format TSVRaw;
show create table distribution_queue format TSVRaw;
show create table enabled_roles format TSVRaw;
show create table errors format TSVRaw;
show create table events format TSVRaw;
show create table formats format TSVRaw;
show create table functions format TSVRaw;
show create table grants format TSVRaw;
show create table graphite_retentions format TSVRaw;
show create table licenses format TSVRaw;
show create table macros format TSVRaw;
show create table merge_tree_settings format TSVRaw;
show create table merges format TSVRaw;
show create table metrics format TSVRaw;
show create table models format TSVRaw;
show create table mutations format TSVRaw;
show create table numbers format TSVRaw;
show create table numbers_mt format TSVRaw;
show create table one format TSVRaw;
show create table part_moves_between_shards format TSVRaw;
show create table parts format TSVRaw;
show create table parts_columns format TSVRaw;
show create table privileges format TSVRaw;
show create table processes format TSVRaw;
show create table projection_parts format TSVRaw;
show create table projection_parts_columns format TSVRaw;
show create table quota_limits format TSVRaw;
show create table quota_usage format TSVRaw;
show create table quotas format TSVRaw;
show create table quotas_usage format TSVRaw;
show create table replicas format TSVRaw;
show create table replicated_fetches format TSVRaw;
show create table replicated_merge_tree_settings format TSVRaw;
show create table replication_queue format TSVRaw;
show create table role_grants format TSVRaw;
show create table roles format TSVRaw;
show create table row_policies format TSVRaw;
show create table settings format TSVRaw;
show create table settings_profile_elements format TSVRaw;
show create table settings_profiles format TSVRaw;
show create table stack_trace format TSVRaw;
show create table storage_policies format TSVRaw;
show create table table_engines format TSVRaw;
show create table table_functions format TSVRaw;
show create table tables format TSVRaw;
show create table time_zones format TSVRaw;
show create table user_directories format TSVRaw;
show create table users format TSVRaw;
show create table warnings format TSVRaw;
show create table zeros format TSVRaw;
show create table zeros_mt format TSVRaw;
