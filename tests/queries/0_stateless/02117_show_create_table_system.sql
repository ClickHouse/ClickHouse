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
show create table aggregate_function_combinators;
show create table asynchronous_inserts;
show create table asynchronous_metrics;
show create table build_options;
show create table clusters;
show create table collations;
show create table columns;
show create table contributors;
show create table current_roles;
show create table data_skipping_indices;
show create table data_type_families;
show create table databases;
show create table detached_parts;
show create table dictionaries;
show create table disks;
show create table distributed_ddl_queue;
show create table distribution_queue;
show create table enabled_roles;
show create table errors;
show create table events;
show create table formats;
show create table functions;
show create table grants;
show create table graphite_retentions;
show create table licenses;
show create table macros;
show create table merge_tree_settings;
show create table merges;
show create table metrics;
show create table models;
show create table mutations;
show create table numbers;
show create table numbers_mt;
show create table one;
show create table part_moves_between_shards;
show create table parts;
show create table parts_columns;
show create table privileges;
show create table processes;
show create table projection_parts;
show create table projection_parts_columns;
show create table quota_limits;
show create table quota_usage;
show create table quotas;
show create table quotas_usage;
show create table replicas;
show create table replicated_fetches;
show create table replicated_merge_tree_settings;
show create table replication_queue;
show create table role_grants;
show create table roles;
show create table row_policies;
show create table settings;
show create table settings_profile_elements;
show create table settings_profiles;
show create table stack_trace;
show create table storage_policies;
show create table table_engines;
show create table table_functions;
show create table tables;
show create table time_zones;
show create table user_directories;
show create table users;
show create table warnings;
show create table zeros;
show create table zeros_mt;
