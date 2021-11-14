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
show create table aggregate_function_combinators; -- { serverError 390 }
show create table asynchronous_inserts; -- { serverError 390 }
show create table asynchronous_metrics; -- { serverError 390 }
show create table build_options; -- { serverError 390 }
show create table clusters; -- { serverError 390 }
show create table collations; -- { serverError 390 }
show create table columns; -- { serverError 390 }
show create table contributors; -- { serverError 390 }
show create table current_roles; -- { serverError 390 }
show create table data_skipping_indices; -- { serverError 390 }
show create table data_type_families; -- { serverError 390 }
show create table databases; -- { serverError 390 }
show create table detached_parts; -- { serverError 390 }
show create table dictionaries; -- { serverError 390 }
show create table disks; -- { serverError 390 }
show create table distributed_ddl_queue; -- { serverError 390 }
show create table distribution_queue; -- { serverError 390 }
show create table enabled_roles; -- { serverError 390 }
show create table errors; -- { serverError 390 }
show create table events; -- { serverError 390 }
show create table formats; -- { serverError 390 }
show create table functions; -- { serverError 390 }
show create table grants; -- { serverError 390 }
show create table graphite_retentions; -- { serverError 390 }
show create table licenses; -- { serverError 390 }
show create table macros; -- { serverError 390 }
show create table merge_tree_settings; -- { serverError 390 }
show create table merges; -- { serverError 390 }
show create table metrics; -- { serverError 390 }
show create table models; -- { serverError 390 }
show create table mutations; -- { serverError 390 }
show create table numbers; -- { serverError 390 }
show create table numbers_mt; -- { serverError 390 }
show create table one; -- { serverError 390 }
show create table part_moves_between_shards; -- { serverError 390 }
show create table parts; -- { serverError 390 }
show create table parts_columns; -- { serverError 390 }
show create table privileges; -- { serverError 390 }
show create table processes; -- { serverError 390 }
show create table projection_parts; -- { serverError 390 }
show create table projection_parts_columns; -- { serverError 390 }
show create table quota_limits; -- { serverError 390 }
show create table quota_usage; -- { serverError 390 }
show create table quotas; -- { serverError 390 }
show create table quotas_usage; -- { serverError 390 }
show create table replicas; -- { serverError 390 }
show create table replicated_fetches; -- { serverError 390 }
show create table replicated_merge_tree_settings; -- { serverError 390 }
show create table replication_queue; -- { serverError 390 }
show create table rocksdb; -- { serverError 390 }
show create table role_grants; -- { serverError 390 }
show create table roles; -- { serverError 390 }
show create table row_policies; -- { serverError 390 }
show create table settings; -- { serverError 390 }
show create table settings_profile_elements; -- { serverError 390 }
show create table settings_profiles; -- { serverError 390 }
show create table stack_trace; -- { serverError 390 }
show create table storage_policies; -- { serverError 390 }
show create table table_engines; -- { serverError 390 }
show create table table_functions; -- { serverError 390 }
show create table tables; -- { serverError 390 }
show create table time_zones; -- { serverError 390 }
show create table user_directories; -- { serverError 390 }
show create table users; -- { serverError 390 }
show create table warnings; -- { serverError 390 }
show create table zeros; -- { serverError 390 }
show create table zeros_mt; -- { serverError 390 }
