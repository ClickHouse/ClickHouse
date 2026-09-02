#include <Compression/CompressionFactory.h>
#include <Core/Field.h>
#include <Databases/enableAllExperimentalSettings.h>
#include <Interpreters/Context.h>

namespace DB
{

const std::vector<std::string> & allExperimentalSettingNames()
{
    static const std::vector<std::string> names = []
    {
        std::vector<std::string> result =
        {
        "allow_experimental_codecs",
        "allow_experimental_funnel_functions",
        "allow_experimental_nlp_functions",
        "allow_fuzz_query_functions",
        "allow_experimental_hash_functions",
        "allow_experimental_vector_similarity_index",
        "allow_experimental_text_index_lazy_apply",
        "allow_experimental_window_functions",
        "allow_experimental_geo_types",
        "allow_experimental_map_type",
        "allow_experimental_bigint_types",
        "allow_experimental_bfloat16_type",
        "allow_experimental_time_time64_type",
        "allow_experimental_nullable_tuple_type",
        "allow_experimental_correlated_subqueries",
        "allow_experimental_unique_key",
        "allow_deprecated_error_prone_window_functions",

        "allow_suspicious_low_cardinality_types",
        "allow_suspicious_fixed_string_types",
        "allow_suspicious_types_in_group_by",
        "allow_suspicious_types_in_order_by",
        "allow_suspicious_indices",
        "allow_minmax_index_for_json",
        "allow_suspicious_codecs",
        "allow_hyperscan",
        "allow_simdjson",
        "allow_deprecated_syntax_for_merge_tree",
        "allow_suspicious_primary_key",
        "allow_suspicious_ttl_expressions",
        "allow_suspicious_variant_types",
        "allow_create_index_without_type",
        "allow_experimental_s3queue",
        "allow_experimental_database_iceberg",
        "allow_experimental_database_hms_catalog",
        "allow_experimental_database_unity_catalog",
        "allow_experimental_database_glue_catalog",
        "allow_database_unity_catalog",
        "allow_database_glue_catalog",
        "allow_database_iceberg",
        "allow_delta_kernel_rs",
        "allow_experimental_ytsaurus_table_function",
        "allow_experimental_eval_table_function",
        "allow_experimental_ytsaurus_table_engine",
        "allow_experimental_ytsaurus_dictionary_source",
        "allow_experimental_time_series_aggregate_functions",
        "allow_experimental_lightweight_update",
        "allow_insert_into_iceberg",
        "allow_experimental_iceberg_compaction",
        "allow_experimental_cleanup_old_data_files_compaction",
        "allow_iceberg_remove_orphan_files",
        "allow_experimental_expire_snapshots",
        "allow_experimental_delta_lake_writes",
        "allow_experimental_paimon_storage_engine",
        "allow_dynamic_type_in_join_keys",
        "allow_experimental_alias_table_engine",
        "allow_experimental_database_paimon_rest_catalog",
        "allow_experimental_object_storage_queue_hive_partitioning",
        "allow_experimental_json_lazy_type_hints",
        "allow_experimental_url_wildcard_from_index_pages",
        "allow_experimental_full_text_index",

        /// clickhouse-private settings
        "allow_experimental_shared_set_join",
        };
        /// Per-codec gates are registered alongside the codecs, so they are read from the factory
        /// rather than duplicated here.
        for (const auto & name : CompressionCodecFactory::instance().getGateSettingNames())
            result.emplace_back(name);
        return result;
    }();
    return names;
}

void enableAllExperimentalSettings(ContextMutablePtr context)
{
    for (const auto & name : allExperimentalSettingNames())
        context->setSetting(name, 1);
}

}
