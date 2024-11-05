#include <Interpreters/Context.h>

namespace DB
{

/*
 * Enables all settings that allow the use of experimental, deprecated, or potentially unsafe features
 * in a CREATE query. This function is used in DatabaseReplicated::recoverLostReplica() to create tables
 * when the original settings used to create the table are not available.
 */

void enableAllExperimentalSettings(ContextMutablePtr context)
{
    context->setSetting("allow_experimental_inverted_index", 1);
    context->setSetting("allow_experimental_full_text_index", 1);
    context->setSetting("allow_experimental_codecs", 1);
    context->setSetting("allow_experimental_live_view", 1);
    context->setSetting("allow_experimental_window_view", 1);
    context->setSetting("allow_experimental_funnel_functions", 1);
    context->setSetting("allow_experimental_nlp_functions", 1);
    context->setSetting("allow_experimental_hash_functions", 1);
    context->setSetting("allow_experimental_object_type", 1);
    context->setSetting("allow_experimental_variant_type", 1);
    context->setSetting("allow_experimental_dynamic_type", 1);
    context->setSetting("allow_experimental_json_type", 1);
    context->setSetting("allow_experimental_vector_similarity_index", 1);
    context->setSetting("allow_experimental_bigint_types", 1);
    context->setSetting("allow_experimental_window_functions", 1);
    context->setSetting("allow_experimental_geo_types", 1);
    context->setSetting("allow_experimental_map_type", 1);
    context->setSetting("allow_deprecated_error_prone_window_functions", 1);

    context->setSetting("allow_suspicious_low_cardinality_types", 1);
    context->setSetting("allow_suspicious_fixed_string_types", 1);
    context->setSetting("allow_suspicious_indices", 1);
    context->setSetting("allow_suspicious_codecs", 1);
    context->setSetting("allow_hyperscan", 1);
    context->setSetting("allow_simdjson", 1);
    context->setSetting("allow_deprecated_syntax_for_merge_tree", 1);
    context->setSetting("allow_suspicious_primary_key", 1);
    context->setSetting("allow_suspicious_ttl_expressions", 1);
    context->setSetting("allow_suspicious_variant_types", 1);
    context->setSetting("enable_zstd_qat_codec", 1);
    context->setSetting("allow_create_index_without_type", 1);
    context->setSetting("allow_experimental_s3queue", 1);

    /// clickhouse-private settings
    context->setSetting("allow_experimental_shared_set_join", 1);
}

}
