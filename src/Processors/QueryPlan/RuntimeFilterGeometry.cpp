#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/RuntimeFilterGeometry.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

namespace QueryPlanSerializationSetting
{
extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_exact_values_limit;
extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_exact_bytes_limit;
extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_bytes;
extern const QueryPlanSerializationSettingsUInt64 join_runtime_bloom_filter_hash_functions;
extern const QueryPlanSerializationSettingsDouble join_runtime_filter_pass_ratio_threshold_for_disabling;
extern const QueryPlanSerializationSettingsUInt64 join_runtime_filter_blocks_to_skip_before_reenabling;
extern const QueryPlanSerializationSettingsDouble join_runtime_bloom_filter_max_ratio_of_set_bits;
}

void RuntimeFilterGeometry::serializeSettings(QueryPlanSerializationSettings & settings, UInt64 version) const
{
    settings[QueryPlanSerializationSetting::join_runtime_filter_exact_values_limit] = exact_values_limit;
    /// A peer below this version rejects the unknown name. Omitting it is fail-open to the default
    /// floor, which is correct for a field-less local step.
    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_RUNTIME_FILTER_EXCHANGES)
        settings[QueryPlanSerializationSetting::join_runtime_filter_exact_bytes_limit] = exact_bytes_limit;
    settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_bytes] = bloom_filter_bytes;
    settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_hash_functions] = bloom_filter_hash_functions;
    settings[QueryPlanSerializationSetting::join_runtime_filter_pass_ratio_threshold_for_disabling] = pass_ratio_threshold_for_disabling;
    settings[QueryPlanSerializationSetting::join_runtime_filter_blocks_to_skip_before_reenabling] = blocks_to_skip_before_reenabling;
    settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_max_ratio_of_set_bits] = max_ratio_of_set_bits_in_bloom_filter;
}

void RuntimeFilterGeometry::validateTransported() const
{
    if (!bloom_filter_bytes || bloom_filter_bytes > MAX_RUNTIME_BLOOM_FILTER_BYTES || !bloom_filter_hash_functions
        || bloom_filter_hash_functions > MAX_RUNTIME_BLOOM_FILTER_HASH_FUNCTIONS || exact_bytes_limit < bloom_filter_bytes
        || exact_bytes_limit > MAX_RUNTIME_BLOOM_FILTER_BYTES)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Transported runtime filter geometry is out of bounds: {} bloom bytes, {} hash functions, {} exact bytes limit",
            bloom_filter_bytes,
            bloom_filter_hash_functions,
            exact_bytes_limit);
}

RuntimeFilterGeometry RuntimeFilterGeometry::fromSettings(const QueryPlanSerializationSettings & settings)
{
    return RuntimeFilterGeometry{
        .exact_values_limit = settings[QueryPlanSerializationSetting::join_runtime_filter_exact_values_limit],
        .exact_bytes_limit = settings[QueryPlanSerializationSetting::join_runtime_filter_exact_bytes_limit],
        .bloom_filter_bytes = settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_bytes],
        .bloom_filter_hash_functions = settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_hash_functions],
        .pass_ratio_threshold_for_disabling
        = settings[QueryPlanSerializationSetting::join_runtime_filter_pass_ratio_threshold_for_disabling],
        .blocks_to_skip_before_reenabling = settings[QueryPlanSerializationSetting::join_runtime_filter_blocks_to_skip_before_reenabling],
        .max_ratio_of_set_bits_in_bloom_filter = settings[QueryPlanSerializationSetting::join_runtime_bloom_filter_max_ratio_of_set_bits],
    };
}

}
