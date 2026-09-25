#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/RuntimeFilterBuildOptions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

BuildRuntimeFilterTransform::BuildRuntimeFilterTransform(
    SharedHeader header_,
    String filter_column_name_,
    const DataTypePtr & filter_column_type_,
    String filter_name_,
    String filter_key_,
    size_t filters_to_merge_,
    const RuntimeFilterBuildOptions & build_options_,
    ContextPtr query_context_)
    : ISimpleTransform(header_, header_, true)
    , filter_column_name(filter_column_name_)
    , filter_column_position(header_->getPositionByName(filter_column_name))
    , filter_column_original_type(header_->getByPosition(filter_column_position).type)
    , filter_column_target_type(filter_column_type_)
    , filter_name(filter_name_)
    , filter_key(std::move(filter_key_))
    , query_context(std::move(query_context_))
{
    const auto & filter_column = header_->getByPosition(filter_column_position);
    if (!filter_column_target_type->equals(*filter_column_original_type))
        cast_to_target_type = createInternalCast(filter_column, filter_column_target_type, CastType::nonAccurate, {}, nullptr);

    const auto & geometry = build_options_.geometry;
    const RuntimeFilterConfig runtime_filter_config{geometry.pass_ratio_threshold_for_disabling, geometry.blocks_to_skip_before_reenabling};

    if (build_options_.polarity == RuntimeFilterPolarity::Contains)
    {
        if (AdaptiveSetRuntimeFilter::isDataTypeSupported(filter_column_target_type))
        {
            built_filter = std::make_unique<RuntimeFilter>(
                filters_to_merge_,
                runtime_filter_config,
                RuntimeFilter::Adaptive(
                    filter_column_target_type,
                    geometry,
                    build_options_.distinct_keys_hint,
                    build_options_.distinct_keys_hint_matches_filter_key));
        }
        else
        {
            built_filter = std::make_unique<RuntimeFilter>(
                filters_to_merge_,
                runtime_filter_config,
                RuntimeFilter::ExactContains(filter_column_target_type, geometry.exact_bytes_limit, geometry.exact_values_limit));
        }
    }
    else
    {
        built_filter = std::make_unique<RuntimeFilter>(
            filters_to_merge_,
            runtime_filter_config,
            RuntimeFilter::ExactNotContains(filter_column_target_type, geometry.exact_bytes_limit, geometry.exact_values_limit));
    }

    /// Only pay the extra min/max scan of the build side when the left side will use it for index analysis.
    if (build_options_.track_key_range)
        built_filter->enableIndexAnalysis();
}


IProcessor::Status BuildRuntimeFilterTransform::prepare()
{
    auto status = ISimpleTransform::prepare();

    if (status == IProcessor::Status::Finished)
        finish();

    return status;
}

void BuildRuntimeFilterTransform::transform(Chunk & chunk)
{
    ColumnPtr filter_column = chunk.getColumns()[filter_column_position];
    if (cast_to_target_type)
    {
        filter_column = cast_to_target_type->execute(
            {ColumnWithTypeAndName(filter_column, filter_column_original_type, "")},
            filter_column_target_type,
            filter_column->size(),
            false);
    }

    built_filter->insert(filter_column);
}

void BuildRuntimeFilterTransform::finish()
{
    /// A deserialized step has no random key and is never executed in practice; nothing to register.
    if (filter_key.empty())
        return;
    if (!query_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context is not available for BuildRuntimeFilterTransform");
    auto filter_lookup = query_context->getRuntimeFilterLookup();
    /// Register under the random key (matches the probe-side `__applyFilter`), not the displayed
    /// stable `filter_name`. Keeping the key off the plan means it never enters a plan-step hash.
    /// The stable name is passed alongside for readable stats logging (the key is opaque).
    filter_lookup->add(filter_key, filter_name, std::move(built_filter));
}

}
