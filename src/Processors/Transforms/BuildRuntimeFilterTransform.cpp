#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

UniqueRuntimeFilterPtr createRuntimeFilter(
    size_t filters_to_merge,
    const DataTypePtr & target_type,
    const RuntimeFilterBuildOptions & build_options,
    const RuntimeFilterConfig & runtime_filter_config)
{
    const bool allow_to_use_not_exact_filter = build_options.polarity == RuntimeFilterPolarity::Contains;
    const bool adaptive_filter_supported = AdaptiveSetRuntimeFilter::isDataTypeSupported(target_type);
    const bool minmax_filter_supported = supportsNumericMinMaxRuntimeFilter(target_type);
    switch (build_options.minmax_mode)
    {
        case RuntimeFilterMinMaxMode::Disabled: break;
        case RuntimeFilterMinMaxMode::Combined:
            if (!allow_to_use_not_exact_filter || !adaptive_filter_supported || !minmax_filter_supported)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot use combined numeric minmax runtime filter mode with type {} and allow_to_use_not_exact_filter={}",
                    target_type->getName(),
                    allow_to_use_not_exact_filter);
            break;
        case RuntimeFilterMinMaxMode::Only:
            if (!allow_to_use_not_exact_filter || !minmax_filter_supported)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Cannot use numeric minmax-only runtime filter mode with type {} and allow_to_use_not_exact_filter={}",
                    target_type->getName(),
                    allow_to_use_not_exact_filter);
            break;
    }

    UniqueRuntimeFilterPtr filter;
    if (allow_to_use_not_exact_filter)
    {
        auto make_adaptive = [&]
        {
            return RuntimeFilter::Adaptive(
                target_type,
                build_options.bloom.bytes,
                build_options.exact_values_limit,
                build_options.bloom.hash_functions,
                build_options.max_ratio_of_set_bits,
                build_options.distinct_keys_hint,
                build_options.distinct_keys_hint_matches_filter_key);
        };

        switch (build_options.minmax_mode)
        {
            case RuntimeFilterMinMaxMode::Only:
                filter = std::make_unique<RuntimeFilter>(filters_to_merge, runtime_filter_config, RuntimeFilter::MinMax(target_type));
                break;
            case RuntimeFilterMinMaxMode::Combined:
                filter = std::make_unique<RuntimeFilter>(
                    filters_to_merge,
                    runtime_filter_config,
                    RuntimeFilter::AdaptiveWithMinMax{make_adaptive(), RuntimeFilter::MinMax(target_type)});
                break;
            case RuntimeFilterMinMaxMode::Disabled:
                if (adaptive_filter_supported)
                    filter = std::make_unique<RuntimeFilter>(filters_to_merge, runtime_filter_config, make_adaptive());
                else
                    filter = std::make_unique<RuntimeFilter>(
                        filters_to_merge,
                        runtime_filter_config,
                        RuntimeFilter::ExactContains(target_type, build_options.bloom.bytes, build_options.exact_values_limit));
                break;
        }
    }
    else
    {
        filter = std::make_unique<RuntimeFilter>(
            filters_to_merge,
            runtime_filter_config,
            RuntimeFilter::ExactNotContains(target_type, build_options.bloom.bytes, build_options.exact_values_limit));
    }

    if (build_options.track_key_range)
        filter->enableIndexAnalysis();
    return filter;
}

}

BuildRuntimeFilterTransform::BuildRuntimeFilterTransform(
    SharedHeader header_,
    String filter_column_name_,
    const DataTypePtr & filter_column_type_,
    String filter_name_,
    String filter_key_,
    size_t filters_to_merge_,
    const RuntimeFilterBuildOptions & build_options_,
    const RuntimeFilterConfig & runtime_filter_config_,
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

    built_filter = createRuntimeFilter(filters_to_merge_, filter_column_target_type, build_options_, runtime_filter_config_);
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
