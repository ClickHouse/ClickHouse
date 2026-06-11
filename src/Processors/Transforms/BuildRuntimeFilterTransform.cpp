#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinUtils.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>


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
    size_t filters_to_merge_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_bytes_,
    UInt64 bloom_filter_hash_functions_,
    Float64 pass_ratio_threshold_for_disabling_,
    UInt64 blocks_to_skip_before_reenabling_,
    Float64 max_ratio_of_set_bits_in_bloom_filter_,
    bool allow_to_use_not_exact_filter_,
    ContextPtr query_context_)
    : ISimpleTransform(header_, header_, true)
    , filter_column_name(filter_column_name_)
    , filter_column_position(header_->getPositionByName(filter_column_name))
    , filter_column_original_type(header_->getByPosition(filter_column_position).type)
    , filter_column_target_type(filter_column_type_)
    , filter_name(filter_name_)
    , query_context(std::move(query_context_))
{
    const auto & filter_column = header_->getByPosition(filter_column_position);
    if (!filter_column_target_type->equals(*filter_column_original_type))
        cast_to_target_type = createInternalCast(filter_column, filter_column_target_type, CastType::nonAccurate, {}, nullptr);

    if (allow_to_use_not_exact_filter_)
    {
        if (ApproximateRuntimeFilter::isDataTypeSupported(filter_column_target_type))
        {
            built_filter = std::make_unique<ApproximateRuntimeFilter>(
                filters_to_merge_,
                filter_column_target_type,
                pass_ratio_threshold_for_disabling_,
                blocks_to_skip_before_reenabling_,
                bloom_filter_bytes_,
                exact_values_limit_,
                bloom_filter_hash_functions_,
                max_ratio_of_set_bits_in_bloom_filter_);
        }
        else
        {
            built_filter = std::make_unique<ExactContainsRuntimeFilter>(
                filters_to_merge_,
                filter_column_target_type,
                pass_ratio_threshold_for_disabling_,
                blocks_to_skip_before_reenabling_,
                bloom_filter_bytes_,
                exact_values_limit_);
        }
    }
    else
    {
        built_filter = std::make_unique<ExactNotContainsRuntimeFilter>(
            filters_to_merge_,
            filter_column_target_type,
            pass_ratio_threshold_for_disabling_,
            blocks_to_skip_before_reenabling_,
            bloom_filter_bytes_,
            exact_values_limit_);
    }
}


IProcessor::Status BuildRuntimeFilterTransform::prepare()
{
    auto status = ISimpleTransform::prepare();

    if (status == IProcessor::Status::Finished)
        finish();

    return status;
}

namespace
{

/// JOIN ON treats `NaN` as never-matching (issue #106531). If a `NaN` row were inserted into
/// the runtime filter, the bloom or exact-match check would match it bitwise on the probe side
/// and either (a) wrongly let an INNER probe through, or (b) wrongly exclude a probe `NaN` from
/// an ANTI/LEFT result. Drop every row whose float payload is `NaN` before insertion, reusing
/// the shared `JoinCommon` recursion (which unwraps `Nullable` / `LowCardinality` / `Tuple` /
/// `ColumnConst` / `ColumnSparse` and detects `NaN` via `ColumnVector::getNanMask`).
ColumnPtr filterOutNaNs(const ColumnPtr & column)
{
    const size_t size = column->size();
    if (size == 0)
        return nullptr;

    /// Type-only fast path: most runtime-filter keys are integer / string / date and never
    /// carry a `NaN`. Skip the per-row allocation in that case.
    if (!JoinCommon::joinKeyContainsFloatPayload(*column))
        return nullptr;

    IColumn::Filter nan_mask(size, 0);
    if (!JoinCommon::markFloatNaNRowsAsNull(*column, nan_mask))
        return nullptr; /// no NaN found - keep the original column unfiltered

    /// Convert drop-mask to keep-mask in place and filter the original column. We filter
    /// the wrapped column (as opposed to its float payload) so the result preserves the
    /// `Nullable`/`LowCardinality`/`Tuple` shape `Set::insertFromColumns` expects.
    for (size_t i = 0; i < size; ++i)
        nan_mask[i] = !nan_mask[i];
    return column->filter(nan_mask, /* result_size_hint = */ -1);
}

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

    /// Strip `NaN` rows from float keys so they are never registered in the runtime filter.
    /// Issue #106531.
    if (auto without_nans = filterOutNaNs(filter_column))
        filter_column = std::move(without_nans);

    built_filter->insert(filter_column);
}

void BuildRuntimeFilterTransform::finish()
{
    if (!query_context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query context is not available for BuildRuntimeFilterTransform");
    auto filter_lookup = query_context->getRuntimeFilterLookup();
    filter_lookup->add(filter_name, std::move(built_filter));
}

}
