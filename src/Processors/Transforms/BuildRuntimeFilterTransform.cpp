#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/FilterDescription.h>
#include <Columns/IColumn.h>
#include <Common/NaNUtils.h>
#include <Interpreters/Context.h>
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

/// `NaN`-detection helpers for `BuildRuntimeFilterTransform`.
///
/// JOIN ON treats `NaN` as never-matching (issue #106531). If a `NaN` row were
/// inserted into the runtime filter, the bloom or exact-match check would match
/// it bitwise on the probe side and either (a) wrongly let an INNER probe through,
/// or (b) wrongly exclude a probe `NaN` from an ANTI/LEFT result.
///
/// Float keys reach the transform wrapped in `Nullable`, `LowCardinality`, or, for
/// multi-key LEFT ANTI, a temporary `Tuple`. The helper below recursively unwraps
/// these layers and OR-s a `nan_mask[i] = 1` for every row whose float payload is
/// `NaN`. Already-NULL rows do not need to be dropped (they are stored as NULL by
/// `Set` and are not bitwise-matched), so a `Nullable` wrapper restricts NaN
/// detection to rows where `null_map[i] == 0`.

void markNaNRowsImpl(const IColumn & column, IColumn::Filter & nan_mask, const UInt8 * is_null);

template <typename T>
void markNaNRowsInVector(const ColumnVector<T> & vec, IColumn::Filter & nan_mask, const UInt8 * is_null)
{
    const auto & data = vec.getData();
    const size_t size = data.size();
    chassert(nan_mask.size() == size);

    if (is_null)
    {
        for (size_t i = 0; i < size; ++i)
            nan_mask[i] |= static_cast<UInt8>(is_null[i] == 0 && isNaN(data[i]));
    }
    else
    {
        for (size_t i = 0; i < size; ++i)
            nan_mask[i] |= static_cast<UInt8>(isNaN(data[i]));
    }
}

void markNaNRowsImpl(const IColumn & column, IColumn::Filter & nan_mask, const UInt8 * is_null)
{
    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(&column))
    {
        markNaNRowsInVector<Float32>(*f32, nan_mask, is_null);
        return;
    }
    if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(&column))
    {
        markNaNRowsInVector<Float64>(*f64, nan_mask, is_null);
        return;
    }
    if (const auto * bf16 = typeid_cast<const ColumnVector<BFloat16> *>(&column))
    {
        markNaNRowsInVector<BFloat16>(*bf16, nan_mask, is_null);
        return;
    }
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        /// We only need to consider rows where the value is not NULL; a NULL stored in
        /// the runtime filter does not bitwise-match anything on the probe side.
        const auto & null_map = nullable->getNullMapData();
        markNaNRowsImpl(nullable->getNestedColumn(), nan_mask, null_map.data());
        return;
    }
    if (const auto * lc = typeid_cast<const ColumnLowCardinality *>(&column))
    {
        /// Materialise once so each row is examined directly. Inspecting the dictionary in
        /// place would require honouring per-row indexes anyway, so this is the simplest
        /// correct path. Runtime filters typically build over modest blocks, so the
        /// allocation is bounded by the chunk size.
        ColumnPtr full = lc->convertToFullColumnIfLowCardinality();
        markNaNRowsImpl(*full, nan_mask, is_null);
        return;
    }
    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        /// A row is dropped if ANY float element of the tuple is `NaN`. For LEFT ANTI
        /// multi-key joins the tuple is the runtime-filter key, so a single `NaN`
        /// element makes the row never-matching at the join level.
        const size_t tuple_size = tuple->tupleSize();
        for (size_t i = 0; i < tuple_size; ++i)
            markNaNRowsImpl(tuple->getColumn(i), nan_mask, is_null);
        return;
    }

    /// Any other column type (integers, strings, dates, ...) cannot carry a float `NaN`.
    /// Leave `nan_mask` untouched.
}

ColumnPtr filterOutNaNs(const ColumnPtr & column)
{
    const size_t size = column->size();
    if (size == 0)
        return nullptr;

    IColumn::Filter nan_mask(size, 0);
    markNaNRowsImpl(*column, nan_mask, /* is_null = */ nullptr);

    /// Common case: no `NaN` rows. Avoid allocating a copy.
    bool has_nan = false;
    for (size_t i = 0; i < size; ++i)
    {
        if (nan_mask[i])
        {
            has_nan = true;
            break;
        }
    }
    if (!has_nan)
        return nullptr;

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
