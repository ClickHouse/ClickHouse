#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
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

/// Drop rows where the float column has `NaN` so the runtime filter never carries any
/// `NaN` entry. JOIN ON treats `NaN` as never-matching (issue #106531) - if `NaN` were
/// inserted, the bloom/exact-match would match it bitwise on the probe side and either
/// (a) wrongly let an INNER probe through, or (b) wrongly exclude a probe `NaN` from an
/// ANTI/LEFT result. Returns the original column when no `NaN` is found or when the
/// column type is not float.
template <typename T>
ColumnPtr filterOutNaNsImpl(const ColumnVector<T> & vec)
{
    const auto & data = vec.getData();
    const size_t size = data.size();

    /// Fast scan for any `NaN`. Common case: there isn't one.
    bool has_nan = false;
    for (size_t i = 0; i < size; ++i)
    {
        if (isNaN(data[i]))
        {
            has_nan = true;
            break;
        }
    }
    if (!has_nan)
        return nullptr;

    auto filter = ColumnUInt8::create(size);
    auto & filter_data = filter->getData();
    for (size_t i = 0; i < size; ++i)
        filter_data[i] = static_cast<UInt8>(!isNaN(data[i]));
    return vec.filter(filter_data, /* result_size_hint = */ -1);
}

ColumnPtr filterOutNaNs(const ColumnPtr & column)
{
    if (const auto * f32 = typeid_cast<const ColumnFloat32 *>(column.get()))
        return filterOutNaNsImpl<Float32>(*f32);
    if (const auto * f64 = typeid_cast<const ColumnFloat64 *>(column.get()))
        return filterOutNaNsImpl<Float64>(*f64);
    if (const auto * bf16 = typeid_cast<const ColumnVector<BFloat16> *>(column.get()))
        return filterOutNaNsImpl<BFloat16>(*bf16);
    return nullptr;
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
