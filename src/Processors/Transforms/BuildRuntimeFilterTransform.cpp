#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Functions/CastOverloadResolver.h>
#include <Functions/IFunction.h>


namespace DB
{

BuildRuntimeFilterTransform::BuildRuntimeFilterTransform(
    SharedHeader header_,
    String filter_column_name_,
    const DataTypePtr & filter_column_type_,
    String filter_name_,
    UInt64 exact_values_limit_,
    UInt64 bloom_filter_bytes_,
    UInt64 bloom_filter_hash_functions_)
    : ISimpleTransform(header_, header_, true)
    , filter_column_name(filter_column_name_)
    , filter_column_position(header_->getPositionByName(filter_column_name))
    , filter_column_original_type(header_->getByPosition(filter_column_position).type)
    , filter_column_target_type(filter_column_type_)
    , filter_name(filter_name_)
    , built_filter(std::make_unique<RuntimeFilter>(filter_column_target_type, exact_values_limit_, bloom_filter_bytes_, bloom_filter_hash_functions_))
{
    const auto & filter_column = header_->getByPosition(filter_column_position);
    if (!filter_column_target_type->equals(*filter_column_original_type))
        cast_to_target_type = createInternalCast(filter_column, filter_column_target_type, CastType::nonAccurate, {}, nullptr);
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
    /// Query context contains filter lookup where per-query filters are stored
    auto query_context = CurrentThread::get().getQueryContext();
    auto filter_lookup = query_context->getRuntimeFilterLookup();
    filter_lookup->add(filter_name, std::move(built_filter));
}

}
