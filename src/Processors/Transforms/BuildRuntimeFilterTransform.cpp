#include <Processors/Transforms/BuildRuntimeFilterTransform.h>

#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Chunk.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

namespace ProfileEvents
{
    extern const Event FilterTransformPassedRows;
    extern const Event FilterTransformPassedBytes;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
}

IProcessor::Status BuildRuntimeFilterTransform::prepare()
{
    auto status = ISimpleTransform::prepare();

//    /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
//    if (status != IProcessor::Status::PortFull)
//        are_prepared_sets_initialized = true;
//
    if (status == IProcessor::Status::Finished)
        finish();

    return status;
}


void BuildRuntimeFilterTransform::transform(Chunk & chunk)
{
//    auto chunk_rows_before = chunk.getNumRows();
    doTransform(chunk);
}

void BuildRuntimeFilterTransform::doTransform(Chunk & chunk)
{
    ColumnPtr filter_column = chunk.getColumns()[filter_column_position];
    const size_t num_rows = chunk.getNumRows();
    for (size_t row = 0; row < num_rows; ++row)
    {
        /// TODO: make this efficient!
        auto value = filter_column->getDataAt(row);
        built_filter->add(value.data, value.size);
    }
}

void BuildRuntimeFilterTransform::finish()
{
    /// Save the filter
}

}
