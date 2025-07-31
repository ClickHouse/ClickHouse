#include <Processors/Transforms/BuildRuntimeFilterTransform.h>
#include <Processors/Chunk.h>
#include <Columns/IColumn.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>


// TODO: add profile events for filtered rows

namespace DB
{

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
    /// Query context contains filter lookup where per-query filters are stored
    /// TODO: Is this the right way to get query context?
    auto query_context = CurrentThread::get().getQueryContext();
    auto filter_lookup = query_context->getRuntimeFilterLookup();
    filter_lookup->add(filter_name, std::move(built_filter));
}

}
