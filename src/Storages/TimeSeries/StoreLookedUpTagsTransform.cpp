#include <Storages/TimeSeries/StoreLookedUpTagsTransform.h>

#include <Common/Exception.h>
#include <Functions/TimeSeries/TimeSeriesTagsFunctionHelpers.h>
#include <Interpreters/ContextTimeSeriesTagsCollector.h>
#include <Storages/TimeSeries/StoreLookedUpTagsStep.h>


namespace DB
{

StoreLookedUpTagsTransform::StoreLookedUpTagsTransform(
    const SharedHeader & input_header_,
    const SharedHeader & output_header_,
    const size_t tags_start_index_,
    const std::shared_ptr<ContextTimeSeriesTagsCollector> & tags_collector_)
    : ISimpleTransform(input_header_, output_header_, /* skip_empty_chunks = */ true)
    , tags_start_index(tags_start_index_)
    , tags_collector(tags_collector_)
{
}

void StoreLookedUpTagsTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    const auto & input_header = getInputPort().getHeader();
    ColumnsWithTypeAndName arguments;
    arguments.reserve(columns.size());
    for (size_t i = 0; i != columns.size(); ++i)
    {
        const auto & header_column = input_header.getByPosition(i);
        arguments.emplace_back(columns[i], header_column.type, header_column.name);
    }

    try
    {
        tags_collector->storeTags(
            arguments.front().column,
            TimeSeriesTagsFunctionHelpers::extractTagNamesAndValuesFromArguments(
                StoreLookedUpTagsStep::FUNCTION_NAME, arguments, tags_start_index));
    }
    catch (Exception & e)
    {
        e.addMessage(" while storing tags and values. It's a bug, please report it.");

        throw;
    }

    columns.resize(tags_start_index);
    chunk.setColumns(std::move(columns), num_rows);
}

}
