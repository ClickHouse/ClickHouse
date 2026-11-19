#include <Processors/Sources/LazyReadFromFileGroupsSource.h>
#include <Processors/Formats/AggregateFunctionStatesFromValuesInputFormat.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatSettings.h>
#include <Common/typeid_cast.h>

#include <algorithm>

namespace DB
{

namespace
{

std::optional<size_t> countLeafColumnsOfType(const IDataType & type)
{
    if (isObject(type) || isDynamic(type))
        return std::nullopt;
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return countLeafColumnsOfType(*nullable->getNestedType());
    if (const auto * low_cardinality = typeid_cast<const DataTypeLowCardinality *>(&type))
        return countLeafColumnsOfType(*low_cardinality->getDictionaryType());
    if (const auto * array = typeid_cast<const DataTypeArray *>(&type))
        return countLeafColumnsOfType(*array->getNestedType());
    if (const auto * map = typeid_cast<const DataTypeMap *>(&type))
    {
        const auto keys = countLeafColumnsOfType(*map->getKeyType());
        const auto values = countLeafColumnsOfType(*map->getValueType());
        if (!keys || !values)
            return std::nullopt;
        return *keys + *values;
    }
    if (const auto * tuple = typeid_cast<const DataTypeTuple *>(&type))
    {
        size_t leaves = 0;
        for (const auto & element : tuple->getElements())
        {
            const auto element_leaves = countLeafColumnsOfType(*element);
            if (!element_leaves)
                return std::nullopt;
            leaves += *element_leaves;
        }
        return std::max<size_t>(leaves, 1);
    }
    return 1;
}

}

std::optional<size_t> countLeafColumns(const Block & header)
{
    size_t leaves = 0;
    for (const auto & column : header)
    {
        const auto column_leaves = countLeafColumnsOfType(*column.type);
        if (!column_leaves)
            return std::nullopt;
        leaves += *column_leaves;
    }
    return leaves;
}

std::optional<size_t> countLeafColumnsToRead(const Block & format_header, const FormatSettings & format_settings, bool is_vortex)
{
    if (is_vortex)
        return std::nullopt;
    const auto header_to_parse
        = AggregateFunctionStatesFromValuesInputFormat::getHeaderToParse(format_header, format_settings.aggregate_function_input_format);
    return countLeafColumns(header_to_parse ? *header_to_parse : format_header);
}

LazyReadFromFileGroupsSource::LazyReadFromFileGroupsSource(SharedHeader header, size_t max_threads_, size_t main_read_leaf_columns_)
    : IProcessor({}, {std::move(header)})
    , max_threads(max_threads_)
    , main_read_leaf_columns(main_read_leaf_columns_)
    , next_input_to_process(inputs.end())
{
}

size_t LazyReadFromFileGroupsSource::getNumReaders(size_t main_read_streams, std::optional<size_t> lazy_read_leaf_columns, size_t num_files) const
{
    main_read_streams = std::clamp<size_t>(main_read_streams, 1, std::max<size_t>(max_threads, 1));
    const size_t max_streams_for_columns = lazy_read_leaf_columns
        ? std::max<size_t>(main_read_streams * main_read_leaf_columns / std::max<size_t>(*lazy_read_leaf_columns, 1), 1)
        : 1;
    return std::min({std::max<size_t>(max_threads, 1), max_streams_for_columns, num_files});
}

IProcessor::PipelineUpdate LazyReadFromFileGroupsSource::addReaders(Processors readers)
{
    for (const auto & reader : readers)
    {
        auto & reader_output = reader->getOutputs().front();
        inputs.emplace_back(reader_output.getHeader(), this);
        connect(reader_output, inputs.back());
        inputs.back().setNeeded();
        const size_t input_index = input_port_to_index.size();
        input_port_to_index[&inputs.back()] = input_index;
    }

    next_input_to_process = inputs.begin();
    chunks.resize(readers.size());
    readers_added = true;
    return PipelineUpdate{.to_add = std::move(readers), .to_remove = {}};
}

IProcessor::Status LazyReadFromFileGroupsSource::prepare(const UpdatedInputPorts & updated_input_ports, const UpdatedOutputPorts & /*updated_output_ports*/)
{
    auto & output = outputs.front();
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    /// Pull the chunk of every updated reader before checking the output (an updated port is reported once,
    /// and a reader whose chunk stays in its port stops), but push the chunks strictly in the order of the groups.
    if (next_input_to_process != inputs.end())
    {
        for (InputPort * input_port : updated_input_ports)
        {
            auto & reader_chunks = chunks[input_port_to_index.at(input_port)];
            if (!input_port->isFinished() && input_port->hasData())
                reader_chunks.emplace_back(input_port->pull());
        }
    }

    if (!output.canPush())
        return Status::PortFull;

    if (!readers_added)
        return Status::UpdatePipeline;

    while (next_input_to_process != inputs.end())
    {
        auto & input = *next_input_to_process;
        auto & pending = chunks[next_chunk_to_process];

        if (!pending.empty())
        {
            output.push(std::move(pending.front()));
            pending.pop_front();
            return Status::PortFull;
        }

        if (input.isFinished())
        {
            ++next_input_to_process;
            ++next_chunk_to_process;
            continue;
        }

        if (!input.hasData())
            return Status::NeedData;

        output.push(input.pull());
        return Status::PortFull;
    }

    /// All the readers have finished, or no rows survived the LIMIT and there is nothing to read.
    output.finish();
    return Status::Finished;
}

}
