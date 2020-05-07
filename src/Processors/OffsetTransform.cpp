#include <Processors/OffsetTransform.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

OffsetTransform::OffsetTransform(
    const Block & header_, size_t offset_, size_t num_streams,
    bool always_read_till_end_, bool with_ties_,
    SortDescription description_)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , offset(offset_)
    , always_read_till_end(always_read_till_end_)
    , with_ties(with_ties_), description(std::move(description_))
{
    if (num_streams != 1 && with_ties)
        throw Exception("Cannot use OffsetTransform with multiple ports and ties.", ErrorCodes::LOGICAL_ERROR);

    ports_data.resize(num_streams);

    size_t cur_stream = 0;
    for (auto & input : inputs)
    {
        ports_data[cur_stream].input_port = &input;
        ++cur_stream;
    }

    cur_stream = 0;
    for (auto & output : outputs)
    {
        ports_data[cur_stream].output_port = &output;
        ++cur_stream;
    }

    for (const auto & desc : description)
    {
        if (!desc.column_name.empty())
            sort_column_positions.push_back(header_.getPositionByName(desc.column_name));
        else
            sort_column_positions.push_back(desc.column_number);
    }
}

Chunk OffsetTransform::makeChunkWithPreviousRow(const Chunk & chunk, size_t row) const
{
    assert(row < chunk.getNumRows());
    ColumnRawPtrs current_columns = extractSortColumns(chunk.getColumns());
    MutableColumns last_row_sort_columns;
    for (size_t i = 0; i < current_columns.size(); ++i)
    {
        last_row_sort_columns.emplace_back(current_columns[i]->cloneEmpty());
        last_row_sort_columns[i]->insertFrom(*current_columns[i], row);
    }
    return Chunk(std::move(last_row_sort_columns), 1);
}


IProcessor::Status OffsetTransform::prepare(
        const PortNumbers & updated_input_ports,
        const PortNumbers & updated_output_ports)
{
    bool has_full_port = false;

    auto process_pair = [&](size_t pos)
    {
        auto status = preparePair(ports_data[pos]);

        switch (status)
        {
            case IProcessor::Status::Finished:
            {
                if (!ports_data[pos].is_finished)
                {
                    ports_data[pos].is_finished = true;
                    ++num_finished_port_pairs;
                }

                return;
            }
            case IProcessor::Status::PortFull:
            {
                has_full_port = true;
                return;
            }
            case IProcessor::Status::NeedData:
                return;
            default:
                throw Exception(
                        "Unexpected status for OffsetTransform::preparePair : " + IProcessor::statusToName(status),
                        ErrorCodes::LOGICAL_ERROR);

        }
    };

    for (auto pos : updated_input_ports)
        process_pair(pos);

    for (auto pos : updated_output_ports)
        process_pair(pos);

    /// All ports are finished. It may happen even before we reached the limit (has less data then limit).
    if (num_finished_port_pairs == ports_data.size())
        return Status::Finished;

    if (has_full_port)
        return Status::PortFull;

    return Status::NeedData;
}

OffsetTransform::Status OffsetTransform::prepare()
{
    if (ports_data.size() != 1)
        throw Exception("prepare without arguments is not supported for multi-port OffsetTransform.",
                        ErrorCodes::LOGICAL_ERROR);

    return prepare({0}, {0});
}

OffsetTransform::Status OffsetTransform::preparePair(PortsData & data)
{
    auto & output = *data.output_port;
    auto & input = *data.input_port;

    /// Check can output.
    bool output_finished = false;
    if (output.isFinished())
    {
        output_finished = true;
    }

    if (!output_finished && !output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Check can input.

    if (input.isFinished())
    {
        output.finish();
        return Status::Finished;
    }

    input.setNeeded();
    if (!input.hasData()) {
        return Status::NeedData;
    }

    data.current_chunk = input.pull(true);

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Process block.

    rows_read += rows;

    if (rows_read < offset)
    {
        data.current_chunk.clear();

        if (input.isFinished())
        {
            output.finish();
            return Status::Finished;
        }

        /// Now, we pulled from input, and it must be empty.
        input.setNeeded();
        return Status::NeedData;
    }

    if (!(rows_read >= offset + rows))
        splitChunk(data);

    output.push(std::move(data.current_chunk));

    return Status::PortFull;
}


void OffsetTransform::splitChunk(PortsData & data)
{
    auto current_chunk_sort_columns = extractSortColumns(data.current_chunk.getColumns());
    size_t num_rows = data.current_chunk.getNumRows();
    size_t num_columns = data.current_chunk.getNumColumns();

    /// return a piece of the block
    size_t start = std::max(
        static_cast<Int64>(0),
        static_cast<Int64>(offset) - static_cast<Int64>(rows_read) + static_cast<Int64>(num_rows));

    size_t length = static_cast<Int64>(rows_read) - static_cast<Int64>(offset);

    if (length == num_rows)
        return;

    auto columns = data.current_chunk.detachColumns();

    for (size_t i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);

    data.current_chunk.setColumns(std::move(columns), length);
}


ColumnRawPtrs OffsetTransform::extractSortColumns(const Columns & columns) const
{
    ColumnRawPtrs res;
    res.reserve(description.size());
    for (size_t pos : sort_column_positions)
        res.push_back(columns[pos].get());

    return res;
}

bool OffsetTransform::sortColumnsEqualAt(const ColumnRawPtrs & current_chunk_sort_columns, size_t current_chunk_row_num) const
{
    assert(current_chunk_sort_columns.size() == previous_row_chunk.getNumColumns());
    size_t size = current_chunk_sort_columns.size();
    const auto & previous_row_sort_columns = previous_row_chunk.getColumns();
    for (size_t i = 0; i < size; ++i)
        if (0 != current_chunk_sort_columns[i]->compareAt(current_chunk_row_num, 0, *previous_row_sort_columns[i], 1))
            return false;
    return true;
}

}

