#include <Processors/OffsetTransform.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

OffsetTransform::OffsetTransform(
    const Block & header_, UInt64 offset_, bool is_offset_positive_, size_t num_streams)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , offset(offset_), is_offset_positive(is_offset_positive_)
{
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
    if (!input.hasData())
        return Status::NeedData;

    data.current_chunk = input.pull(true);

    auto rows = data.current_chunk.getNumRows();

    if (rows_before_limit_at_least)
        rows_before_limit_at_least->add(rows);

    /// Process block.

    rows_read += rows;

    if (rows_read <= offset)
    {
        reverse_chunks.emplace_back(data.current_chunk.getColumns());
        reverse_chunks_size.emplace_back(data.current_chunk.getNumRows());
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
    if (is_offset_positive)
    {
        if (!(rows <= std::numeric_limits<UInt64>::max() - offset && rows_read >= offset + rows))
            splitChunk(data);
    }
    else
    {
        if (rows == 8192 || rows == 16384 || rows == 32768 || rows == 65536 || rows == 65505)
        {
            reverse_chunks.emplace_back(data.current_chunk.getColumns());
            reverse_chunks_size.emplace_back(data.current_chunk.getNumRows());
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
        else
        {
            reverse_chunks.emplace_back(data.current_chunk.getColumns());
            reverse_chunks_size.emplace_back(data.current_chunk.getNumRows());
        }

        MutableColumns whole_columns;
        ColumnRawPtrs current_columns;
        for (auto column : reverse_chunks.back())
            current_columns.push_back(column.get());

        for (auto j = 0u; j < current_columns.size(); ++j)
            whole_columns.emplace_back(current_columns[j]->cloneEmpty());

        for (int i = reverse_chunks.size() - 1; i >= 0; --i)
        {
            UInt64 inversion_rows = reverse_chunks_size[i];
            reverse_rows_read += inversion_rows;
            if (reverse_rows_read <= offset)
                continue;

            if (!(inversion_rows <= std::numeric_limits<UInt64>::max() - offset && reverse_rows_read >= offset + inversion_rows))
            {
                MutableColumns res_columns;
                for (auto j = 0u; j < current_columns.size(); ++j)
                    res_columns.emplace_back(current_columns[j]->cloneEmpty());
                data.current_chunk.setColumns(reverse_chunks[i], inversion_rows);

                splitChunk(data);
                
                for (auto j = 0u; j < current_columns.size(); ++j)
                {
                    res_columns[j]->insertRangeFrom(*data.current_chunk.getColumns()[j], 0, data.current_chunk.getNumRows());
                    res_columns[j]->insertRangeFrom(*whole_columns[j], 0, whole_columns[j]->size());
                }
                whole_columns.swap(res_columns);
            }
            else
            {
                MutableColumns res_columns;
                for (auto j = 0u; j < current_columns.size(); ++j)
                    res_columns.emplace_back(current_columns[j]->cloneEmpty());
                
                for (auto j = 0u; j < current_columns.size(); ++j)
                {
                    res_columns[j]->insertRangeFrom(*reverse_chunks[i][j], 0, reverse_chunks_size[i]);
                    res_columns[j]->insertRangeFrom(*whole_columns[j], 0, whole_columns[j]->size());
                }
                whole_columns.swap(res_columns);
            }
        }
        data.current_chunk.setColumns(std::move(whole_columns), whole_columns[0]->size());
    }

    output.push(std::move(data.current_chunk));

    return Status::PortFull;
}


void OffsetTransform::splitChunk(PortsData & data) const
{
    UInt64 num_rows = data.current_chunk.getNumRows();
    UInt64 num_columns = data.current_chunk.getNumColumns();

    /// return a piece of the block
    UInt64 start = 0;

    /// ------------[....(.....]
    /// <----------------------> rows_read
    ///             <----------> num_rows
    /// <---------------> offset
    ///             <---> start


    UInt64 length = 0;
    if (is_offset_positive)
    {
        assert(offset < rows_read);
        if (offset + num_rows > rows_read)
            start = offset + num_rows - rows_read;
        else
            return;
        length = num_rows - start;
    }
    else
    {
        assert(offset < reverse_rows_read);
        if (offset + num_rows > reverse_rows_read)
            start = 0;
        else
            return;
        length = reverse_rows_read - offset;
    }

    auto columns = data.current_chunk.detachColumns();

    for (UInt64 i = 0; i < num_columns; ++i)
        columns[i] = columns[i]->cut(start, length);
    data.current_chunk.setColumns(std::move(columns), length);
}

}

