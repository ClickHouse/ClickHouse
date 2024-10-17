#include <Processors/ShuffleProcessor.h>
#include <Columns/ColumnsNumber.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

IProcessor::Status ShuffleProcessor::prepare(const PortNumbers & /*updated_inputs*/, const PortNumbers & updated_outputs)
{
    if (!initialized)
    {
        initialized = true;
        auto & input = inputs.front();
        input.setNeeded();
        input_port = {.port = &input, .status = InputStatus::NotActive};

        for (auto & output : outputs)
        {
            output_ports.push_back({.port = &output, .status = OutputStatus::NotActive});
        }        
    }

    for (const auto & output_number : updated_outputs)
    {
        auto & output = output_ports[output_number];
        if (output.port->isFinished())
        {
            if (output.status != OutputStatus::Finished)
            {
                --num_unfinished_outputs;
                output.status = OutputStatus::Finished;
            }

            continue;
        }

        if (output.port->canPush())
        {
            if (output.status != OutputStatus::NeedData)
            {
                output.status = OutputStatus::NeedData;
            }
        }
    }

    if (!num_unfinished_outputs)
    {
        for (auto & input : inputs)
            input.close();

        return Status::Finished;
    }

    if (std::any_of(output_ports.begin(), output_ports.end(), [](const auto & output) { return output.status == OutputStatus::PortFull; }))
    {
        return Status::PortFull;
    }

    if (input_port.port->isFinished())
    {
        if (input_port.status != InputStatus::Finished)
        {
            input_port.status = InputStatus::Finished;
            for (auto & output : outputs)
            {
                output.finish();
            }
            return Status::Finished;
        }
    }

    if (input_port.port->hasData())
    {
        input_port.status = InputStatus::HasData;

        // Shuffle input data
        auto data = input_port.port->pullData();
        assert(!input_port.port->hasData());
        const auto * shuffle_column = checkAndGetColumn<ColumnUInt64>(data.chunk.getColumns()[0].get()); // todo: 未来用参数指定依据哪一列做shuffle
        if (!shuffle_column)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Shuffle column must be UInt64, but {}",
                shuffle_column->getDataType());
        }

        for (size_t i = 0; i < thresholds.size(); ++i)
        {
            const auto & threshold = thresholds[i];
            shuffle_column = checkAndGetColumn<ColumnUInt64>(data.chunk.getColumns()[0].get()); // todo: 未来用参数指定依据哪一列做shuffle
            auto & raw_id_data = shuffle_column->getData();
            auto range_bound = std::lower_bound(raw_id_data.begin(), raw_id_data.end(), threshold);
            if (range_bound == raw_id_data.end())
            {
                // 整体数据块放入该桶
                output_ports[i].port->pushData(std::move(data));
                output_ports[i].status = OutputStatus::PortFull;
                break;
            }
            else
            {
                // 部分数据块放入该桶，其余的后续阶段处理。
                Columns source_columns = data.chunk.detachColumns();
                Columns res_columns;                
                size_t part_length = range_bound - raw_id_data.begin();                
                for (auto & source_column : source_columns)
                {
                    res_columns.push_back(source_column->cut(0, part_length));
                }
                output_ports[i].port->pushData({.chunk = Chunk(res_columns, part_length), .exception = {}});
                output_ports[i].status = OutputStatus::PortFull;

                size_t remain_length = raw_id_data.end() - range_bound;
                for (auto & source_column : source_columns)
                {
                    source_column = source_column->cut(part_length, remain_length);
                }
                data = {.chunk = Chunk(source_columns, remain_length), .exception = {}};
            }
        }
        if (data.chunk.hasRows())
        {
            // 放入最后一桶。
            output_ports.back().port->pushData(std::move(data));
            output_ports.back().status = OutputStatus::PortFull;
        }
        return Status::PortFull;
    }
    else
    {
        return Status::NeedData;
    }
}

}
