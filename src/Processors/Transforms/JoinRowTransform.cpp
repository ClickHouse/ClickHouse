#include <Processors/Transforms/JoinRowTransform.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

JoinRowTransform::JoinRowTransform(const Blocks & headers, const Block & output_header)
    : IProcessor(InputPorts(headers.begin(), headers.end()), OutputPorts(1, output_header))
{
    if (headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinRowTransform expects 2 inputs, got {}", headers.size());
    if (output_header.columns() > headers[0].columns() + headers[1].columns())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "JoinRowTransform expects output header to have equal or less columns than input headers");


    auto left_idx_map = headers[0].getNamesToIndexesMap();
    auto right_idx_map = headers[1].getNamesToIndexesMap();

    for (const auto & column : output_header)
    {
        if (left_idx_map.find(column.name) != left_idx_map.end())
        {
            output_to_inputs_index_map.emplace_back(left_idx_map[column.name], false);
        }
        else if (right_idx_map.find(column.name) != right_idx_map.end())
        {
            output_to_inputs_index_map.emplace_back(right_idx_map[column.name], true);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "JoinRowTransform expects output header to have columns from input headers");
        }
    }
}

IProcessor::Status JoinRowTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        if (!has_right_data)
        {
            status = prepareConsumeRight();
        }
        else if (!has_data)
        {
            status = prepareConsume();
        }
        else
        {
            status = prepareGenerate();
        }
    }

    return status;
}

IProcessor::Status JoinRowTransform::prepareConsumeRight()
{
    if (inputs.back().hasData())
    {
        has_right_data = true;
        right_chunk = inputs.back().pull();
        inputs.back().setNotNeeded();
        inputs.front().setNeeded();
        return Status::Ready;
    }

    if (inputs.back().isFinished())
    {
        inputs.front().close();
        outputs.front().finish();
        return Status::Finished;
    }

    inputs.front().setNotNeeded();
    inputs.back().setNeeded();
    return Status::NeedData;
}

IProcessor::Status JoinRowTransform::prepareGenerate()
{
    if (outputs.front().canPush())
    {
        Chunk output_chunk;
        for (const auto& [idx, right] : output_to_inputs_index_map)
        {
            if (right)
            {
                auto columnToInsert = IColumn::mutate(right_chunk.getColumns()[idx]);
                columnToInsert->insertManyFrom(*columnToInsert.get(), 0, left_chunk.getNumRows() - 1);
                output_chunk.addColumn(std::move(columnToInsert));
            }
            else
            {
                output_chunk.addColumn(left_chunk.getColumns()[idx]);
            }
        }

        outputs.front().push(std::move(output_chunk));
        has_data = false;

        return Status::Ready;
    }

    if (outputs.front().isFinished())
    {
        inputs.front().close();
        inputs.back().close();

        return Status::Finished;
    }

    return Status::PortFull;
}

IProcessor::Status JoinRowTransform::prepareConsume()
{
    if (inputs.front().hasData())
    {
        has_data = true;
        left_chunk = inputs.front().pull();
        return Status::Ready;
    }

    if (inputs.front().isFinished())
    {
        inputs.back().close();
        outputs.front().finish();
        return Status::Finished;
    }

    return Status::NeedData;
}

}
