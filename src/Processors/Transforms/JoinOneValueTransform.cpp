#include <Processors/Transforms/JoinOneValueTransform.h>

namespace DB {

JoinOneValueTransform::JoinOneValueTransform(const Blocks & headers, const Block & output_header)
    : IProcessor(InputPorts(headers.begin(), headers.end()), OutputPorts(1, output_header)) {
    if (headers.size() != 2)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JoinOneValueTransform expects 2 inputs, got {}", headers.size());
    if (output_header.columns() != headers[0].columns() + 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JoinOneValueTransform expects output header to have one more column than input headers");
    if (headers[1].columns() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JoinOneValueTransform expects second input to have one column, got {}", headers[1].columns());

    auto insert_ptr = std::find(output_header.begin(), output_header.end(), headers[1].getByPosition(0));

    if (insert_ptr == output_header.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "JoinOneValueTransform expects second input to be in output header");

    right_idx = insert_ptr - output_header.begin();
}

IProcessor::Status JoinOneValueTransform::prepare()
{
    Status status = Status::Ready;

    while (status == Status::Ready)
    {
        if (!has_right_data) {
            status = prepareConsumeRight();
        }
        else if (!has_data) {
            status = prepareConsume();
        } else {
            status = prepareGenerate();
        }
    }

    return status;
}

IProcessor::Status JoinOneValueTransform::prepareConsumeRight() {
    if (inputs.back().hasData()) {
        has_right_data = true;
        right_chunk = inputs.back().pull();
        inputs.back().setNotNeeded();
        inputs.front().setNeeded();
        return Status::Ready;
    }

    if (inputs.back().isFinished()) {
        inputs.front().close();
        outputs.front().finish();
        return Status::Finished;
    }

    inputs.front().setNotNeeded();
    inputs.back().setNeeded();
    return Status::NeedData;
}

IProcessor::Status JoinOneValueTransform::prepareGenerate() {
    if (outputs.front().canPush()) {
        auto columnToInsert = IColumn::mutate(right_chunk.getColumns().front());
        columnToInsert->insertManyFrom(*columnToInsert.get(), 0, left_chunk.getNumRows() - 1);

        left_chunk.addColumn(right_idx, std::move(columnToInsert));
        outputs.front().push(std::move(left_chunk));
        has_data = false;

        return Status::Ready;
    }

    if (outputs.front().isFinished()) {
        inputs.front().close();
        inputs.back().close();

        return Status::Finished;
    }

    return Status::PortFull;
}

IProcessor::Status JoinOneValueTransform::prepareConsume() {
    if (inputs.front().hasData()) {
        has_data = true;
        left_chunk = inputs.front().pull();
        return Status::Ready;
    }

    if (inputs.front().isFinished()) {
        inputs.back().close();
        outputs.front().finish();
        return Status::Finished;
    }

    return Status::NeedData;
}

}
