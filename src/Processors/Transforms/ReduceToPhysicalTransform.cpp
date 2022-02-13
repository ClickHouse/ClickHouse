#include <Processors/Transforms/ReduceToPhysicalTransform.h>
#include <Common/PODArray.h>

namespace DB
{

Block ReduceToPhysicalTransform::transformToPhysicalBlock(const Block & header, const ColumnsDescription & columns)
{
    Block out = header.cloneWithoutColumns();

    for (const auto & column : columns.getEphemeral())
        if (out.has(column.name))
            out.erase(column.name);

    return out;
}

ReduceToPhysicalTransform::ReduceToPhysicalTransform(const Block & input_header, const ColumnsDescription & columns) :
        ISimpleTransform(input_header, transformToPhysicalBlock(input_header, columns), false)
{
    /// Find non-physical columns in input_header
    for (const auto & column : columns.getEphemeral())
        if (input_header.has(column.name))
            index.push_back(input_header.getPositionByName(column.name));

    std::sort(index.begin(), index.end(), std::greater());
}

void ReduceToPhysicalTransform::transform(Chunk & chunk)
{
    for (size_t i : index)
        chunk.erase(i);
}

}
