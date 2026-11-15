#include <Processors/Transforms/NestedElementsValidationTransform.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

NestedElementsValidationTransform::NestedElementsValidationTransform(SharedHeader header) : ISimpleTransform(header, header, false)
{
}

void NestedElementsValidationTransform::transform(Chunk & chunk)
{
    Nested::validateArraySizes(getOutputPort().getHeader().cloneWithColumns(chunk.getColumns()));
}

}
