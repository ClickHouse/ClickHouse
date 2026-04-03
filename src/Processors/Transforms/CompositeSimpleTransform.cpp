#include <Processors/Transforms/CompositeSimpleTransform.h>


namespace DB
{

CompositeSimpleTransform::CompositeSimpleTransform(SharedHeader header, std::vector<SimpleTransformPtr> transforms_)
    : ISimpleTransform(header, header, true)
    , transforms(std::move(transforms_))
{
}

void CompositeSimpleTransform::transform(Chunk & chunk)
{
    for (const auto & transform : transforms)
    {
        if (transform)
            transform->transform(chunk);
    }
}

}
