#pragma once

#include <Processors/ISimpleTransform.h>
#include <vector>
#include <memory>


namespace DB
{

/// Combines multiple ISimpleTransform instances, applying them sequentially
class CompositeSimpleTransform : public ISimpleTransform
{
public:
    using SimpleTransformPtr = std::shared_ptr<ISimpleTransform>;

    CompositeSimpleTransform(SharedHeader header, std::vector<SimpleTransformPtr> transforms_);

    String getName() const override { return "CompositeSimpleTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    std::vector<SimpleTransformPtr> transforms;
};

}
