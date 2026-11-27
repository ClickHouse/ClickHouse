#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class RemovingReplicatedColumnsTransform final : public ISimpleTransform
{
public:
    explicit RemovingReplicatedColumnsTransform(SharedHeader header);

    String getName() const override { return "RemovingReplicatedColumnsTransform"; }

protected:
    void transform(Chunk & chunk) override;
};

}
