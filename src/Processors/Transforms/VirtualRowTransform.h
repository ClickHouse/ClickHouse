#pragma once

#include <Processors/IInflatingTransform.h>
#include <Processors/ISimpleTransform.h>

namespace DB
{

class VirtualRowTransform : public IInflatingTransform
{
public:
    explicit VirtualRowTransform(const Block & header);

    String getName() const override { return "VirtualRowTransform"; }

    Status prepare() override;

protected:
    void consume(Chunk chunk) override;
    bool canGenerate() override;
    Chunk generate() override;

private:
    bool is_first = false;
    Chunk temp_chunk;
};

}
