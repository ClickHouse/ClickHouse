#pragma once
#include <Processors/ISimpleTransform.h>

namespace DB
{

class ExtremesTransform : public ISimpleTransform
{

public:
    explicit ExtremesTransform(const Block & header);

    String getName() const override { return "ExtremesTransform"; }

    OutputPort & getExtremesPort() { return outputs.back(); }

    Status prepare() override;
    void work() override;

protected:
    void transform(Chunk & chunk) override;

    bool finished_transform = false;
    Chunk extremes;

private:
    MutableColumns extremes_columns;
};

}

