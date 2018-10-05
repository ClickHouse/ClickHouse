#pragma once

#include <Processors/IProcessor.h>


namespace DB
{


class ITransform : public IProcessor
{
protected:

    Blocks input_blocks;
    Blocks output_blocks;

    virtual Blocks transform(Blocks && blocks) = 0;

public:
    ITransform(Blocks input_headers, Blocks output_headers);

    Status prepare() override;
    void work() override;
};

}
