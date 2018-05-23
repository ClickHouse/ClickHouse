#pragma once

#include <Processors/IProcessor.h>


namespace DB
{

class ResizeProcessor : public IProcessor
{
public:
    using IProcessor::IProcessor;

    String getName() const override { return "Resize"; }

    Status prepare() override;
};

}
