#pragma once

#include <Processors/IProcessor.h>

namespace DB
{

class DelayedSource : public IProcessor
{
public:
    using Creator = std::function<Processors()>;

    DelayedSource(Block header, Creator processors_creator);
    String getName() const override { return "Delayed"; }

    Status prepare() override;
    void work() override;

private:
    Creator creator;
};

}
