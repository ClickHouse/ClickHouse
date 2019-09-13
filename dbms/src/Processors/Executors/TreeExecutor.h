#pragma once
#include <DataStreams/IBlockInputStream.h>
#include <Processors/IProcessor.h>

namespace DB
{

class TreeExecutor : public IBlockInputStream
{
public:
    explicit TreeExecutor(Processors processors_) : processors(std::move(processors_)) { init(); }

    String getName() const override { return root->getName(); }
    Block getHeader() const override { return root->getOutputs().front().getHeader(); }

protected:
    Block readImpl() override;

private:
    Processors processors;
    IProcessor * root = nullptr;
    std::unique_ptr<InputPort> port;

    void init();
    void execute();
};

}
