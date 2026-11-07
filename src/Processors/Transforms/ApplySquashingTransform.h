#pragma once
#include <Interpreters/Squashing.h>
#include <Processors/ISimpleTransform.h>
#include <Processors/Sinks/SinkToStorage.h>

namespace DB
{

class ApplySquashingTransform : public ExceptionKeepingTransform
{
public:
    explicit ApplySquashingTransform(SharedHeader header);

    String getName() const override { return "ApplySquashingTransform"; }

    void work() override;

protected:
    void onConsume(Chunk chunk) override;

    GenerateResult onGenerate() override;

private:
    Chunk cur_chunk;
};

}
