#pragma once
#include "Processors/IProcessor.h"

namespace DB {
    class JoinOneValueTransform : public IProcessor
    {
    public:
        JoinOneValueTransform(const Blocks & headers, const Block & output_header);

        String getName() const override { return "JoinOneValue"; }
        Status prepare() override;

    private:
        Chunk left_chunk;
        Chunk right_chunk;
        bool has_data = false;
        bool has_right_data = false;
        size_t right_idx;

        Status prepareGenerate();
        Status prepareConsume();
        Status prepareConsumeRight();
    };

}
