#pragma once
#include "Processors/IProcessor.h"

namespace DB
{
// This transformation expects only one row in the right input.
// The value of each column in that row is added to each row in the left input.

class JoinRowTransform : public IProcessor
{
public:
    JoinRowTransform(const Blocks & headers, const Block & output_header);

    String getName() const override { return "JoinRowValue"; }
    Status prepare() override;

private:
    Chunk left_chunk;
    Chunk right_chunk;
    bool has_data = false;
    bool has_right_data = false;
    std::vector<std::pair<size_t, bool>> output_to_inputs_index_map; // if bool == true then column is from right_chunk

    Status prepareGenerate();
    Status prepareConsume();
    Status prepareConsumeRight();
};

}
