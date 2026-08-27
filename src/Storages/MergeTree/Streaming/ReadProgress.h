#pragma once

#include <Processors/Chunk.h>

namespace DB
{

/// Cumulative delivery progress of a streaming read across the read rounds.
struct StreamReadProgress
{
    int64_t finished_rounds = 0;
    int64_t current_round_read_rows = 0;
    int64_t current_round_read_bytes = 0;
    int64_t overall_read_rows = 0;
    int64_t overall_read_bytes = 0;

public:
    void accountChunk(const Chunk & data);
    void accountRound();
};

}
