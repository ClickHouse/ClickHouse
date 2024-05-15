#pragma once
#include <cassert>
#include <memory>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>
#include "base/types.h"

namespace Poco
{
class Logger;
}

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

struct Side
{
    void addChunk(Chunk & chunk)
    {
        if (chunk.empty())
        {
            isFinished = true;
            return;
        }
        bytes_in_chunks += chunk.allocatedBytes();
        chunks.emplace_back(std::move(chunk));
    }

    bool isEmpty() const { return chunks.empty(); }

    std::pair<const Chunk &, UInt32> getChunk()
    {
        assert(already_joined_in_last_chunk < chunks.back().getNumRows());
        return {chunks.back(), already_joined_in_last_chunk};
    }

    void consume(UInt64 rows)
    {
        already_joined_in_last_chunk += rows;
        assert(already_joined_in_last_chunk <= chunks.back().getNumRows());
        while (!chunks.empty() && already_joined_in_last_chunk >= chunks.back().getNumRows())
        {
            chunks.pop_back();
            already_joined_in_last_chunk = 0;
        }
    }

    const Chunks & getAllChunks() { return chunks; }

    bool isFull() const { return isFinished; }

    UInt64 bytesInChunks() const { return bytes_in_chunks; }

private:
    Chunks chunks{};

    UInt64 bytes_in_chunks{0};
    UInt32 already_joined_in_last_chunk{0};
    bool isFinished{false};
};


enum class CrossJoinState : uint8_t
{
    MinimumBelowThreshold,
    ReadRightTable,
    ReadLeftTableAndJoin,
};

class CrossJoinAlgorithm final : public IMergingAlgorithm
{
public:
    CrossJoinAlgorithm(JoinPtr table_join, const Blocks & input_headers);

    const char * getName() const override { return "CrossJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

private:
    JoinPtr table_join;

    void changeOrder() noexcept;
    Chunk getRes(Chunk & left_table_res, Chunk & right_table_res) const;

    std::array<Side, 2> sides;
    std::array<Block, 2> headers;

    LoggerPtr log;
    const UInt64 max_bytes_to_swap_order;
    const size_t max_joined_block_rows;
    UInt32 right_table_num{1};
    UInt32 left_table_num{0};
    CrossJoinState state{CrossJoinState::MinimumBelowThreshold};
    UInt32 last_consumed_right_chunk{0};
};

class CrossJoinTransform final : public IMergingTransform<CrossJoinAlgorithm>
{
    using Base = IMergingTransform<CrossJoinAlgorithm>;

public:
    CrossJoinTransform(JoinPtr table_join, const Blocks & input_headers, const Block & output_header, UInt64 limit_hint = 0);

    String getName() const override { return "CrossJoinTransform"; }

protected:
    void onFinish() override;

    LoggerPtr log;
};

}
