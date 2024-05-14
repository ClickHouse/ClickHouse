#pragma once
#include <memory>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace Poco { class Logger; }

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

class CrossJoinAlgorithm final : public IMergingAlgorithm
{
public:
    explicit CrossJoinAlgorithm(JoinPtr table_join, const Blocks & input_headers, size_t max_block_size_);

    const char * getName() const override { return "CrossJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;

private:
    Chunk createBlockWithDefaults(size_t source_num);
    Chunk createBlockWithDefaults(size_t source_num, size_t start, size_t num_rows) const;

    JoinPtr table_join;

    bool isSwapped{false};

    void swap() noexcept;

    size_t getRealIndex(size_t num) const;
    
    std::array<Chunks, 2> chunks;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};

        size_t max_blocks_loaded = 0;
    };

    Statistic stat;
    LoggerPtr log;
    size_t max_block_size;
};

class CrossJoinTransform final : public IMergingTransform<CrossJoinAlgorithm>
{
    using Base = IMergingTransform<CrossJoinAlgorithm>;

public:
    CrossJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint = 0);

    String getName() const override { return "CrossJoinTransform"; }

protected:
    void onFinish() override;

    LoggerPtr log;
};

}
