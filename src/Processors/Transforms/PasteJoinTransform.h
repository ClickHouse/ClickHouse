#pragma once
#include <cassert>
#include <cstddef>
#include <memory>
#include <mutex>
#include <utility>

#include <boost/core/noncopyable.hpp>

#include <Common/PODArray.h>
#include <IO/ReadBuffer.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Chunk.h>
#include <Processors/Merges/Algorithms/IMergingAlgorithm.h>
#include <Processors/Merges/IMergingTransform.h>

namespace Poco { class Logger; }

namespace DB
{

class IJoin;
using JoinPtr = std::shared_ptr<IJoin>;

/*
 * This class is used to join chunks from two sorted streams.
 * It is used in MergeJoinTransform.
 */
class PasteJoinAlgorithm final : public IMergingAlgorithm
{
public:
    explicit PasteJoinAlgorithm(JoinPtr table_join, const Blocks & input_headers, size_t max_block_size_);

    const char * getName() const override { return "PasteJoinAlgorithm"; }
    void initialize(Inputs inputs) override;
    void consume(Input & input, size_t source_num) override;
    Status merge() override;
    MergedStats getMergedStats() const override;

private:
    Chunk createBlockWithDefaults(size_t source_num);
    Chunk createBlockWithDefaults(size_t source_num, size_t start, size_t num_rows) const;

    /// For `USING` join key columns should have values from right side instead of defaults
    std::unordered_map<size_t, size_t> left_to_right_key_remap;

    std::array<Chunk, 2> chunks;

    JoinPtr table_join;

    size_t max_block_size;

    struct Statistic
    {
        size_t num_blocks[2] = {0, 0};
        size_t num_rows[2] = {0, 0};
        size_t num_bytes[2] = {0, 0};

        size_t max_blocks_loaded = 0;
    };

    Statistic stat;

    LoggerPtr log;
    UInt64 last_used_row[2] = {0, 0};
};

class PasteJoinTransform final : public IMergingTransform<PasteJoinAlgorithm>
{
    using Base = IMergingTransform<PasteJoinAlgorithm>;

public:
    PasteJoinTransform(
        JoinPtr table_join,
        const Blocks & input_headers,
        const Block & output_header,
        size_t max_block_size,
        UInt64 limit_hint = 0);

    String getName() const override { return "PasteJoinTransform"; }

protected:
    void onFinish() override;

    LoggerPtr log;
};

}
