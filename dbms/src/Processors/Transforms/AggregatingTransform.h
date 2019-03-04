#pragma once
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/Aggregator.h>
#include <IO/ReadBufferFromFile.h>
#include <Compression/CompressedReadBuffer.h>

namespace DB
{

class AggregatedChunkInfo : public ChunkInfo
{
public:
    bool is_overflows = false;
    Int32 bucket_num = -1;
};

class IBlockInputStream;
using BlockInputStreamPtr = std::shared_ptr<IBlockInputStream>;

struct AggregatingTransformParams
{
    Aggregator::Params params;
    Aggregator aggregator;
    bool final;

    AggregatingTransformParams(const Aggregator::Params & params, bool final)
        : params(params), aggregator(params), final(final) {}

    Block getHeader() const { return aggregator.getHeader(final); }
};

using AggregatingTransformParamsPtr = std::unique_ptr<AggregatingTransformParams>;

class AggregatingTransform : public IAccumulatingTransform
{
public:
    AggregatingTransform(Block header, AggregatingTransformParamsPtr params_);
    ~AggregatingTransform() override;

    String getName() const override { return "AggregatingTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    /// To read the data that was flushed into the temporary data file.
    struct TemporaryFileStream
    {
        ReadBufferFromFile file_in;
        CompressedReadBuffer compressed_in;
        BlockInputStreamPtr block_in;

        TemporaryFileStream(const std::string & path);
    };

    AggregatingTransformParamsPtr params;
    Logger * log = &Logger::get("AggregatingTransform");

    StringRefs key;
    ColumnRawPtrs key_columns;
    Aggregator::AggregateColumns aggregate_columns;
    bool no_more_keys = false;

    AggregatedDataVariantsPtr data_variants;

    std::vector<std::unique_ptr<TemporaryFileStream>> temporary_inputs;
    std::unique_ptr<IBlockInputStream> impl;

    /// TODO: calculate time only for aggregation.
    Stopwatch watch;

    UInt64 src_rows = 0;
    UInt64 src_bytes = 0;

    bool is_generate_initialized = false;

    void initGenerate();
};

}
