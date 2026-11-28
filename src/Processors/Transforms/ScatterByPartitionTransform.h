#pragma once
#include <Columns/IColumn.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType.h>
#include <Processors/Chunk.h>
#include <Processors/IProcessor.h>
#include <Common/PODArray.h>

#include <memory>
#include <pcg_random.hpp>

namespace DB
{

struct ScatterByPartitionTransform : IProcessor
{
    /// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to
    /// before hashing. Casting is internal to routing; output rows are unchanged.
    /// `canonicalize_float_keys` hashes `-0.` as `0.` and every NaN as one NaN for the keys of `canonicalizesKeyType`.
    ScatterByPartitionTransform(
        SharedHeader header,
        size_t output_size_,
        ColumnNumbers key_columns_,
        DataTypes hash_cast_types_ = {},
        bool canonicalize_float_keys = false);

    /// `Float32` or `Float64`, also inside `Nullable`.
    static bool canonicalizesKeyType(const IDataType & type);

    /// Round-robin mode: each input chunk goes whole to the next output in turn, starting
    /// at `start_bucket`. For distribution without a placement requirement.
    static std::shared_ptr<ScatterByPartitionTransform> createRoundRobin(SharedHeader header, size_t output_size_, size_t start_bucket);

    /// Random mode: every row goes to a uniformly random output (used to shuffle the rows in parallel).
    static std::shared_ptr<ScatterByPartitionTransform> createRandom(SharedHeader header, size_t output_size_);

    String getName() const override { return "ScatterByPartitionTransform"; }

    Status prepare() override;
    void work() override;

    bool requiresAllOutputsPushable() const override { return true; }

private:

    void generateOutputChunks();
    /// Hands the scattered chunks to the outputs. Returns true once all of them are handed over.
    bool pushOutputChunks();

    size_t output_size;
    ColumnNumbers key_columns;
    DataTypes hash_input_types;
    DataTypes hash_cast_types;
    /// One flag per key: hash its canonical floats. Empty without `canonicalize_float_keys`.
    VectorWithMemoryTracking<char> canonical_float_keys;
    /// When set, chunks are routed round-robin starting from this output instead of by key hash.
    std::optional<size_t> round_robin_bucket;
    /// When set, every row goes to a random output.
    std::unique_ptr<pcg64> random_generator;

    bool has_output_chunks = false;
    VectorWithMemoryTracking<char> was_output_processed;
    Chunk chunk;

    PaddedPODArray<UInt32> hash;
    IColumn::Selector selector;
    Chunks output_chunks;
};

}
