#pragma once

#include <Core/Types.h>


namespace DB
{
class IMergeTreeDataPart;

/// Generates the block identifier which is used to deduplicate inserted blocks.
/// For a parallel insert the same instance of InsertBlockIDGenerator must be shared between all sinks.
class InsertBlockIDGenerator
{
public:
    explicit InsertBlockIDGenerator(const String & insert_deduplication_token_);

    /// Generates the block identifier.
    String generateBlockID(const IMergeTreeDataPart & part);

private:
    const String insert_deduplication_token;

    /// Ordinal number of the current chunk.
    /// Multiple blocks can be inserted within the same insert query and so this ordinal number is added to the deduplication token
    /// to generate a distinctive block id for each block.
    /// During a parallel insert generateBlockID() can be called from multiple threads, that's why it's an atomic.
    std::atomic<UInt64> chunk_dedup_seqnum = 0;
};

using InsertBlockIDGeneratorPtr = std::shared_ptr<InsertBlockIDGenerator>;

}
