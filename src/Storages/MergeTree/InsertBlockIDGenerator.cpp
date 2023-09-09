#include <Storages/MergeTree/InsertBlockIDGenerator.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>


namespace DB
{

InsertBlockIDGenerator::InsertBlockIDGenerator(const String & insert_deduplication_token_)
    : insert_deduplication_token(insert_deduplication_token_)
{
}

String InsertBlockIDGenerator::generateBlockID(const IMergeTreeDataPart & part)
{
    String block_dedup_token;

    if (!insert_deduplication_token.empty())
    {
        /// We add a simple incrementing counter to the deduplication token to generate a distinctive block id for each block.
        block_dedup_token = fmt::format("{}_{}", insert_deduplication_token, chunk_dedup_seqnum++);
    }

    /// If `insert_deduplication_token` is not empty then we concatenate the partition identifier and the hash of `block_dedup_token`.
    /// Otherwise we concatenate the partition identifier and the hash of the part's data.
    /// In this case the same data can't be inserted to the same partition twice.
    return part.getZeroLevelPartBlockID(block_dedup_token);
}

}
