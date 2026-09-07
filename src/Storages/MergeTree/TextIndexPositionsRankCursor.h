#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>

#include <limits>
#include <span>
#include <vector>

namespace DB
{

class MergeTreeReaderStream;

/// Reads a document's positions by posting rank, its ordinal in the posting list, decoding each block once.
class TextIndexPositionsRankCursor
{
public:
    TextIndexPositionsRankCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_, UInt64 expected_num_docs);

    /// Valid until the next call.
    std::span<const UInt32> seek(UInt64 rank);

private:
    MergeTreeReaderStream * stream = nullptr;
    const TokenPostingsInfo * info = nullptr;
    TextIndexBlockedPositionsCodec::Directory directory;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;

    PaddedPODArray<UInt32> block_offsets;
    PaddedPODArray<UInt32> block_positions;
    std::vector<UInt32> all_local_ranks;
    size_t current_block = std::numeric_limits<size_t>::max();
};

}
