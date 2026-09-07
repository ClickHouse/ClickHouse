#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/TextIndexBlockedPositionsCodec.h>

#include <limits>
#include <span>
#include <vector>

namespace DB
{

class MergeTreeReaderStream;

/// The positions stream carries no document ids: it is addressed by posting rank, a document's ordinal in its posting list.
/// Reads one document's positions by rank, decoding a block once and serving every rank inside it.
class TextIndexPositionsRankCursor
{
public:
    TextIndexPositionsRankCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_, UInt64 expected_num_docs);

    /// Valid until the next call.
    std::span<const UInt32> seek(UInt64 rank);

    size_t blocksDecoded() const { return blocks_decoded; }

private:
    MergeTreeReaderStream * stream = nullptr;
    const TokenPostingsInfo * info = nullptr;
    TextIndexBlockedPositionsCodec::Directory directory;
    TextIndexBlockedPositionsCodec::DecodeScratch scratch;

    PaddedPODArray<UInt32> block_offsets;
    PaddedPODArray<UInt32> block_positions;
    std::vector<UInt32> all_local_ranks;
    size_t current_block = std::numeric_limits<size_t>::max();

    size_t blocks_decoded = 0;
};

}
