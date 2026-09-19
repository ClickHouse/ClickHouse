#pragma once

#include <Storages/MergeTree/MergeTreeIndexText.h>
#include <Storages/MergeTree/PostingListBlockCodec.h>

#include <memory>
#include <vector>

namespace DB
{

class MergeTreeReaderStream;

/// Walks a token's posting list, reporting each document's rank (its ordinal in the list) without materialising the list.
class TextIndexPostingsRankCursor
{
public:
    TextIndexPostingsRankCursor(MergeTreeReaderStream & stream_, const TokenPostingsInfo & info_);
    /// Over a flat posting list (embedded or raw), where a document's rank is its index.
    explicit TextIndexPostingsRankCursor(std::vector<UInt32> docs);

    bool valid() const { return is_valid; }
    UInt32 docId() const { return current_doc_id; }
    UInt64 rank() const { return segment_first_rank + block_first_rank + index_in_block; }

    void next();
    /// Positions the cursor on the first document >= target, or invalidates it.
    void advance(UInt32 target);

private:
    struct Segment
    {
        UInt64 doc_count = 0;
        UInt32 first_row_id = 0;
        IPostingListCodec::Type codec_type = IPostingListCodec::Type::None;
        std::vector<UInt32> block_last_row_ids;
        std::vector<UInt64> block_offsets;
        std::vector<uint8_t> payload;
        size_t blockCount() const { return block_last_row_ids.size(); }
    };

    struct SegmentHeader
    {
        IPostingListCodec::Type codec_type = IPostingListCodec::Type::None;
        UInt64 payload_bytes = 0;
        UInt32 doc_count = 0;
        UInt32 first_row_id = 0;
    };

    /// Leaves the stream at the payload. Needs the ranks of the preceding segments to check this one's.
    SegmentHeader readSegmentHeader(size_t segment_idx);
    /// Sums the document counts each segment header already records, up to `segment_idx`.
    void ensureSegmentRank(size_t segment_idx);
    void loadSegment(size_t segment_idx);
    void decodeBlock(size_t block_idx);
    bool seekWithinLoadedSegment(UInt32 target);

    MergeTreeReaderStream * stream = nullptr;
    const TokenPostingsInfo * info = nullptr;

    size_t total_segments = 0;
    /// segment_ranks[i] is the rank of segment i's first document; entries below `ranks_known` are filled.
    std::vector<UInt64> segment_ranks;
    size_t ranks_known = 1;

    Segment segment;
    size_t current_segment_idx = 0;
    bool has_segment = false;

    size_t current_block = 0;
    size_t decoded_count = 0;
    size_t index_in_block = 0;
    UInt64 segment_first_rank = 0;
    UInt64 block_first_rank = 0;
    UInt32 current_doc_id = 0;
    bool is_valid = false;
    bool is_flat = false;

    std::unique_ptr<IPostingListBlockCodec> block_codec;
    std::vector<UInt32> decoded_doc_ids;
};

}
