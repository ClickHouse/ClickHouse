#pragma once

#include <Storages/MergeTree/MergeTreeIndices.h>
#include <base/types.h>
#include <vector>

namespace DB
{

struct MergeTreeIndexTextParams
{
    size_t dictionary_block_size = 0;
    size_t dictionary_block_frontcoding_compression = 1;
    size_t posting_list_block_size = 1024 * 1024;
    String preprocessor;
    bool enable_postings_compression = false;
};

using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

/// Closed range of rows.
struct RowsRange
{
    size_t begin;
    size_t end;

    RowsRange() = default;
    RowsRange(size_t begin_, size_t end_) : begin(begin_), end(end_) {}

    bool intersects(const RowsRange & other) const;
};

/// Stores information about posting list for a token.
struct TokenPostingsInfo
{
    UInt64 header = 0;
    UInt32 cardinality = 0;
    std::vector<UInt64> offsets;
    std::vector<RowsRange> ranges;
    PostingListPtr embedded_postings;

    /// Returns indexes of posting list blocks to read for the given range of rows.
    std::vector<size_t> getBlocksToRead(const RowsRange & range) const;
};

}
