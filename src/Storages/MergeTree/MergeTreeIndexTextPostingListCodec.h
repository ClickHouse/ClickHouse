#pragma once
#include <Common/Exception.h>
#include <roaring/roaring.hh>

namespace DB
{
struct TokenPostingsInfo;
class WriteBuffer;
class ReadBuffer;
using PostingList = roaring::Roaring;
struct PostingListBuilder;

/// Codec for serializing/deserializing a single postings list to/from a binary stream.
struct PostingListCodec
{
    /// Serializes a postings list into a `Write buffer`.
    /// Serialization is segment-oriented and controlled by `posting_list_block_size`:
    /// if a postings list is long, it is split into multiple consecutive segments, each containing
    /// up to `posting_list_block_size` values.
    static void encode(const PostingListBuilder &, size_t posting_list_block_size, TokenPostingsInfo &, WriteBuffer &);
    /// Performs the inverse operation, read from `ReadBuffer` and reconstructs the in-memory `PostingList`.
    static void decode(ReadBuffer &, PostingList &);
};
}
