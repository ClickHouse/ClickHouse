#pragma once

#include <DataTypes/Serializations/ISerialization.h>

#include <vector>

namespace DB
{

constexpr auto PROJECTION_INDEX_LARGE_POSTING_SUFFIX = ".pst";
constexpr auto PROJECTION_INDEX_INDEX_SUFFIX = ".pidx";
constexpr auto PROJECTION_INDEX_POSITION_SUFFIX = ".pos";

class MergedPartOffsets;

struct LargePostingListReaderStream;
using LargePostingListReaderStreamPtr = std::shared_ptr<LargePostingListReaderStream>;
using LargePostingListReaderStreamGetter = std::function<LargePostingListReaderStreamPtr(const ISerialization::SubstreamPath &)>;

struct DataTypePostingList;

struct ProjectionIndexDeserializationContext
{
    LargePostingListReaderStreamGetter large_posting_getter;
    LargePostingListReaderStreamGetter position_getter;
    LargePostingListReaderStreamGetter index_getter;
    UInt64 row_start = 0;
    UInt64 row_end = 0;

    /// When non-null, only deserialize posting data for rows at the given indices.
    /// Other rows are skipped cheaply (no heap allocation). Indices must be sorted.
    /// Set by MergeTreeProjectionIndexGranuleText after reading the term column.
    const std::vector<size_t> * matched_row_indices = nullptr;
};

struct LargePostingListWriterStream;
using LargePostingListWriterStreamGetter = std::function<LargePostingListWriterStream *(const ISerialization::SubstreamPath &)>;
struct ProjectionIndexSerializationContext
{
    LargePostingListWriterStreamGetter large_posting_getter;
    LargePostingListWriterStreamGetter position_getter;
    LargePostingListWriterStreamGetter index_getter;
};

}
