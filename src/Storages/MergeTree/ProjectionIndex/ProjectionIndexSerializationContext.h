#pragma once

#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

constexpr auto PROJECTION_INDEX_LARGE_POSTING_SUFFIX = ".lpst";

class MergedPartOffsets;

struct LargePostingListReaderStream;
using LargePostingListReaderStreamPtr = std::shared_ptr<LargePostingListReaderStream>;
using LargePostingListReaderStreamGetter = std::function<LargePostingListReaderStreamPtr (const ISerialization::SubstreamPath &)>;

struct DataTypePostingList;

struct ProjectionIndexDeserializationContext
{
    LargePostingListReaderStreamGetter large_posting_getter;
    UInt64 row_start = 0;
    UInt64 row_end = 0;
};

struct LargePostingListWriterStream;
using LargePostingListWriterStreamGetter = std::function<LargePostingListWriterStream *(const ISerialization::SubstreamPath &)>;
struct ProjectionIndexSerializationContext
{
    LargePostingListWriterStreamGetter large_posting_getter;
};

}
