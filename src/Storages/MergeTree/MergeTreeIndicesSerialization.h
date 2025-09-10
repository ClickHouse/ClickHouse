#pragma once
#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Formats/MarkInCompressedFile.h>

namespace DB
{

class IMergeTreeIndexCondition;

struct IndexSubstream
{
    enum class Type
    {
        Regular,
        TextIndexDictionary,
        TextIndexPostings,
    };

    Type type;
    String suffix;
    String extension;
};

using IndexSubstreams = std::vector<IndexSubstream>;
using MergeTreeIndexVersion = uint8_t;

struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    IndexSubstreams substreams;

    explicit operator bool() const { return version != 0; }
};

using IndexWriterStream = MergeTreeWriterStream<false>;
using IndexOutputStreams = std::map<IndexSubstream::Type, IndexWriterStream *>;

using IndexReaderStream = MergeTreeReaderStream;
using IndexInputStreams = std::map<IndexSubstream::Type, IndexReaderStream *>;

struct IndexDeserializationState
{
    MergeTreeIndexVersion version;
    const IMergeTreeIndexCondition * condition;
};

}
