#pragma once

#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Formats/MarkInCompressedFile.h>

namespace DB
{

class IMergeTreeIndexCondition;

/// Represents a substream of a merge tree index.
/// By default skip indexes have one substream (skp_idx_<name>.idx),
/// but some indexes (e.g. text index) may have multiple substreams.
/// Each substream has file with data and file with marks.
struct MergeTreeIndexSubstream
{
    enum class Type
    {
        Regular,
        TextIndexDictionary,
        TextIndexPostings,
    };

    Type type;
    /// Suffix that is added to the end of index substream's filename.
    String suffix;
    /// Extension of the index substream's file with data. Encodes the serialization version (".idx", "idx2", etc.)
    String extension;
};

using MergeTreeIndexSubstreams = std::vector<MergeTreeIndexSubstream>;
using MergeTreeIndexVersion = uint8_t;

struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    MergeTreeIndexSubstreams substreams;

    explicit operator bool() const { return version != 0; }
};

using MergeTreeIndexWriterStream = MergeTreeWriterStream<false>;
using MergeTreeIndexOutputStreams = std::map<MergeTreeIndexSubstream::Type, MergeTreeIndexWriterStream *>;

using MergeTreeIndexReaderStream = MergeTreeReaderStream;
using MergeTreeIndexInputStreams = std::map<MergeTreeIndexSubstream::Type, MergeTreeIndexReaderStream *>;

struct MergeTreeIndexDeserializationState
{
    MergeTreeIndexVersion version;
    const IMergeTreeIndexCondition * condition;
    String path_to_data_part;
    String index_name;
    size_t index_mark;
};

}
