#pragma once

#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Formats/MarkInCompressedFile.h>

namespace DB
{

/// Prefix used by every per-substream skip-index file name on disk: "skp_idx_<name>...".
inline constexpr std::string_view SKIP_INDEX_FILE_PREFIX = "skp_idx_";

/// Reserved name for the per-part archive that aggregates packed skip-index substreams.
/// Cannot collide with a per-file skip-index entry: those start with SKIP_INDEX_FILE_PREFIX
/// (underscore-suffixed), while the archive uses "skp_idx." (dot-suffixed).
inline constexpr std::string_view SKIP_INDICES_PACKED_FILENAME = "skp_idx.packed";

class IMergeTreeIndexCondition;
class IMergeTreeDataPart;
struct IMergeTreeIndex;

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

    static bool isCompressed(Type type)
    {
        /// Text index postings are not compressed by write buffer,
        /// because the compression is implicitly applied during building them.
        return type != Type::TextIndexPostings;
    }
};

using MergeTreeIndexSubstreams = std::vector<MergeTreeIndexSubstream>;
using MergeTreeIndexVersion = uint8_t;

struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version;
    MergeTreeIndexSubstreams substreams;

    explicit operator bool() const { return version != 0; }
};

using MergeTreeIndexWriterStream = MergeTreeWriterStream;
using MergeTreeIndexOutputStreams = std::map<MergeTreeIndexSubstream::Type, MergeTreeIndexWriterStream *>;

using MergeTreeIndexReaderStream = MergeTreeReaderStream;
using MergeTreeIndexInputStreams = std::map<MergeTreeIndexSubstream::Type, MergeTreeIndexReaderStream *>;

struct MergeTreeIndexDeserializationState
{
    MergeTreeIndexVersion version;
    const IMergeTreeIndexCondition * condition;
    const IMergeTreeDataPart & part;
    const IMergeTreeIndex & index;
};

}
