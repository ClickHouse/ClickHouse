#pragma once

#include <Storages/MergeTree/MergeTreeWriterStream.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Formats/MarkInCompressedFile.h>

#include <algorithm>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Prefix used by every per-substream skip-index file name on disk: "skp_idx_<name>...".
inline constexpr std::string_view SKIP_INDEX_FILE_PREFIX = "skp_idx_";

/// Reserved name for the per-part archive that aggregates packed skip-index substreams.
/// Cannot collide with a per-file skip-index entry: those start with SKIP_INDEX_FILE_PREFIX
/// (underscore-suffixed), while the archive uses "skp_idx." (dot-suffixed).
inline constexpr std::string_view SKIP_INDICES_PACKED_FILENAME = "skp_idx.packed";

/// True iff @name looks like a per-substream skip-index file name (and so could be a virtual
/// file inside skp_idx.packed). The archive itself uses SKIP_INDICES_PACKED_FILENAME
/// ("skp_idx.packed", dot-suffixed) so this check never matches the archive — callers can use
/// it as a cheap pre-filter before consulting the archive overlay without risking recursion.
inline bool looksLikePackedSkipIndexFile(std::string_view name)
{
    return name.starts_with(SKIP_INDEX_FILE_PREFIX);
}

class IMergeTreeIndexCondition;
class IMergeTreeDataPartInfoForReader;
struct IMergeTreeIndex;
struct MarkRanges;

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
        TextIndexPositions,
        /// Identity dictionary is compressed like the token dictionary. Identity postings are plain, like token postings.
        TextIndexIdentityDictionary,
        TextIndexIdentityPostings,
    };

    Type type;
    /// Suffix that is added to the end of index substream's filename.
    String suffix;
    /// Extension of the index substream's file with data. Encodes the serialization version (".idx", "idx2", etc.)
    String extension;

    static bool isCompressed(Type type)
    {
        /// Text index postings and positions are not compressed by write buffer,
        /// because the compression is implicitly applied during building them.
        /// Identity postings share that layout. Identity dictionary stays compressed.
        return type != Type::TextIndexPostings && type != Type::TextIndexPositions && type != Type::TextIndexIdentityPostings;
    }
};

using MergeTreeIndexSubstreams = std::vector<MergeTreeIndexSubstream>;
using MergeTreeIndexVersion = uint8_t;

/// Finds a substream of the given type, or returns nullptr if absent.
inline const MergeTreeIndexSubstream * findSubstream(const MergeTreeIndexSubstreams & substreams, MergeTreeIndexSubstream::Type type)
{
    const auto it = std::ranges::find_if(substreams, [type](const auto & substream) { return substream.type == type; });
    return it == substreams.end() ? nullptr : &*it;
}

/// Finds a substream of the given type, or throws if absent.
inline const MergeTreeIndexSubstream & requireSubstream(const MergeTreeIndexSubstreams & substreams, MergeTreeIndexSubstream::Type type)
{
    const auto * substream = findSubstream(substreams, type);
    if (!substream)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Text index is missing a substream of type {}", static_cast<int>(type));
    return *substream;
}

struct MergeTreeIndexFormat
{
    MergeTreeIndexVersion version{};
    MergeTreeIndexSubstreams substreams;

    explicit operator bool() const { return version != 0; }
};

using MergeTreeIndexWriterStream = MergeTreeWriterStream;
using MergeTreeIndexOutputStreams = std::map<MergeTreeIndexSubstream::Type, MergeTreeIndexWriterStream *>;

using MergeTreeIndexReaderStream = MergeTreeReaderStream;
using MergeTreeIndexInputStreams = std::map<MergeTreeIndexSubstream::Type, MergeTreeIndexReaderStream *>;

struct MergeTreeIndexDeserializationState
{
    MergeTreeIndexVersion version = 0;
    const IMergeTreeIndexCondition * condition = nullptr;
    const IMergeTreeDataPartInfoForReader & part_info;
    const IMergeTreeIndex & index;
    const MarkRanges * readable_ranges = nullptr;
    bool text_index_read_postings = true;
    const MergeTreeReaderSettings & reader_settings;
    /// When false, a V3 part does not open `.idt` streams. Positive `hasToken` skip and
    /// filter-only Exact reads do not need identity postings.
    bool load_identity_streams = true;
};

}
