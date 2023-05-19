#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MinMaxIndex.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeDataPartTTLInfo.h>
#include <Interpreters/TransactionVersionMetadata.h>
#include <DataTypes/Serializations/SerializationInfo.h>
#include <Storages/MergeTree/MergeTreePartition.h>

namespace DB
{

class IPartMetadataManager;
class IDataPartStorage;

/// Possible versions of the metadata.txt file format.
/// Not to be confused with MergeTreeData::format_version, or with metadata_version, or with VersionMetadata.
enum DataPartMetadataFormatVersions : UInt16
{
    /// (Here we will have version numbers when various fields were added/removed, for use by
    /// deserialization code.)

    /// Separate files: columns.txt, checksums.txt, etc.
    OLDEST = 0,

    /// First version with metadata.txt
    ONE_FILE = 1,

    DEFAULT_FOR_WRITE = 0,
    LATEST = 1,
};

/// Various information about a merge tree data part.
/// Stored in file metadata.txt inside the part's directory.
///
/// This struct is intended for things that are serialized/deserialized in metadata.txt.
/// Other (memory-only) information about the data part lives in IMergeTreeDataPart.
///
/// Historically, some of this information used to be scattered in multiple small text files
/// (columns.txt, checksums.txt, etc), and some of it was inferred from names of files inside the
/// part's directory (e.g. .mrk vs .mrk2).
/// We're currently in the middle of a migration to consolidate it in this struct instead.
///
/// TODO: Actually implement this. Currently metadata.txt is not a thing, and this struct is always
///       populated from old-style files.
struct DataPartMetadata
{
    static inline constexpr auto FILE_NAME = "metadata.txt";

    /// Part unique identifier.
    /// The intention is to use it for identifying cases where the same part is
    /// processed by multiple shards.
    /// (Formerly uuid.txt)
    UUID uuid = UUIDHelpers::Nil;

    /// (Formerly count.txt)
    size_t rows_count = 0;

    /// (Formerly ttl.txt)
    //asdqwe find why this is mutable (make immutable and see build errors)
    mutable MergeTreeDataPartTTLInfos ttl_infos;

    /// (Formerly partition.dat)
    MergeTreePartition partition;

    /// (Formerly minmax_<column_name>.idx)
    MinMaxIndexPtr minmax_idx = std::make_shared<MinMaxIndex>();

    /// Checksums of each file in the part. Often used just to see which files exist.
    /// (Formerly checksums.txt)
    MergeTreeDataPartChecksums checksums;

    /// Compression codec name which was used to compress part columns by default. Some columns may
    /// have their own compression codecs, but default will be stored here.
    /// (Formerly default_compression_codec.txt)
    CompressionCodecPtr default_codec;

    /// Version of part metadata (columns, pk and so on - roughly the things altered by ALTER queries).
    /// (Formerly metadata_version.txt)
    int32_t metadata_version;

    /// Columns description.
    /// (Formerly columns.txt)
    NamesAndTypesList columns;

    /// Information about kinds of serialization of columns and information that helps to choose
    /// kind of serialization later during merging (number of rows, number of rows with default
    /// values, etc).
    /// (Formerly serialization.json)
    SerializationInfoByName serialization_infos;

    /// TODO: Checksum metadata.txt

    /// Serialized in a concise mostly-text format.
    /// (Currently it's all text, but in future we may want to inline primary.idx and marks
    /// granularities into this file, for small parts. The binary data can be placed after the text,
    /// to keep metadata.txt mostly human-readable.)
    void serialize(WriteBuffer & out, UInt16 metadata_format_version = DataPartMetadataFormatVersions::DEFAULT_FOR_WRITE) const;
    void deserialize(ReadBuffer & in);

    /// Working with files in the old format: columns.txt, count.txt, etc.
    void loadFromOldStyleFiles(const IPartMetadataManager & manager, bool is_projection);
    void storeToOldStyleFiles(IDataPartStorage & data_part_storage);
    /// Produces a set of file names that includes all metadata files that may exist in this part
    /// (and possibly some files that don't exist). Appends to `out`.
    /// This doesn't include index file and marks files - those weren't moved to metadata.txt
    /// (because they can be big).
    void listPossibleOldStyleFiles(Strings & out) const;

    /// All of the old-style file names that metadata.txt replaces.
    /// TODO: Not actually all, search IMergeTreeDataPart for string literals.
    /// TODO: Search for the literals and replace with these constants.
    static inline constexpr auto DEFAULT_COMPRESSION_CODEC_FILE_NAME = "default_compression_codec.txt";
    static inline constexpr auto UUID_FILE_NAME = "uuid.txt";
    static inline constexpr auto COUNT_FILE_NAME = "count.txt";
    static inline constexpr auto TTL_FILE_NAME = "ttl.txt";
    static inline constexpr auto CHECKSUMS_FILE_NAME = "checksums.txt";
    static inline constexpr auto SERIALIZATION_FILE_NAME = "serialization.json";
    static inline constexpr auto METADATA_VERSION_FILE_NAME = "metadata_version.txt";
};

}
