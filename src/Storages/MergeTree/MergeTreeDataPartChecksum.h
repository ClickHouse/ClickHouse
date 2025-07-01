#pragma once

#include <base/types.h>
#include <Disks/IDisk.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Core/Types_fwd.h>

#include <map>
#include <optional>
#include <city.h>

class SipHash;

namespace DB
{

class IDataPartStorage;
class ReadBuffer;
class WriteBuffer;

/// Checksum of one file.
struct MergeTreeDataPartChecksum
{
    using uint128 = CityHash_v1_0_2::uint128;

    UInt64 file_size {};
    uint128 file_hash {};

    bool is_compressed = false;
    UInt64 uncompressed_size {};
    uint128 uncompressed_hash {};

    MergeTreeDataPartChecksum() = default;
    MergeTreeDataPartChecksum(UInt64 file_size_, uint128 file_hash_) : file_size(file_size_), file_hash(file_hash_) {}
    MergeTreeDataPartChecksum(UInt64 file_size_, uint128 file_hash_, UInt64 uncompressed_size_, uint128 uncompressed_hash_)
        : file_size(file_size_), file_hash(file_hash_), is_compressed(true),
        uncompressed_size(uncompressed_size_), uncompressed_hash(uncompressed_hash_) {}

    void checkEqual(const MergeTreeDataPartChecksum & rhs, bool have_uncompressed, const String & name, const String & part_name) const;
    void checkSize(const IDataPartStorage & storage, const String & name) const;
};


/** Checksums of all non-temporary files.
  * For compressed files, the check sum and the size of the decompressed data are stored to not depend on the compression method.
  */
struct MergeTreeDataPartChecksums
{
    using Checksum = MergeTreeDataPartChecksum;

    /// The order is important.
    using FileChecksums = std::map<String, Checksum>;
    FileChecksums files;

    void addFile(const String & file_name, UInt64 file_size, Checksum::uint128 file_hash);

    void add(MergeTreeDataPartChecksums && rhs_checksums);

    bool has(const String & file_name) const { return files.find(file_name) != files.end(); }

    bool remove(const String & file_name) { return files.erase(file_name); }

    bool empty() const { return files.empty(); }

    /// Checks that the set of columns and their checksums are the same. If not, throws an exception.
    /// If have_uncompressed, for compressed files it compares the checksums of the decompressed data.
    /// Otherwise, it compares only the checksums of the files.
    void checkEqual(const MergeTreeDataPartChecksums & rhs, bool have_uncompressed, const String & part_name) const;

    static bool isBadChecksumsErrorCode(int code);

    /// Returns false if the checksum is too old.
    bool read(ReadBuffer & in);
    /// Assume that header with version (the first line) is read
    bool read(ReadBuffer & in, size_t format_version);
    bool readV2(ReadBuffer & in);
    bool readV3(ReadBuffer & in);
    bool readV4(ReadBuffer & from);

    void write(WriteBuffer & to) const;

    /// Checksum from the set of checksums of .bin files (for deduplication).
    void computeTotalChecksumDataOnly(SipHash & hash) const;

    /// SipHash of all all files hashes represented as hex string
    String getTotalChecksumHex() const;

    Checksum::uint128 getTotalChecksumUInt128() const;

    String getSerializedString() const;
    static MergeTreeDataPartChecksums deserializeFrom(const String & s);

    UInt64 getTotalSizeOnDisk() const;
    UInt64 getTotalSizeUncompressedOnDisk() const;

    Strings getFileNames() const;
};

/// A kind of MergeTreeDataPartChecksums intended to be stored in ZooKeeper (to save its RAM)
/// MinimalisticDataPartChecksums and MergeTreeDataPartChecksums have the same serialization format
///  for versions less than MINIMAL_VERSION_WITH_MINIMALISTIC_CHECKSUMS.
struct MinimalisticDataPartChecksums
{
    UInt64 num_compressed_files = 0;
    UInt64 num_uncompressed_files = 0;

    using uint128 = MergeTreeDataPartChecksum::uint128;
    uint128 hash_of_all_files {};
    uint128 hash_of_uncompressed_files {};
    uint128 uncompressed_hash_of_compressed_files {};

    bool operator==(const MinimalisticDataPartChecksums & other) const
    {
        return num_compressed_files == other.num_compressed_files
            && num_uncompressed_files == other.num_uncompressed_files
            && hash_of_all_files == other.hash_of_all_files
            && hash_of_uncompressed_files == other.hash_of_uncompressed_files
            && uncompressed_hash_of_compressed_files == other.uncompressed_hash_of_compressed_files;
    }

    /// Is set only for old formats
    std::optional<MergeTreeDataPartChecksums> full_checksums;

    static constexpr size_t MINIMAL_VERSION_WITH_MINIMALISTIC_CHECKSUMS = 5;

    MinimalisticDataPartChecksums() = default;
    void computeTotalChecksums(const MergeTreeDataPartChecksums & full_checksums);

    bool deserialize(ReadBuffer & in);
    void deserializeWithoutHeader(ReadBuffer & in);
    static MinimalisticDataPartChecksums deserializeFrom(const String & s);

    void serialize(WriteBuffer & to) const;
    void serializeWithoutHeader(WriteBuffer & to) const;
    String getSerializedString() const;
    static String getSerializedString(const MergeTreeDataPartChecksums & full_checksums, bool minimalistic);

    void checkEqual(const MinimalisticDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files, const String & part_name) const;
    void checkEqual(const MergeTreeDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files, const String & part_name) const;
    void checkEqualImpl(const MinimalisticDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const;
};


}
