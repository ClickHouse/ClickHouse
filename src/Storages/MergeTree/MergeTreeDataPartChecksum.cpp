#include "MergeTreeDataPartChecksum.h"
#include <Common/SipHash.h>
#include <base/hex.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Storages/MergeTree/IDataPartStorage.h>


namespace DB
{


namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int BAD_SIZE_OF_FILE_IN_DATA_PART;
    extern const int FORMAT_VERSION_TOO_OLD;
    extern const int FILE_DOESNT_EXIST;
    extern const int UNEXPECTED_FILE_IN_DATA_PART;
    extern const int UNKNOWN_FORMAT;
    extern const int NO_FILE_IN_DATA_PART;
}


void MergeTreeDataPartChecksum::checkEqual(const MergeTreeDataPartChecksum & rhs, bool have_uncompressed, const String & name) const
{
    if (is_compressed && have_uncompressed)
    {
        if (!rhs.is_compressed)
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "No uncompressed checksum for file {}", name);
        if (rhs.uncompressed_size != uncompressed_size)
            throw Exception(ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART, "Unexpected uncompressed size of file {} in data part", name);
        if (rhs.uncompressed_hash != uncompressed_hash)
            throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "Checksum mismatch for uncompressed file {} in data part", name);
        return;
    }
    if (rhs.file_size != file_size)
        throw Exception(ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART, "Unexpected size of file {} in data part", name);
    if (rhs.file_hash != file_hash)
        throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "Checksum mismatch for file {} in data part", name);
}

void MergeTreeDataPartChecksum::checkSize(const IDataPartStorage & storage, const String & name) const
{
    /// Skip inverted index files, these have a default MergeTreeDataPartChecksum with file_size == 0
    if (name.ends_with(".gin_dict") || name.ends_with(".gin_post") || name.ends_with(".gin_seg") || name.ends_with(".gin_sid"))
        return;

    if (!storage.exists(name))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "{} doesn't exist", fs::path(storage.getRelativePath()) / name);

    // This is a projection, no need to check its size.
    if (storage.isDirectory(name))
        return;

    UInt64 size = storage.getFileSize(name);
    if (size != file_size)
        throw Exception(ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART,
            "{} has unexpected size: {} instead of {}",
            fs::path(storage.getRelativePath()) / name, size, file_size);
}


void MergeTreeDataPartChecksums::checkEqual(const MergeTreeDataPartChecksums & rhs, bool have_uncompressed) const
{
    for (const auto & it : rhs.files)
    {
        const String & name = it.first;

        if (!files.contains(name))
            throw Exception(ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART, "Unexpected file {} in data part", name);
    }

    for (const auto & it : files)
    {
        const String & name = it.first;

        /// Exclude files written by inverted index from check. No correct checksums are available for them currently.
        if (name.ends_with(".gin_dict") || name.ends_with(".gin_post") || name.ends_with(".gin_seg") || name.ends_with(".gin_sid"))
            continue;

        auto jt = rhs.files.find(name);
        if (jt == rhs.files.end())
            throw Exception(ErrorCodes::NO_FILE_IN_DATA_PART, "No file {} in data part", name);

        it.second.checkEqual(jt->second, have_uncompressed, name);
    }
}

void MergeTreeDataPartChecksums::checkSizes(const IDataPartStorage & storage) const
{
    for (const auto & it : files)
    {
        const String & name = it.first;
        it.second.checkSize(storage, name);
    }
}

UInt64 MergeTreeDataPartChecksums::getTotalSizeOnDisk() const
{
    UInt64 res = 0;
    for (const auto & it : files)
        res += it.second.file_size;
    return res;
}

bool MergeTreeDataPartChecksums::read(ReadBuffer & in, size_t format_version)
{
    switch (format_version)
    {
        case 1:
            return false;
        case 2:
            return readV2(in);
        case 3:
            return readV3(in);
        case 4:
            return readV4(in);
        default:
            throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Bad checksums format version: {}", DB::toString(format_version));
    }
}

bool MergeTreeDataPartChecksums::read(ReadBuffer & in)
{
    files.clear();

    assertString("checksums format version: ", in);
    size_t format_version;
    readText(format_version, in);
    assertChar('\n', in);

    read(in, format_version);
    return true;
}

bool MergeTreeDataPartChecksums::readV2(ReadBuffer & in)
{
    size_t count;

    readText(count, in);
    assertString(" files:\n", in);

    for (size_t i = 0; i < count; ++i)
    {
        String name;
        Checksum sum;

        readString(name, in);
        assertString("\n\tsize: ", in);
        readText(sum.file_size, in);
        assertString("\n\thash: ", in);
        readText(sum.file_hash.low64, in);
        assertString(" ", in);
        readText(sum.file_hash.high64, in);
        assertString("\n\tcompressed: ", in);
        readText(sum.is_compressed, in);
        if (sum.is_compressed)
        {
            assertString("\n\tuncompressed size: ", in);
            readText(sum.uncompressed_size, in);
            assertString("\n\tuncompressed hash: ", in);
            readText(sum.uncompressed_hash.low64, in);
            assertString(" ", in);
            readText(sum.uncompressed_hash.high64, in);
        }
        assertChar('\n', in);

        files.insert(std::make_pair(name, sum));
    }

    return true;
}

bool MergeTreeDataPartChecksums::readV3(ReadBuffer & in)
{
    size_t count;

    readVarUInt(count, in);

    for (size_t i = 0; i < count; ++i)
    {
        String name;
        Checksum sum;

        readBinary(name, in);
        readVarUInt(sum.file_size, in);
        readPODBinary(sum.file_hash, in);
        readBinary(sum.is_compressed, in);

        if (sum.is_compressed)
        {
            readVarUInt(sum.uncompressed_size, in);
            readPODBinary(sum.uncompressed_hash, in);
        }

        files.emplace(std::move(name), sum);
    }

    return true;
}

bool MergeTreeDataPartChecksums::readV4(ReadBuffer & from)
{
    CompressedReadBuffer in{from};
    return readV3(in);
}

void MergeTreeDataPartChecksums::write(WriteBuffer & to) const
{
    writeString("checksums format version: 4\n", to);

    CompressedWriteBuffer out{to, CompressionCodecFactory::instance().getDefaultCodec(), 1 << 16};

    writeVarUInt(files.size(), out);

    for (const auto & it : files)
    {
        const String & name = it.first;
        const Checksum & sum = it.second;

        writeBinary(name, out);
        writeVarUInt(sum.file_size, out);
        writePODBinary(sum.file_hash, out);
        writeBinary(sum.is_compressed, out);

        if (sum.is_compressed)
        {
            writeVarUInt(sum.uncompressed_size, out);
            writePODBinary(sum.uncompressed_hash, out);
        }
    }
}

void MergeTreeDataPartChecksums::addFile(const String & file_name, UInt64 file_size, MergeTreeDataPartChecksum::uint128 file_hash)
{
    files[file_name] = Checksum(file_size, file_hash);
}

void MergeTreeDataPartChecksums::add(MergeTreeDataPartChecksums && rhs_checksums)
{
    for (auto && checksum : rhs_checksums.files)
    {
        files[checksum.first] = std::move(checksum.second);
    }

    rhs_checksums.files.clear();
}

/// Checksum computed from the set of control sums of .bin files.
void MergeTreeDataPartChecksums::computeTotalChecksumDataOnly(SipHash & hash) const
{
    /// We use fact that iteration is in deterministic (lexicographical) order.
    for (const auto & it : files)
    {
        const String & name = it.first;
        const Checksum & sum = it.second;

        if (!endsWith(name, ".bin"))
            continue;

        UInt64 len = name.size();
        hash.update(len);
        hash.update(name.data(), len);
        hash.update(sum.uncompressed_size);
        hash.update(sum.uncompressed_hash);
    }
}

String MergeTreeDataPartChecksums::getSerializedString() const
{
    WriteBufferFromOwnString out;
    write(out);
    return out.str();
}

MergeTreeDataPartChecksums MergeTreeDataPartChecksums::deserializeFrom(const String & s)
{
    ReadBufferFromString in(s);
    MergeTreeDataPartChecksums res;
    if (!res.read(in))
        throw Exception(ErrorCodes::FORMAT_VERSION_TOO_OLD, "Checksums format is too old");
    assertEOF(in);
    return res;
}

bool MergeTreeDataPartChecksums::isBadChecksumsErrorCode(int code)
{
    return code == ErrorCodes::CHECKSUM_DOESNT_MATCH
           || code == ErrorCodes::BAD_SIZE_OF_FILE_IN_DATA_PART
           || code == ErrorCodes::NO_FILE_IN_DATA_PART
           || code == ErrorCodes::UNEXPECTED_FILE_IN_DATA_PART;
}

/// Puts into hash "stream" length of the string and its bytes
static void updateHash(SipHash & hash, const std::string & data)
{
    UInt64 len = data.size();
    hash.update(len);
    hash.update(data.data(), len);
}

/// Hash is the same as MinimalisticDataPartChecksums::hash_of_all_files
String MergeTreeDataPartChecksums::getTotalChecksumHex() const
{
    return getHexUIntUppercase(getTotalChecksumUInt128());
}

MergeTreeDataPartChecksums::Checksum::uint128 MergeTreeDataPartChecksums::getTotalChecksumUInt128() const
{
    SipHash hash_of_all_files;

    for (const auto & elem : files)
    {
        const String & name = elem.first;
        const auto & checksum = elem.second;

        updateHash(hash_of_all_files, name);
        hash_of_all_files.update(checksum.file_hash);
    }

    MergeTreeDataPartChecksums::Checksum::uint128 ret;
    hash_of_all_files.get128(reinterpret_cast<char *>(&ret));
    return ret;
}

void MinimalisticDataPartChecksums::serialize(WriteBuffer & to) const
{
    writeString("checksums format version: 5\n", to);
    serializeWithoutHeader(to);
}

void MinimalisticDataPartChecksums::serializeWithoutHeader(WriteBuffer & to) const
{
    writeVarUInt(num_compressed_files, to);
    writeVarUInt(num_uncompressed_files, to);

    writePODBinary(hash_of_all_files, to);
    writePODBinary(hash_of_uncompressed_files, to);
    writePODBinary(uncompressed_hash_of_compressed_files, to);
}

String MinimalisticDataPartChecksums::getSerializedString() const
{
    WriteBufferFromOwnString wb;
    serialize(wb);
    return wb.str();
}

bool MinimalisticDataPartChecksums::deserialize(ReadBuffer & in)
{
    assertString("checksums format version: ", in);
    size_t format_version;
    readText(format_version, in);
    assertChar('\n', in);

    if (format_version < MINIMAL_VERSION_WITH_MINIMALISTIC_CHECKSUMS)
    {
        MergeTreeDataPartChecksums new_full_checksums;
        if (!new_full_checksums.read(in, format_version))
            return false;

        computeTotalChecksums(new_full_checksums);
        full_checksums = std::move(new_full_checksums);
        return true;
    }

    if (format_version > MINIMAL_VERSION_WITH_MINIMALISTIC_CHECKSUMS)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT, "Unknown checksums format version: {}", DB::toString(format_version));

    deserializeWithoutHeader(in);

    return true;
}

void MinimalisticDataPartChecksums::deserializeWithoutHeader(ReadBuffer & in)
{
    readVarUInt(num_compressed_files, in);
    readVarUInt(num_uncompressed_files, in);

    readPODBinary(hash_of_all_files, in);
    readPODBinary(hash_of_uncompressed_files, in);
    readPODBinary(uncompressed_hash_of_compressed_files, in);
}

void MinimalisticDataPartChecksums::computeTotalChecksums(const MergeTreeDataPartChecksums & full_checksums_)
{
    num_compressed_files = 0;
    num_uncompressed_files = 0;

    SipHash hash_of_all_files_state;
    SipHash hash_of_uncompressed_files_state;
    SipHash uncompressed_hash_of_compressed_files_state;

    for (const auto & [name, checksum] : full_checksums_.files)
    {
        updateHash(hash_of_all_files_state, name);
        hash_of_all_files_state.update(checksum.file_hash);

        if (!checksum.is_compressed)
        {
            ++num_uncompressed_files;
            updateHash(hash_of_uncompressed_files_state, name);
            hash_of_uncompressed_files_state.update(checksum.file_hash);
        }
        else
        {
            ++num_compressed_files;
            updateHash(uncompressed_hash_of_compressed_files_state, name);
            uncompressed_hash_of_compressed_files_state.update(checksum.uncompressed_hash);
        }
    }

    auto get_hash = [] (SipHash & hash, uint128 & data)
    {
        hash.get128(data);
    };

    get_hash(hash_of_all_files_state, hash_of_all_files);
    get_hash(hash_of_uncompressed_files_state, hash_of_uncompressed_files);
    get_hash(uncompressed_hash_of_compressed_files_state, uncompressed_hash_of_compressed_files);
}

String MinimalisticDataPartChecksums::getSerializedString(const MergeTreeDataPartChecksums & full_checksums, bool minimalistic)
{
    if (!minimalistic)
        return full_checksums.getSerializedString();

    MinimalisticDataPartChecksums checksums;
    checksums.computeTotalChecksums(full_checksums);
    return checksums.getSerializedString();
}

void MinimalisticDataPartChecksums::checkEqual(const MinimalisticDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const
{
    if (full_checksums && rhs.full_checksums)
        full_checksums->checkEqual(*rhs.full_checksums, check_uncompressed_hash_in_compressed_files);

    // If full checksums were checked, check total checksums just in case
    checkEqualImpl(rhs, check_uncompressed_hash_in_compressed_files);
}

void MinimalisticDataPartChecksums::checkEqual(const MergeTreeDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const
{
    if (full_checksums)
        full_checksums->checkEqual(rhs, check_uncompressed_hash_in_compressed_files);

    // If full checksums were checked, check total checksums just in case
    MinimalisticDataPartChecksums rhs_minimalistic;
    rhs_minimalistic.computeTotalChecksums(rhs);
    checkEqualImpl(rhs_minimalistic, check_uncompressed_hash_in_compressed_files);
}

void MinimalisticDataPartChecksums::checkEqualImpl(const MinimalisticDataPartChecksums & rhs, bool check_uncompressed_hash_in_compressed_files) const
{
    if (num_compressed_files != rhs.num_compressed_files || num_uncompressed_files != rhs.num_uncompressed_files)
    {
        throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                        "Different number of files: {} compressed (expected {}) and {} uncompressed ones (expected {})",
                        rhs.num_compressed_files, num_compressed_files, rhs.num_uncompressed_files, num_uncompressed_files);
    }

    Strings errors;

    if (hash_of_uncompressed_files != rhs.hash_of_uncompressed_files)
        errors.emplace_back("hash of uncompressed files doesn't match");

    if (check_uncompressed_hash_in_compressed_files)
    {
        if (uncompressed_hash_of_compressed_files != rhs.uncompressed_hash_of_compressed_files)
            errors.emplace_back("uncompressed hash of compressed files doesn't match");
    }
    else
    {
        if (hash_of_all_files != rhs.hash_of_all_files)
            errors.emplace_back("total hash of all files doesn't match");
    }

    if (!errors.empty())
    {
        throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH, "Checksums of parts don't match: {}", fmt::join(errors, ", "));
    }
}

MinimalisticDataPartChecksums MinimalisticDataPartChecksums::deserializeFrom(const String & s)
{
    MinimalisticDataPartChecksums res;
    ReadBufferFromString rb(s);
    res.deserialize(rb);
    return res;
}

}
