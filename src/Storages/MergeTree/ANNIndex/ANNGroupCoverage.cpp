#include <Storages/MergeTree/ANNIndex/ANNGroupCoverage.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <xxhash.h>

#include <cstring>
#include <iomanip>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int LOGICAL_ERROR;
}

template class IntervalSet<UInt64>;

namespace
{
    constexpr char COVERAGE_MAGIC[8]     = {'A', 'N', 'N', 'C', 'O', 'V', '_', '_'};
    constexpr char COVERAGE_MAGIC_END[8] = {'E', 'N', 'D', 'C', 'O', 'V', '_', '_'};
    constexpr UInt32 COVERAGE_VERSION = 1;
    constexpr std::string_view COVERAGE_FILE_NAME = "coverage.bin";
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = 64 * 1024;
}

void ANNGroupCoverage::addPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block)
{
    auto & set = by_partition_hash[partition_hash];
    set.addInterval(min_block, max_block);
}

bool ANNGroupCoverage::containsPart(UInt64 partition_hash, UInt64 min_block, UInt64 max_block) const
{
    auto it = by_partition_hash.find(partition_hash);
    if (it == by_partition_hash.end())
        return false;
    return it->second.containsRange(min_block, max_block);
}

void ANNGroupCoverage::writeTo(IANNGroupStorage & storage) const
{
    if (by_partition_hash.size() > std::numeric_limits<UInt32>::max())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Too many partitions in coverage ({}) to serialise",
            by_partition_hash.size());

    /// First build the body into a memory buffer so we can hash it cheaply before writing.
    String body;
    {
        WriteBufferFromString body_out(body);
        for (const auto & [partition_hash, interval_set] : by_partition_hash)
        {
            writePODBinary(partition_hash, body_out);
            interval_set.serialize(body_out);
        }
        body_out.finalize();
    }

    const UInt64 checksum = XXH64(body.data(), body.size(), /*seed*/ 0);

    auto out = storage.writeFile(
        std::string(COVERAGE_FILE_NAME), DEFAULT_WRITE_BUFFER_SIZE, WriteMode::Rewrite, WriteSettings{});

    out->write(COVERAGE_MAGIC, sizeof(COVERAGE_MAGIC));
    writePODBinary(COVERAGE_VERSION, *out);
    const UInt32 partition_count = static_cast<UInt32>(by_partition_hash.size());
    writePODBinary(partition_count, *out);

    out->write(body.data(), body.size());

    writePODBinary(checksum, *out);
    out->write(COVERAGE_MAGIC_END, sizeof(COVERAGE_MAGIC_END));

    out->finalize();
}

void ANNGroupCoverage::readFrom(IANNGroupStorage & storage)
{
    by_partition_hash.clear();

    const size_t file_size = storage.getFileSize(std::string(COVERAGE_FILE_NAME));
    constexpr size_t HEADER_SIZE = sizeof(COVERAGE_MAGIC) + sizeof(UInt32) + sizeof(UInt32);
    constexpr size_t FOOTER_SIZE = sizeof(UInt64) + sizeof(COVERAGE_MAGIC_END);
    if (file_size < HEADER_SIZE + FOOTER_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`coverage.bin`: file too small ({} bytes)", file_size);

    auto in = storage.readFile(
        std::string(COVERAGE_FILE_NAME), ReadSettings{}, file_size);

    /// Slurp the whole file into memory. coverage.bin is tiny (dozens of partitions × a few
    /// intervals), so this is cheap and simplifies checksum verification.
    String contents;
    contents.resize(file_size);
    in->readStrict(contents.data(), file_size);

    const char * data = contents.data();

    if (std::memcmp(data, COVERAGE_MAGIC, sizeof(COVERAGE_MAGIC)) != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`coverage.bin`: invalid magic (expected `ANNCOV__`)");

    UInt32 version = 0;
    UInt32 partition_count = 0;
    std::memcpy(&version, data + 8, sizeof(version));
    std::memcpy(&partition_count, data + 12, sizeof(partition_count));

    if (version != COVERAGE_VERSION)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`coverage.bin`: unsupported version {}", version);

    const size_t body_size = file_size - HEADER_SIZE - FOOTER_SIZE;
    const char * body_begin = data + HEADER_SIZE;

    /// Checksum first — cheaper to fail fast than to parse garbage.
    UInt64 stored_checksum = 0;
    std::memcpy(&stored_checksum, data + HEADER_SIZE + body_size, sizeof(stored_checksum));
    const UInt64 actual_checksum = XXH64(body_begin, body_size, /*seed*/ 0);
    if (stored_checksum != actual_checksum)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`coverage.bin`: checksum mismatch (stored 0x{:016x}, actual 0x{:016x})",
            stored_checksum, actual_checksum);

    if (std::memcmp(data + HEADER_SIZE + body_size + sizeof(UInt64), COVERAGE_MAGIC_END,
            sizeof(COVERAGE_MAGIC_END)) != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`coverage.bin`: invalid end marker (expected `ENDCOV__`)");

    /// Parse body from the in-memory blob.
    ReadBufferFromMemory body_in(body_begin, body_size);
    for (UInt32 i = 0; i < partition_count; ++i)
    {
        UInt64 partition_hash = 0;
        readPODBinary(partition_hash, body_in);

        IntervalSet<UInt64> interval_set;
        interval_set.deserialize(body_in);

        auto [iter, inserted] = by_partition_hash.emplace(partition_hash, std::move(interval_set));
        if (!inserted)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "`coverage.bin`: duplicate partition hash 0x{:016x}", partition_hash);
    }
    if (!body_in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`coverage.bin`: trailing bytes after partition body");
}

std::string ANNGroupCoverage::describe() const
{
    std::ostringstream oss;
    oss << "ANNGroupCoverage{partitions=" << by_partition_hash.size() << ": ";
    bool first_partition = true;
    for (const auto & [partition_hash, interval_set] : by_partition_hash)
    {
        if (!first_partition)
            oss << ", ";
        first_partition = false;
        oss << "0x" << std::hex << std::setw(16) << std::setfill('0') << partition_hash
            << std::dec << "=[";
        bool first_iv = true;
        for (const auto & iv : interval_set.intervals())
        {
            if (!first_iv)
                oss << ",";
            first_iv = false;
            oss << "[" << iv.lo << "," << iv.hi << "]";
        }
        oss << "]";
    }
    oss << "}";
    return oss.str();
}

}
