#include <Storages/MergeTree/ANNIndex/PartRowIdMapReader.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMap.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <Common/Exception.h>
#include <IO/ReadHelpers.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace
{
    constexpr std::string_view ID_MAP_FILE_NAME = "id_map.bin";
}

void PartRowIdMapReader::loadFrom(IANNGroupStorage & storage, const ReadSettings & settings)
{
    using namespace PartRowIdMapFormat;

    records.clear();

    const size_t file_size = storage.getFileSize(std::string(ID_MAP_FILE_NAME));
    if (file_size < HEADER_SIZE + FOOTER_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: file too small ({} bytes)", file_size);

    auto in = storage.readFile(std::string(ID_MAP_FILE_NAME), settings, file_size);

    /// Header.
    char header[HEADER_SIZE];
    in->readStrict(header, HEADER_SIZE);

    UInt32 magic = 0;
    UInt32 version = 0;
    UInt32 record_size = 0;
    UInt32 hash_algo = 0;
    UInt64 record_count = 0;
    std::memcpy(&magic,        header + 0,  sizeof(magic));
    std::memcpy(&version,      header + 4,  sizeof(version));
    std::memcpy(&record_size,  header + 8,  sizeof(record_size));
    std::memcpy(&hash_algo,    header + 12, sizeof(hash_algo));
    std::memcpy(&record_count, header + 16, sizeof(record_count));

    if (magic != MAGIC)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: invalid magic (expected 0x{:08x}, got 0x{:08x})", MAGIC, magic);
    if (version != VERSION)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: unsupported version {}", version);
    if (record_size != RECORD_SIZE)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: unexpected record size {} (expected {})", record_size, RECORD_SIZE);
    if (hash_algo != PARTITION_HASH_ALGO_SIPHASH64)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: unsupported partition hash algorithm {}", hash_algo);

    const size_t expected_body = record_count * RECORD_SIZE;
    if (expected_body + HEADER_SIZE + FOOTER_SIZE != file_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: size mismatch (header says {} records = {} bytes, body slot = {} bytes)",
            record_count, expected_body, file_size - HEADER_SIZE - FOOTER_SIZE);

    /// Body.
    records.resize(record_count);
    if (expected_body > 0)
        in->readStrict(reinterpret_cast<char *>(records.data()), expected_body);

    /// Footer.
    char footer[FOOTER_SIZE];
    in->readStrict(footer, FOOTER_SIZE);

    UInt64 stored_checksum = 0;
    UInt64 stored_magic_end = 0;
    std::memcpy(&stored_checksum, footer + 0, sizeof(stored_checksum));
    std::memcpy(&stored_magic_end, footer + 8, sizeof(stored_magic_end));

    const UInt64 actual_checksum = expected_body
        ? PartRowIdMapFormat::hashBody(records.data(), expected_body)
        : PartRowIdMapFormat::hashBody(nullptr, 0);

    if (stored_checksum != actual_checksum)
    {
        records.clear();
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: checksum mismatch (stored 0x{:016x}, actual 0x{:016x})",
            stored_checksum, actual_checksum);
    }

    if (stored_magic_end != MAGIC_END)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "`id_map.bin`: invalid end marker");
}

}
