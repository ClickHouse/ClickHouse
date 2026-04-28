#include <Storages/MergeTree/ANNIndex/PartRowIdMapWriter.h>
#include <Storages/MergeTree/ANNIndex/PartRowIdMap.h>
#include <Storages/MergeTree/ANNIndex/IANNGroupStorage.h>

#include <IO/WriteHelpers.h>

namespace DB
{

namespace
{
    constexpr std::string_view ID_MAP_FILE_NAME = "id_map.bin";
    constexpr size_t DEFAULT_WRITE_BUFFER_SIZE = 1024 * 1024;
}

void PartRowIdMapWriter::append(const PartRowId & row)
{
    records.push_back(row);
}

void PartRowIdMapWriter::writeTo(IANNGroupStorage & storage, const WriteSettings & settings) const
{
    using namespace PartRowIdMapFormat;

    auto out = storage.writeFile(
        std::string(ID_MAP_FILE_NAME), DEFAULT_WRITE_BUFFER_SIZE, WriteMode::Rewrite, settings);

    const UInt32 magic = MAGIC;
    const UInt32 version = VERSION;
    const UInt32 record_size = RECORD_SIZE;
    const UInt32 hash_algo = PARTITION_HASH_ALGO_SIPHASH64;
    const UInt64 record_count = records.size();
    const UInt64 reserved = 0;

    writePODBinary(magic, *out);
    writePODBinary(version, *out);
    writePODBinary(record_size, *out);
    writePODBinary(hash_algo, *out);
    writePODBinary(record_count, *out);
    writePODBinary(reserved, *out);

    const size_t body_bytes = records.size() * sizeof(PartRowId);
    if (body_bytes > 0)
        out->write(reinterpret_cast<const char *>(records.data()), body_bytes);

    /// Checksum covers the body; on an empty body this is XXH64 of zero bytes,
    /// which is a nonzero well-known constant — the reader expects the same.
    const UInt64 checksum = PartRowIdMapFormat::hashBody(
        body_bytes > 0 ? static_cast<const void *>(records.data()) : nullptr,
        body_bytes);

    writePODBinary(checksum, *out);
    const UInt64 magic_end = MAGIC_END;
    writePODBinary(magic_end, *out);

    out->finalize();
}

}
