#include <Storages/ObjectStorage/DataLakes/Iceberg/PuffinThetaSketches.h>

#if USE_AVRO

#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergWrites.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Common/Exception.h>
#include <base/unaligned.h>

#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace
{

constexpr std::string_view puffin_magic = "PFA1";

}

PuffinThetaSketchFile writePuffinThetaSketches(const std::vector<PuffinThetaSketchBlob> & blobs, Int64 snapshot_id, Int64 sequence_number)
{
    PuffinThetaSketchFile file{.bytes = String(puffin_magic), .footer_size = 0, .blob_metadata = new Poco::JSON::Array};
    Poco::JSON::Array::Ptr footer_blobs = new Poco::JSON::Array;
    for (const auto & theta_blob : blobs)
    {
        Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
        properties->set(Iceberg::f_ndv, std::to_string(theta_blob.ndv));
        Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
        fields->add(theta_blob.field_id);

        Poco::JSON::Object::Ptr blob = new Poco::JSON::Object;
        blob->set(Iceberg::f_type, Iceberg::theta_sketch_blob_type);
        blob->set(Iceberg::f_metadata_snapshot_id, snapshot_id);
        blob->set(Iceberg::f_metadata_sequence_number, sequence_number);
        blob->set(Iceberg::f_fields, fields);
        blob->set(Iceberg::f_properties, properties);
        file.blob_metadata->add(Iceberg::deepCopy(blob));

        blob->set(Iceberg::f_offset, file.bytes.size());
        blob->set(Iceberg::f_length, theta_blob.sketch_bytes.size());
        footer_blobs->add(blob);
        file.bytes.append(theta_blob.sketch_bytes);
    }

    Poco::JSON::Object::Ptr footer = new Poco::JSON::Object;
    footer->set(Iceberg::f_blobs, footer_blobs);
    Poco::JSON::Object::Ptr file_properties = new Poco::JSON::Object;
    file_properties->set(Iceberg::f_created_by, "ClickHouse");
    footer->set(Iceberg::f_properties, file_properties);
    const String payload = stringifyJSON(footer);

    const size_t footer_offset = file.bytes.size();
    const Int32 payload_size = static_cast<Int32>(payload.size());
    const Int32 flags = 0;
    file.bytes.append(puffin_magic);
    file.bytes.append(payload);
    char trailer[2 * sizeof(Int32)];
    unalignedStoreLittleEndian<Int32>(trailer, payload_size);
    unalignedStoreLittleEndian<Int32>(trailer + sizeof(Int32), flags);
    file.bytes.append(trailer, sizeof(trailer));
    file.bytes.append(puffin_magic);
    file.footer_size = file.bytes.size() - footer_offset;
    return file;
}

std::optional<std::map<Int32, String>> readPuffinThetaSketches(const String & puffin_bytes)
{
    constexpr size_t trailer_size = sizeof(Int32) + sizeof(Int32) + puffin_magic.size();
    if (puffin_bytes.size() < puffin_magic.size() * 2 + trailer_size || !puffin_bytes.starts_with(puffin_magic)
        || !puffin_bytes.ends_with(puffin_magic))
        return {};

    const char * trailer = puffin_bytes.data() + puffin_bytes.size() - trailer_size;
    const auto payload_size = unalignedLoadLittleEndian<Int32>(trailer);
    const auto flags = unalignedLoadLittleEndian<Int32>(trailer + sizeof(Int32));
    /// Flag bit 0 is an LZ4-compressed footer.
    if (flags & 1 || payload_size < 0 || static_cast<size_t>(payload_size) + puffin_magic.size() * 2 + trailer_size > puffin_bytes.size())
        return {};

    const size_t payload_offset = puffin_bytes.size() - trailer_size - payload_size;
    const size_t footer_offset = payload_offset - puffin_magic.size();
    if (puffin_bytes.compare(footer_offset, puffin_magic.size(), puffin_magic) != 0)
        return {};

    Poco::JSON::Parser parser;
    const auto footer = parser.parse(puffin_bytes.substr(payload_offset, payload_size)).extract<Poco::JSON::Object::Ptr>();
    const auto blobs = footer->getArray(Iceberg::f_blobs);
    std::map<Int32, String> result;
    for (unsigned i = 0; blobs && i < blobs->size(); ++i)
    {
        const auto blob = blobs->getObject(i);
        const auto fields = blob->getArray(Iceberg::f_fields);
        if (blob->getValue<String>(Iceberg::f_type) != Iceberg::theta_sketch_blob_type || !fields || fields->size() != 1)
            continue;
        const auto offset = blob->getValue<UInt64>(Iceberg::f_offset);
        const auto length = blob->getValue<UInt64>(Iceberg::f_length);
        if (offset < puffin_magic.size() || offset + length > footer_offset)
            throw Exception(
                ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                "Puffin blob [{}, {}) is outside the blob section [{}, {})",
                offset,
                offset + length,
                puffin_magic.size(),
                footer_offset);

        String bytes = puffin_bytes.substr(offset, length);
        /// The two codecs of the spec; Spark writes zstd.
        if (blob->has(Iceberg::f_compression_codec) && !blob->isNull(Iceberg::f_compression_codec))
        {
            const auto codec = blob->getValue<String>(Iceberg::f_compression_codec);
            if (codec != "zstd" && codec != "lz4")
                continue;
            auto decompressed = wrapReadBufferWithCompressionMethod(
                std::make_unique<ReadBufferFromString>(bytes), codec == "zstd" ? CompressionMethod::Zstd : CompressionMethod::Lz4);
            String plain;
            readStringUntilEOF(plain, *decompressed);
            bytes = std::move(plain);
        }
        result.emplace(fields->getElement<Int32>(0), std::move(bytes));
    }
    return result;
}

}

#endif
