#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>

#include <Common/Exception.h>
#include <Common/config_version.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <limits>
#include <sstream>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

PuffinWriter::PuffinWriter(WriteBuffer & out_)
    : out(out_)
{
}

void PuffinWriter::writeHeaderIfNeeded()
{
    if (written_bytes != 0)
        return;
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
    written_bytes = sizeof(PUFFIN_MAGIC);
}

PuffinBlob PuffinWriter::addBlob(PuffinBlob blob, std::string_view bytes)
{
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot add a blob to a finalized Puffin file");

    writeHeaderIfNeeded();
    blob.offset = static_cast<Int64>(written_bytes);
    blob.length = static_cast<Int64>(bytes.size());
    out.write(bytes.data(), bytes.size());
    written_bytes += bytes.size();
    blobs.push_back(blob);
    return blob;
}

size_t PuffinWriter::finalize()
{
    if (finalized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Puffin file is already finalized");
    finalized = true;

    writeHeaderIfNeeded();

    Poco::JSON::Array::Ptr blobs_json = new Poco::JSON::Array;
    for (const auto & blob : blobs)
    {
        Poco::JSON::Array::Ptr fields_json = new Poco::JSON::Array;
        for (Int32 field_id : blob.fields)
            fields_json->add(field_id);

        Poco::JSON::Object::Ptr blob_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
        blob_json->set("type", blob.type);
        blob_json->set("fields", fields_json);
        blob_json->set("snapshot-id", blob.snapshot_id);
        blob_json->set("sequence-number", blob.sequence_number);
        blob_json->set("offset", blob.offset);
        blob_json->set("length", blob.length);
        if (!blob.compression_codec.empty())
            blob_json->set("compression-codec", blob.compression_codec);
        if (!blob.properties.empty())
        {
            Poco::JSON::Object::Ptr properties_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
            for (const auto & [key, value] : blob.properties)
                properties_json->set(key, value);
            blob_json->set("properties", properties_json);
        }
        blobs_json->add(blob_json);
    }

    Poco::JSON::Object::Ptr file_properties_json = new Poco::JSON::Object(Poco::JSON_PRESERVE_KEY_ORDER);
    file_properties_json->set("created-by", fmt::format("ClickHouse {}", VERSION_STRING));

    Poco::JSON::Object footer_json(Poco::JSON_PRESERVE_KEY_ORDER);
    footer_json.set("blobs", blobs_json);
    footer_json.set("properties", file_properties_json);

    std::ostringstream payload_stream; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    footer_json.stringify(payload_stream);
    const String payload = payload_stream.str();
    if (payload.size() > static_cast<size_t>(std::numeric_limits<Int32>::max()))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin footer of {} bytes is too large", payload.size());

    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
    out.write(payload.data(), payload.size());
    writeBinaryLittleEndian(static_cast<Int32>(payload.size()), out);
    writeBinaryLittleEndian(UInt32{0}, out);
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));

    const size_t footer_size = sizeof(PUFFIN_MAGIC) + payload.size() + sizeof(Int32) + sizeof(UInt32) + sizeof(PUFFIN_MAGIC);
    written_bytes += footer_size;
    return footer_size;
}

}
