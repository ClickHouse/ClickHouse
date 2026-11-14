#include <Processors/Formats/Impl/PuffinFile.h>

#include <IO/CompressionMethod.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Common/Exception.h>
#include <Common/config_version.h>

#include <sstream>

namespace DB
{

PuffinReader::PuffinReader(ReadBuffer & in_)
    : in(in_)
    , footer(readPuffinFooter(in, /*seekable_read=*/ true))
{
}

std::optional<String> PuffinReader::readBlob(const PuffinBlob & blob) const
{
    String bytes = readPuffinBlobBytes(blob, in, footer.data, /*seekable_read=*/ true);
    if (blob.compression_codec.empty())
        return bytes;

    CompressionMethod method;
    if (blob.compression_codec == "zstd")
        method = CompressionMethod::Zstd;
    else if (blob.compression_codec == "lz4")
        method = CompressionMethod::Lz4;
    else
        return {};

    auto decompressed = wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromString>(bytes), method);
    LimitReadBuffer limited(
        *decompressed,
        {.read_no_more = PUFFIN_BLOB_MAX_DECOMPRESSED_SIZE,
         .expect_eof = true,
         .excetion_hint = fmt::format("Puffin blob decompresses to more than {} bytes", PUFFIN_BLOB_MAX_DECOMPRESSED_SIZE)});
    String result;
    readStringUntilEOF(result, limited);
    return result;
}

PuffinWriter::PuffinWriter(WriteBuffer & out_)
    : out(out_)
{
    chassert(out.count() == 0, fmt::format("A Puffin file starts at offset 0 of its buffer, not {}", out.count()));
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
}

void PuffinWriter::addBlob(PuffinBlob blob, std::string_view bytes)
{
    blob.offset = static_cast<Int64>(out.count());
    blob.length = static_cast<Int64>(bytes.size());
    out.write(bytes.data(), bytes.size());
    blobs.push_back(std::move(blob));
}

size_t PuffinWriter::finalize()
{
    Poco::JSON::Array::Ptr blobs_json = new Poco::JSON::Array;
    for (const auto & blob : blobs)
        blobs_json->add(blob.toJSON(/*in_footer=*/ true));
    Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
    properties->set("created-by", fmt::format("ClickHouse {}", VERSION_STRING));
    Poco::JSON::Object footer;
    footer.set("blobs", blobs_json);
    footer.set("properties", properties);
    std::ostringstream payload; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    payload.exceptions(std::ios::failbit);
    footer.stringify(payload);
    const String payload_json = payload.str();

    const size_t footer_start = out.count();
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
    writeString(payload_json, out);
    writeBinaryLittleEndian(static_cast<Int32>(payload_json.size()), out);
    const UInt8 flags[4] = {};
    out.write(reinterpret_cast<const char *>(flags), sizeof(flags));
    out.write(reinterpret_cast<const char *>(PUFFIN_MAGIC), sizeof(PUFFIN_MAGIC));
    return out.count() - footer_start;
}

}
