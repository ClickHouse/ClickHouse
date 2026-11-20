#include <Processors/Formats/Impl/Puffin/PuffinReader.h>

#include <IO/CompressionMethod.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

PuffinReader::PuffinReader(ReadBuffer & in_)
    : in(in_)
    , footer(readPuffinFooter(in, /*seekable_read=*/ true))
{
}

std::optional<String> PuffinReader::readBlob(const PuffinBlob & blob) const
{
    if (static_cast<UInt64>(blob.length) > PUFFIN_BLOB_MAX_SIZE)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED, "Puffin blob of {} bytes is larger than {} bytes", blob.length, PUFFIN_BLOB_MAX_SIZE);
    String bytes = readPuffinBlobBytes(blob, in, footer.data, /*seekable_read=*/ true);
    if (blob.compression_codec.empty())
        return bytes;

    CompressionMethod method = CompressionMethod::None;
    if (blob.compression_codec == "zstd")
        method = CompressionMethod::Zstd;
    else if (blob.compression_codec == "lz4")
        method = CompressionMethod::Lz4;
    else
        return {};

    auto decompressed = wrapReadBufferWithCompressionMethod(
        std::make_unique<ReadBufferFromString>(bytes), method, PUFFIN_BLOB_MAX_ZSTD_WINDOW_LOG);
    LimitReadBuffer limited(
        *decompressed,
        {.read_no_more = PUFFIN_BLOB_MAX_SIZE,
         .expect_eof = true,
         .excetion_hint = fmt::format("Puffin blob decompresses to more than {} bytes", PUFFIN_BLOB_MAX_SIZE)});
    String result;
    readStringUntilEOF(result, limited);
    return result;
}

}
