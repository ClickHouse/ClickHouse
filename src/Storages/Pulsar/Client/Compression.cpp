#include <Storages/Pulsar/Client/Compression.h>

#include "PulsarApi.pb.h"

#include <lz4.h>
#include <zstd.h>
#include <snappy.h>
#include <zlib.h>

namespace DB::Pulsar
{

bool decompress(int compression_type, const char * data, size_t size, size_t uncompressed_size, std::string & out)
{
    /// NONE carries the payload verbatim; the declared uncompressed size is not meaningful.
    if (compression_type == pulsar::proto::NONE)
    {
        out.assign(data, size);
        return true;
    }

    if (uncompressed_size > MAX_UNCOMPRESSED_MESSAGE_SIZE)
        return false;

    out.resize(uncompressed_size);

    switch (compression_type)
    {
        case pulsar::proto::LZ4:
        {
            /// `LZ4_decompress_safe` is bounded by both the input and the output size, so a malformed
            /// frame cannot read or write out of bounds.
            const int decoded = LZ4_decompress_safe(
                data, out.data(), static_cast<int>(size), static_cast<int>(uncompressed_size));
            return decoded >= 0 && static_cast<size_t>(decoded) == uncompressed_size;
        }
        case pulsar::proto::ZLIB:
        {
            uLongf destination_size = static_cast<uLongf>(uncompressed_size);
            const int status = ::uncompress(
                reinterpret_cast<Bytef *>(out.data()), &destination_size,
                reinterpret_cast<const Bytef *>(data), static_cast<uLong>(size));
            return status == Z_OK && destination_size == uncompressed_size;
        }
        case pulsar::proto::ZSTD:
        {
            const size_t decoded = ZSTD_decompress(out.data(), uncompressed_size, data, size);
            return !ZSTD_isError(decoded) && decoded == uncompressed_size;
        }
        case pulsar::proto::SNAPPY:
        {
            size_t decoded_size = 0;
            if (!snappy::GetUncompressedLength(data, size, &decoded_size) || decoded_size != uncompressed_size)
                return false;
            return snappy::RawUncompress(data, size, out.data());
        }
        default:
            return false;
    }
}

}
