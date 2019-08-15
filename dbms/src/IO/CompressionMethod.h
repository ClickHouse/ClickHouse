#pragma once

namespace DB
{

enum class CompressionMethod
{
    /// DEFLATE compression with gzip header and CRC32 checksum.
    /// This option corresponds to files produced by gzip(1) or HTTP Content-Encoding: gzip.
    Gzip,
    /// DEFLATE compression with zlib header and Adler32 checksum.
    /// This option corresponds to HTTP Content-Encoding: deflate.
    Zlib,
    Brotli,
};

}
