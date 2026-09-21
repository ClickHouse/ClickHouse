#pragma once

#include <memory>
#include <string>

#include <Core/Defines.h>
#include <IO/SnappyMode.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;

/** These are "generally recognizable" compression methods for data import/export.
  * Do not mess with more efficient compression methods used by ClickHouse internally
  *  (they use non-standard framing, indexes, checksums...)
  */

enum class CompressionMethod : uint8_t
{
    None,
    /// DEFLATE compression with gzip header and CRC32 checksum.
    /// This option corresponds to files produced by gzip(1) or HTTP Content-Encoding: gzip.
    Gzip,
    /// DEFLATE compression with zlib header and Adler32 checksum.
    /// This option corresponds to HTTP Content-Encoding: deflate.
    Zlib,
    /// LZMA2-based content compression
    /// This option corresponds to HTTP Content-Encoding: xz
    Xz,
    /// Zstd compressor
    ///  This option corresponds to HTTP Content-Encoding: zstd
    Zstd,
    Brotli,
    Lz4,
    Bzip2,
    Snappy,
};

/// How the compression method is named in HTTP.
std::string toContentEncodingName(CompressionMethod method);

/** Choose compression method from path and hint.
  * if hint is "auto" or empty string, then path is analyzed,
  *  otherwise path parameter is ignored and hint is used as compression method name.
  * path is arbitrary string that will be analyzed for file extension (gz, br...) that determines compression.
  */
CompressionMethod chooseCompressionMethod(const std::string & path, const std::string & hint);

/** Choose a compression method from HTTP header list of supported compression methods.
  */
CompressionMethod chooseHTTPCompressionMethod(const std::string & list);

/// Get a range of the valid compression levels for the compression method.
std::pair<uint64_t, uint64_t> getCompressionLevelRange(const CompressionMethod & method);

/// Ceiling for the output of one DEFLATE block, for the places that decompress a request body before
/// the query owns the allocation. Real encoders stay three orders of magnitude below it: zlib flushes
/// roughly every lit_bufsize symbols and libdeflate's SOFT_MAX_BLOCK_LENGTH is 300000 bytes. Only a
/// crafted stream reaches it, and such a stream otherwise allocates one buffer of its own chosen size.
constexpr size_t MAX_DEFLATE_BLOCK_OUTPUT_FOR_REQUEST_BODY = 64u << 20;

/// `max_deflate_block_output` bounds the output of a single gzip/zlib block, which the decoder has to
/// buffer whole; 0 means no bound, and the allocation is covered by the memory tracker alone.
std::unique_ptr<ReadBuffer> wrapReadBufferWithCompressionMethod(
    std::unique_ptr<ReadBuffer> nested,
    CompressionMethod method,
    int zstd_window_log_max = 0,
    SnappyMode snappy_mode = SnappyMode::Basic,
    size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
    char * existing_memory = nullptr,
    size_t alignment = 0,
    size_t max_deflate_block_output = 0);

std::unique_ptr<WriteBuffer> wrapWriteBufferWithCompressionMethod(
    std::unique_ptr<WriteBuffer> nested,
    CompressionMethod method,
    int level,
    int zstd_window_log = 0,
    SnappyMode snappy_mode = SnappyMode::Basic,
    size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
    char * existing_memory = nullptr,
    size_t alignment = 0,
    bool compress_empty = true);

std::unique_ptr<WriteBuffer> wrapWriteBufferWithCompressionMethod(
    WriteBuffer * nested,
    CompressionMethod method,
    int level,
    int zstd_window_log,
    SnappyMode snappy_mode = SnappyMode::Basic,
    size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE,
    char * existing_memory = nullptr,
    size_t alignment = 0,
    bool compress_empty = true);

}
