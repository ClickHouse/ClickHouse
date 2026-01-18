#pragma once

#include "config.h"

#if USE_AVRO

#include <string>
#include <IO/ReadBuffer.h>
#include <IO/BufferWithOwnMemory.h>
#include <DataFile.hh>

namespace DB
{

/// Parsed Avro file header information needed for parallel parsing.
/// Can be extracted from an existing DataFileReaderBase after init().
struct AvroHeaderState
{
    avro::ValidSchema schema;
    avro::DataFileSync sync_marker{};  /// 16-byte sync marker
    avro::Codec codec = avro::NULL_CODEC;
};

/// Adapter to use ClickHouse ReadBuffer as Avro InputStream.
/// Used to delegate header parsing to the Avro library.
class ReadBufferInputStream : public avro::InputStream
{
public:
    explicit ReadBufferInputStream(ReadBuffer & in_) : in(in_), bytes_read(0) {}

    bool next(const uint8_t ** data, size_t * len) override
    {
        if (in.eof())
            return false;
        *data = reinterpret_cast<const uint8_t *>(in.position());
        *len = in.available();
        bytes_read += *len;
        in.position() += *len;
        return true;
    }

    void backup(size_t len) override
    {
        in.position() -= len;
        bytes_read -= len;
    }

    void skip(size_t len) override
    {
        in.ignore(len);
        bytes_read += len;
    }

    size_t byteCount() const override { return bytes_read; }

private:
    ReadBuffer & in;
    size_t bytes_read;
};

/// Utility class for reading raw Avro blocks.
/// Leverages Avro library where possible, adds ClickHouse-specific utilities.
class AvroBlockReader
{
public:
    /// Parse Avro file header directly from ReadBuffer.
    /// Positions the buffer at the start of the first block after parsing.
    /// This avoids issues with the Avro library's stream adapter buffering.
    static AvroHeaderState parseHeader(ReadBuffer & in);

    /// Read a complete Avro block directly into Memory<>, avoiding string allocation.
    /// Appends: objectCount (varint) + byteCount (varint) + compressedData.
    /// Does NOT consume the sync marker - caller must verify separately.
    /// Returns object_count.
    static int64_t readBlockInto(ReadBuffer & in, Memory<> & memory);

    /// Read and verify sync marker matches expected (16 bytes).
    /// Returns true if matches, throws on mismatch.
    /// Returns false only on clean EOF before any bytes read.
    static bool verifySyncMarker(ReadBuffer & in, const avro::DataFileSync & expected);

    /// Decompress block data - delegates to avro::DataFileReaderBase::decompressBlock().
    static void decompressBlock(
        const char * data,
        size_t size,
        avro::Codec codec,
        std::string & out);
};

}

#endif
