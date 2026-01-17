#pragma once

#include "config.h"

#if USE_AVRO

#include <string>
#include <IO/ReadBuffer.h>
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

/// Utility class for reading raw Avro blocks.
/// Leverages Avro library where possible, adds ClickHouse-specific utilities.
class AvroBlockReader
{
public:
    /// Extract header state from an initialized DataFileReaderBase.
    /// This reuses the Avro library's header parsing (DRY).
    static AvroHeaderState extractHeaderState(avro::DataFileReaderBase & reader);

    /// Read a varint-encoded int64 (zigzag encoding) from buffer.
    /// Reuses ClickHouse's VarInt.h which is compatible with Avro.
    static int64_t readVarInt(ReadBuffer & in);

    /// Write a varint-encoded int64 to a string (for segment reconstruction).
    static void writeVarInt(int64_t value, std::string & out);

    /// Read a complete Avro block (objectCount + byteCount + compressedData).
    /// Does NOT consume the sync marker - caller must verify separately.
    /// Returns {object_count, compressed_data}.
    static std::pair<int64_t, std::string> readBlock(ReadBuffer & in);

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
