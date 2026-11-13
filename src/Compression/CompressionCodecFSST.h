#pragma once

#include <Compression/FSST.h>
#include <Compression/ICompressionCodec.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

/** The `FSST` codec compresses every block with its own FSST symbol table (see `FSST.h`).
  *
  * The compressed data (after the block header) is:
  * - one byte of the format: `STORED` if the data is stored as is, because FSST does not make it smaller,
  *   `ENCODED`, or `ENCODED_ALIGNED`;
  * - for `ENCODED`: the serialized symbol table, then the codes until the end of the block;
  * - for `ENCODED_ALIGNED`: the serialized symbol table, the size of the codes (`UInt32`), the codes, then the index
  *   of the values: the number of the boundaries of the values in the block, and the positions of their codes,
  *   as `VarUInt` differences with the previous one. A reader finds where a value ends by the index,
  *   without walking over its codes.
  *
  * The codec does not interpret the data, but it is effective only for strings. A `String` column stores
  * the sizes of its values in a separate stream (`string_serialization_version = 'with_size_stream'`),
  * so the data stream is the concatenation of the values, and the reader can check the values against
  * a filter without decoding them (see `CompressedReadBufferFromFile::readFSSTBlock`).
  */
class CompressionCodecFSST : public ICompressionCodec
{
public:
    enum class Format : UInt8
    {
        STORED = 0,
        ENCODED = 1,
        /// Encoded with the boundaries of the values (see `IWriteBufferWithValueBoundaries`): no symbol crosses
        /// a boundary, so every value that starts in the block is encoded exactly as `FSST::encodeValue` encodes it.
        ENCODED_ALIGNED = 2,
    };

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return false; }
    String getDescription() const override
    {
        return "Fast Static Symbol Table string compression; values can be filtered without decompressing them.";
    }

    bool needsValueBoundaries() const override { return true; }

    /// An encoded block: the codes, and the symbol table, which is filled by `parseBlock`.
    struct EncodedBlock
    {
        FSST::SymbolTable table;
        const UInt8 * codes = nullptr;
        size_t codes_size = 0;
        /// The format is `ENCODED_ALIGNED`.
        bool aligned = false;
        /// For `ENCODED_ALIGNED`, if requested: the positions of the codes at the boundaries of the values, increasing.
        VectorWithMemoryTracking<UInt32> boundary_code_positions;
        /// The code of the exception for malformed codes, which are detected while they are processed.
        int error_code = 0;
    };

    /// Parses the compressed data of a block (without the block header) of `decompressed_size` bytes.
    /// Returns false if the block is stored without encoding. Throws an exception with `error_code` on malformed data.
    /// The codes are not validated here: every function that processes them checks them (see `FSST::decode`),
    /// so that the block is not read one more time.
    static bool parseBlock(
        const char * source, size_t source_size, size_t decompressed_size, EncodedBlock & block, int error_code, bool read_index);

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    UInt32 doCompressDataWithValueBoundaries(
        const char * source, UInt32 source_size, char * dest, std::span<const UInt32> value_boundaries) const override;
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

private:
    UInt32 compressImpl(const char * source, UInt32 source_size, char * dest, const std::span<const UInt32> * value_boundaries) const;
};

}
