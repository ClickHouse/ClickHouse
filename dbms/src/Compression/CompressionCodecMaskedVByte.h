#pragma once

#include <Compression/ICompressionCodec.h>

#include <inttypes.h>
#include <stdint.h> // please use a C99-compatible compiler
#include <stddef.h>

namespace DB {

    class CompressionCodecMaskedVByte : public ICompressionCodec
    {
    public:
        CompressionCodecMaskedVByte(UInt8 masked_bytes_size_);

        String getCodecDesc() const override;
        UInt8 getMethodByte() const override;

    protected:
        UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

        void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

        UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { return uncompressed_size + 2; }

        // Encode an array of a given length read from in to bout in varint format.
        // Returns the number of bytes written.
        size_t vbyte_encode(uint32_t *in, size_t length, uint8_t *bout);

        // Read "length" 32-bit integers in varint format from in, storing the result in out.  Returns the number of bytes read.
        size_t masked_vbyte_decode(const uint8_t *in, uint32_t *out, uint64_t length);
    };

    class CompressionCodecFactory;
    void registerCodecDelta(CompressionCodecFactory & factory);

}