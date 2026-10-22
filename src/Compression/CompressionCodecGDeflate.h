#pragma once

#include "config.h"

#if USE_GPU

#include <Compression/ICompressionCodec.h>

namespace DB
{

/// GDeflate of nvcomp (https://docs.nvidia.com/cuda/nvcomp/gdeflate.html), a variant of Deflate that a GPU expands in
/// parallel: a block is compressed in chunks of 64 KiB, each on its own, see `GPU::gdeflate_chunk_bytes`. Both the
/// compression and the expansion run on the server's GPU, and the device reads the chunks of a column with the other
/// compressed blocks it aggregates. Of a column of integers, a chunk whose integers lie close together is packed before it
/// is compressed, each in as many bits as it differs from the least of the chunk by, where that makes it smaller.
class CompressionCodecGDeflate : public ICompressionCodec
{
public:
    /// nvcomp's GDeflate algorithms, from the fastest, 0, to the densest, 5, of which 4 and 5 compress alike.
    static constexpr int DEFAULT_ALGORITHM = 4;
    static constexpr int MAX_ALGORITHM = 5;

    /// `element_size` 0 packs nothing.
    explicit CompressionCodecGDeflate(int algorithm_, UInt8 element_size_ = 0, bool is_signed_ = false);

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    String getDescription() const override
    {
        return "GDeflate of nvcomp, a Deflate a GPU expands in parallel, compressed and expanded on the server's GPU. Algorithms 0..5.";
    }

private:
    const int algorithm;
    /// Of the integers whose chunks are packed; not a part of the description, as a block holds how it is packed.
    const UInt8 element_size;
    const bool is_signed;
};

}

#endif
