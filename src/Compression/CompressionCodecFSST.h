#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <double-conversion/utils.h>
#include "base/defines.h"
#include "base/types.h"

#include <fsst.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <exception>
#include <stdexcept>
#include <vector>
#include <iostream>

#define SERIALIZE(l,p) { (p)[0] = ((l)>>16)&255; (p)[1] = ((l)>>8)&255; (p)[2] = (l)&255; }

namespace DB
{

class CompressionCodecFSST : public ICompressionCodec
{
private:
    using SplittedLens = size_t*;
    using SplittedMutableRows = unsigned char**;
    using SplittedConstRows = const unsigned char**;

public:
    explicit CompressionCodecFSST() {
        setCodecDescription("FSST");
    }

    uint8_t getMethodByte() const override {
        return static_cast<uint8_t>(CompressionMethodByte::FSST);
    }

    void updateHash(SipHash & hash) const override {
        getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    }

    static const int OUT_SIZE = 2281337; // only for test runs

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override {
        const size_t rows_count{countRowsInData(reinterpret_cast<const unsigned char*>(source), source_size)};
        size_t len_in[rows_count];
        const unsigned char* str_in[rows_count];
        splitDataByRows(reinterpret_cast<const unsigned char*>(source), source_size, str_in, len_in);
        
        ///
        std::cerr << "rows_count " << rows_count << std::endl;
        for (size_t i = 0; i < rows_count; ++i) {
            std::cout << len_in[i] << " " << str_in[i] << std::endl;
        }
        ///

        fsst_encoder_t *encoder = fsst_create(rows_count, len_in,
                    const_cast<SplittedMutableRows>(str_in), 1);

        std::cerr << "encoder created" << std::endl;

        size_t fsst_header_size = fsst_export(encoder, reinterpret_cast<unsigned char*>(dest));

        size_t len_out[rows_count];
        const unsigned char* str_out[rows_count];
        size_t header_size{fsst_header_size + sizeof(rows_count) + sizeof(len_out) + sizeof(str_out)};
        /* codec_header |(dest*) fsst_header(encoder) rows_count len_out str_out data */

        std::cerr << "header size: " << header_size << std::endl;

        if (fsst_compress(encoder,
                        rows_count,
                        len_in,
                        const_cast<SplittedMutableRows>(str_in),
                        OUT_SIZE, /* дичь какая-то */
                        reinterpret_cast<unsigned char *>(dest + header_size),
                        len_out,
                        const_cast<SplittedMutableRows>(str_out)) < rows_count) {
            throw std::runtime_error("FSST compression failed");
        }
        fsst_destroy(encoder);

        std::cerr << "compressed" << std::endl;

        /* Copy prerequisites to dest */
        memcpy(dest + fsst_header_size, &rows_count, sizeof(rows_count));
        memcpy(dest + fsst_header_size + sizeof(rows_count), len_out, sizeof(len_out));
        memcpy(dest + fsst_header_size + sizeof(rows_count) + sizeof(len_out), str_out, sizeof(str_out));

        std::cerr << "copied" << std::endl;

        /* Count data total compressed size without header */
        UInt32 compressed_size{0};
        for (size_t i = 0; i < rows_count; ++i) {
            compressed_size += len_out[i];
        }

        std::cerr << compressed_size << " " << header_size << std::endl;

        return static_cast<UInt32>(header_size) + compressed_size;
    }

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override {
        UNUSED(uncompressed_size);
        fsst_decoder_t decoder;
        size_t fsst_header_size = fsst_import(&decoder,
        reinterpret_cast<unsigned char *>(const_cast<char*>(source)));

        size_t rows_count;
        memcpy(&rows_count, source + fsst_header_size, sizeof(rows_count));

        size_t lens[rows_count];
        unsigned char* strs[rows_count]; /* Mutable */
        memcpy(lens, source + fsst_header_size + sizeof(rows_count), sizeof(lens));
        memcpy(strs, source + fsst_header_size + sizeof(rows_count) + sizeof(lens), sizeof(strs));

        std::cerr << "header size: " << fsst_header_size << "source size: " << source_size << "rows count: " << rows_count << std::endl;

        size_t shift{0};
        for (size_t i = 0; i < rows_count; ++i) {
            auto decompressed_size = fsst_decompress(&decoder,
                lens[i],
                strs[i],
                OUT_SIZE, /* дичь какая-то */
                reinterpret_cast<unsigned char *>(dest + shift)
            );
            shift += lens[i];
            std::cout << "decompressed size: " << decompressed_size << std::endl;
        }
    }

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { 
        return uncompressed_size + FSST_MAXHEADER;
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    size_t countRowsInData(const unsigned char* data, UInt32 size) const {
        size_t rows_count{0};
        for (UInt32 i = 0; i < size; ++i) {
            if (data[i] == '\0') {
                ++rows_count;
            }
        }
        return rows_count;
    }

    void splitDataByRows(const unsigned char* data, UInt32 size, SplittedConstRows rows, SplittedLens lens) const {
        UInt32 ptr = 0;
        rows[ptr] = data;

        for (UInt32 i = 0; i < size; ++i) {
            if (data[i] == '\0') {
                ++ptr;
                rows[ptr] = data + i + 1;
                lens[ptr - 1] = rows[ptr] - rows[ptr - 1] - 1;
            }
        }
        lens[ptr] = (data + size) - rows[ptr - 1] - 1;
    }
};

void registerCodecFSST(CompressionCodecFactory & factory);
}
