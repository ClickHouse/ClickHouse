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
        params = FsstParams(reinterpret_cast<const unsigned char*>(source), source_size);
        std::cerr << "params.n: " << params.n << std::endl;
        fsst_encoder_t *encoder = fsst_create(params.n, params.len_in,
                    const_cast<unsigned char**>(params.str_in), 1);

        size_t header_size = fsst_export(encoder, reinterpret_cast<unsigned char*>(dest));

        std::cerr << "exported" << std::endl;

        if (fsst_compress(encoder,
                        params.n,
                        params.len_in,
                        const_cast<unsigned char **>(params.str_in),
                        OUT_SIZE,
                        reinterpret_cast<unsigned char *>(dest + header_size),
                        params.len_out,
                        const_cast<unsigned char **>(params.str_out)) < params.n) {
            throw std::runtime_error("FSST compression failed");
        }

        std::cerr << "compressed" << std::endl;
        fsst_destroy(encoder);

        // auto compressed_size = params.len_out[params.n - 1] + (params.str_out[params.n - 1] - reinterpret_cast<const unsigned char*>(source));
        UInt32 compressed_size = 0;
        for (size_t i = 0; i < params.n; ++i) {
            compressed_size += params.len_out[i];
        }
        std::cerr << compressed_size << " " << header_size << std::endl;
        return static_cast<UInt32>(header_size) + compressed_size;
    }

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override {
        UNUSED(uncompressed_size);
        fsst_decoder_t decoder;
        size_t hdr = fsst_import(&decoder,
        reinterpret_cast<unsigned char *>(const_cast<char*>(source)));
        std::cerr << "header version: " << decoder.version << std::endl;

        std::cerr << "header size: " << hdr << std::endl;
        std::cerr << "source size: " << source_size << std::endl;

        auto decompressed_size = fsst_decompress(&decoder,
            source_size - hdr,
            reinterpret_cast<unsigned char*>(const_cast<char *>(source + hdr)),
            OUT_SIZE,
            reinterpret_cast<unsigned char *>(dest));

        std::cout << "decompressed size: " << decompressed_size << std::endl;
    }

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { 
        return uncompressed_size + FSST_MAXHEADER;
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    // void RebuildSymbolTable();

private:
    struct FsstParams {
        FsstParams() = default;
        size_t n = 0;
        // std::vector <size_t> len_in;
        // std::vector <const unsigned char*> str_in;
        // std::vector <size_t> len_out;
        // std::vector <const unsigned char*> str_out;
        size_t* len_in;
        const unsigned char** str_in;
        size_t* len_out;
        const unsigned char** str_out;

        explicit FsstParams(const unsigned char* start, UInt32 size) {
            for (UInt32 i = 0; i <= size; ++i) {
                if (start[i] == '\0') {
                    ++n;
                }
            }
            len_in = new size_t[n];
            len_out = new size_t[n];
            str_in = new const unsigned char*[n];
            str_out = new const unsigned char*[n];
            UInt32 ptr = 0;
            str_in[ptr] = start;

            for (UInt32 i = 0; i <= size; ++i) {
                if (start[i] == '\0') {
                    ++ptr;
                    str_in[ptr] = start + i + 1;
                    len_in[ptr - 1] = str_in[ptr] - str_in[ptr - 1] - 1;
                }
            }
            len_in[ptr] = (start + size) - str_in[ptr - 1] - 1;
        }
    };

    mutable FsstParams params;
};

void registerCodecFSST(CompressionCodecFactory & factory);

}
