#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <double-conversion/utils.h>
#include "base/defines.h"
#include "base/types.h"
#include "fsst12.h"

#include <fsst.h>
#include <cassert>
#include <cstring>
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

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override {
        FsstParams params = FsstParams(reinterpret_cast<const unsigned char*>(source), source_size);
        fsst_encoder_t *encoder = fsst_create(params.n, params.len_in.data(),
                    const_cast<unsigned char**>(params.str_in.data()), 1);

        // if (fsst_compress(encoder, 1, lenIn, strIn,
        //                   source_size * 5 + 2166,
        //                   reinterpret_cast<unsigned char*>(dest + FSST_MAXHEADER + 3),
        //                   lenOut,
        //                   strOut) < 1) return -1;
        // lenOut[0] += 3 + hdr;
        // strOut[0] -= 3 + hdr;
        // SERIALIZE(lenOut[0], strOut[0])
        // std::copy(tmp, tmp + hdr, strOut[0] + 3);
        // fsst_destroy(encoder);

        // std::cerr << fsst_decoder(encoder).version << " version" << std::endl;

        // auto header_size = fsst_export(encoder, reinterpret_cast<unsigned char*>(dest));
        // lenOut.resize(params.n);
        // strOut.resize(params.n);

        // auto compressed_count = fsst_compress(encoder, params.n, params.len_in.data(),
        //     const_cast<unsigned char**>(params.str_in.data()),
        //     source_size + FSST_MAXHEADER - header_size + 100000, reinterpret_cast<unsigned char*>(dest + header_size),
        //     lenOut.data(), strOut.data());

        // if (compressed_count < params.n) {
        // std::cerr << "bebra " << "8" << std::endl;
        //     throw std::runtime_error("FSST compression failed");
        // }

        // auto compressed_size = lenOut.back() + (strOut.back() - reinterpret_cast<const unsigned char*>(source));
        // fsst_destroy(encoder);
        // return header_size;
        // return static_cast<UInt32>(header_size) + static_cast<UInt32>(compressed_size);
    }

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override {
        fsst_decoder_t decoder;
        unsigned char* mutable_source = const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(source));

        auto header_len = fsst_import(&decoder, mutable_source);
        std::cerr << "version: " << decoder.version << " " << header_len << std::endl;
        // auto uncompressed_len = fsst_decompress(&decoder, source_size,
        //     mutable_source + header_len, uncompressed_size,
        //     reinterpret_cast<unsigned char*>(dest));
        UNUSED(source_size, dest, uncompressed_size);
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    // void RebuildSymbolTable();

private:

    // mutable size_t lenIn[100];
    // mutable unsigned char* strIn[100];

    // mutable size_t lenOut[100];
    // mutable unsigned char* strOut[100];

    // mutable std::vector<size_t> lenOut;
    // mutable std::vector<unsigned char*> strOut;
    struct FsstParams {
        size_t n{0};
        std::vector <size_t> len_in;
        std::vector <const unsigned char*> str_in;

        explicit FsstParams(const unsigned char* start, UInt32 size) {
            for (UInt32 i = 0; i <= size; ++i) {
                if (start[i] == '\0') {
                    ++n;
                }
            }
            len_in.reserve(n);
            str_in.reserve(n);
            str_in.push_back(start);
            for (UInt32 i = 0; i <= size; ++i) {
                if (start[i] == '\0') {
                    len_in.push_back(i + str_in.back() - start);
                    str_in.push_back(start + i + 1);
                }
            }
            str_in.pop_back();
        }
    };
};

void registerCodecFSST(CompressionCodecFactory & factory);

}
