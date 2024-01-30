#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <double-conversion/utils.h>
#include "base/types.h"
#include "fsst12.h"

#include <fsst.h>
#include <cstring>
#include <stdexcept>
#include <vector>

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
        auto header_size = fsst_export(encoder, reinterpret_cast<unsigned char*>(dest));

        lenOut.reserve(params.n);
        strOut.reserve(params.n);
        auto compressed_count = fsst_compress(encoder, params.n, params.len_in.data(),
            const_cast<unsigned char**>(params.str_in.data()),
            source_size + FSST_MAXHEADER - header_size, reinterpret_cast<unsigned char*>(dest + header_size),
            lenOut.data(), strOut.data());
        if (compressed_count < params.n) {
            throw std::runtime_error("FSST compression failed");
        }
        auto compressed_size = lenOut.back() + (strOut.back() - reinterpret_cast<const unsigned char*>(source));
        fsst_destroy(encoder);
        return static_cast<UInt32>(header_size) + static_cast<UInt32>(compressed_size);
    }

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override {
        fsst_decoder_t decoder;
        unsigned char* mutable_source = const_cast<unsigned char*>(reinterpret_cast<const unsigned char*>(source));

        auto header_len = fsst_import(&decoder, mutable_source);
        auto uncompressed_len = fsst_decompress(&decoder, source_size,
            mutable_source + header_len, uncompressed_size,
            reinterpret_cast<unsigned char*>(dest));
        ASSERT(uncompressed_len == uncompressed_size)
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

    // void RebuildSymbolTable();

private:
    mutable std::vector<size_t> lenOut;
    mutable std::vector<unsigned char*> strOut;
    struct FsstParams {
        size_t n;
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
                    len_in.push_back(start + i - str_in.back());
                    str_in.push_back(start + i + 1);
                }
            }
            str_in.pop_back();
        }
    };
};

void registerCodecFSST(CompressionCodecFactory & factory);

}
