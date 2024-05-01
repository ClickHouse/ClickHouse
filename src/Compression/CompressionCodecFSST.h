#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <IO/VarInt.h>
#include "base/types.h"

#include <fsst.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#include <iostream>

namespace DB
{

class CompressionCodecFSST : public ICompressionCodec
{
private:
    using SplittedMutableLens = size_t*;
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
        std::cerr << "Fsst compress " << source_size << " " << strlen(dest) << std::endl;

        std::vector<size_t> len_in;
        std::vector<const unsigned char*> str_in;

        splitDataByRows(reinterpret_cast<const unsigned char*>(source), str_in, len_in, source_size);
        size_t rows_count{len_in.size()};

        fsst_encoder_t *encoder = fsst_create(rows_count, len_in.data(),
                    const_cast<SplittedMutableRows>(str_in.data()), 0);

        size_t fsst_header_size = fsst_export(encoder, reinterpret_cast<unsigned char*>(dest));

        size_t len_out[rows_count];
        const unsigned char* str_out[rows_count];
        size_t header_size{fsst_header_size + sizeof(rows_count) + sizeof(len_out) + sizeof(str_out)};
        /* codec_header |(dest*) fsst_header(encoder) rows_count len_out str_out data */

        if (fsst_compress(encoder,
                        rows_count,
                        len_in.data(),
                        const_cast<SplittedMutableRows>(str_in.data()),
                        OUT_SIZE, /* дичь какая-то */
                        reinterpret_cast<unsigned char *>(dest + header_size),
                        len_out,
                        const_cast<SplittedMutableRows>(str_out)) < rows_count) {
            throw std::runtime_error("FSST compression failed");
        }
        fsst_destroy(encoder);

        /* Copy prerequisites to dest */
        memcpy(dest + fsst_header_size, &rows_count, sizeof(rows_count));
        memcpy(dest + fsst_header_size + sizeof(rows_count), len_out, sizeof(len_out));
        memcpy(dest + fsst_header_size + sizeof(rows_count) + sizeof(len_out), str_out, sizeof(str_out));

        /* Count data total compressed size without header */
        std::cerr << "Compress" << std::endl; 
        UInt32 compressed_size{0};
        for (size_t i = 0; i < rows_count; ++i) {
            compressed_size += len_out[i];
            std::cerr << len_out[i] << std::endl;
            for (size_t j = 0; j < len_out[i]; ++j) {
                std::cerr << (size_t)str_out[j];
            }
            std::cerr << std::endl;
        }

        return static_cast<UInt32>(header_size) + compressed_size;
    }

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override {
        std::cerr << "Fsst decompress" << std::endl;

        UNUSED(uncompressed_size, source_size);
        fsst_decoder_t decoder;
        size_t fsst_header_size = fsst_import(&decoder,
        reinterpret_cast<unsigned char *>(const_cast<char*>(source)));

        size_t rows_count;
        memcpy(&rows_count, source + fsst_header_size, sizeof(rows_count));

        size_t lens[rows_count];
        unsigned char* strs[rows_count]; /* Mutable */
        memcpy(lens, source + fsst_header_size + sizeof(rows_count), sizeof(lens));
        memcpy(strs, source + fsst_header_size + sizeof(rows_count) + sizeof(lens), sizeof(strs));

        auto* kek = dest;

        // std::cerr << static_cast<void*>(kek) << std::endl;
        for (size_t i = 0; i < rows_count; ++i) {
            std::cerr << "Debug " << lens[i] << " " << std::endl;
            std::cerr << static_cast<void*>(dest) << std::endl;
            for (size_t j = 0; j < lens[i]; ++j) {
                std::cerr << (size_t)strs[j];
            }
            std::cerr << std::endl;
            dest = writeVarUInt(lens[i], dest);
            std::cerr << "Cringe " << size_t(*(dest - 1)) << std::endl;
            // std::cerr << static_cast<void*>(dest) << std::endl;
            auto decompressed_size = fsst_decompress(&decoder,
                lens[i],
                strs[i],
                OUT_SIZE, /* дичь какая-то */
                reinterpret_cast<unsigned char *>(dest)
            );
            // std::cerr << static_cast<void*>(dest) << std::endl;
            std::cerr << "decompressed " << decompressed_size << std::endl;
            dest += decompressed_size;
        }

        std::cerr << "Data " << kek << std::endl;
    }

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { 
        return uncompressed_size + FSST_MAXHEADER;
    }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    void splitDataByRows(
        const unsigned char* data,
        std::vector<const unsigned char*>& rows,
        std::vector<size_t>& lens,
        UInt32 source_size
    ) const {
        const unsigned char* end{data + source_size};

        while (data != end) {
            UInt64 cur_len;
            data = reinterpret_cast<const unsigned char*>(
                readVarUInt(cur_len, reinterpret_cast<const char*>(data), source_size)
            );
            lens.push_back(cur_len);
            rows.push_back(data);
            data += cur_len;
        }
    }
};

void registerCodecFSST(CompressionCodecFactory & factory);
}
