#ifdef ENABLE_FSST

#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <IO/VarInt.h>
#include <base/types.h>

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <stdexcept>

#pragma clang diagnostic ignored "-Wcast-align"
#pragma clang diagnostic ignored "-Wcast-qual"
#pragma clang diagnostic ignored "-Wold-style-cast"
#pragma clang diagnostic ignored "-Wimplicit-fallthrough"

#include <fsst.h>

#include <iostream>

namespace DB
{

class CompressionCodecFSST : public ICompressionCodec
{
public:
    explicit CompressionCodecFSST() { setCodecDescription("FSST"); }

    uint8_t getMethodByte() const override { return static_cast<uint8_t>(CompressionMethodByte::FSST); }

    void updateHash(SipHash & hash) const override { getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/true); }

    static constexpr int out_size = 2281337;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override
    {
        char* saved_dest{dest};
        std::vector<size_t> len_in;
        std::vector<const unsigned char *> str_in;

        splitDataByRows(reinterpret_cast<const unsigned char *>(source), str_in, len_in, source_size);
        size_t rows_count{len_in.size()};
        UInt32 compressed_size{0};

        fsst_encoder_t * encoder = fsst_create(rows_count, len_in.data(), str_in.data(), 0);

        size_t fsst_header_size = fsst_export(encoder, reinterpret_cast<unsigned char *>(dest));

        size_t len_out[rows_count];
        unsigned char * str_out[rows_count];
        size_t header_size{fsst_header_size + sizeof(rows_count) + sizeof(compressed_size)};
        /* codec_header |(dest*) fsst_header(encoder) rows_count compressed_size data compressed_lens */

        if (fsst_compress(
                encoder,
                rows_count,
                len_in.data(),
                str_in.data(),
                out_size,
                reinterpret_cast<unsigned char *>(dest + header_size),
                len_out,
                str_out)
            < rows_count)
            throw std::runtime_error("FSST compression failed");
        /* fsst_destroy(encoder); TODO(ebek): Понять почему вызывается деструктор какого-то левого кодека */

        /* Copy prerequisites to dest */
        memcpy(dest + fsst_header_size, &rows_count, sizeof(rows_count));

        /* Count data total compressed size without header */
        for (size_t i = 0; i < rows_count; ++i) {
            compressed_size += len_out[i];
        }
        memcpy(dest + fsst_header_size + sizeof(rows_count), &compressed_size, sizeof(compressed_size));

        dest += header_size + compressed_size;
        for (size_t i = 0; i < rows_count; ++i) {
            dest = writeVarUInt(len_out[i], dest);
        }

        return static_cast<UInt32>(dest - saved_dest);
    }

    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override
    {
        UNUSED(uncompressed_size, source_size);
        fsst_decoder_t decoder;
        size_t fsst_header_size = fsst_import(&decoder, reinterpret_cast<unsigned char *>(const_cast<char *>(source)));

        size_t rows_count;
        UInt32 compressed_size;
        memcpy(&rows_count, source + fsst_header_size, sizeof(rows_count));
        memcpy(&compressed_size, source + fsst_header_size + sizeof(rows_count), sizeof(compressed_size));

        size_t header_size{fsst_header_size + sizeof(rows_count) + sizeof(compressed_size)};

        const char * str{source + header_size};
        const char * lens{source + header_size + compressed_size};
        for (size_t i = 0; i < rows_count; ++i)
        {
            UInt64 len;
            lens = readVarUInt(len, lens, sizeof(len));
            unsigned char decoded_str[10 * len];

            auto decompressed_size = fsst_decompress(
                &decoder,
                len,
                reinterpret_cast<const unsigned char *>(str),
                OUT_SIZE,
                decoded_str);

            dest = writeVarUInt(decompressed_size, dest);
            memcpy(dest, decoded_str, decompressed_size);

            str += len;
            dest += decompressed_size;
        }
    }

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { return uncompressed_size + FSST_MAXHEADER; }

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    void splitDataByRows(
        const unsigned char * data, std::vector<const unsigned char *> & rows, std::vector<size_t> & lens, UInt32 source_size) const
    {
        const unsigned char * end{data + source_size};

        while (data != end)
        {
            UInt64 cur_len;
            data = reinterpret_cast<const unsigned char *>(readVarUInt(cur_len, reinterpret_cast<const char *>(data), source_size));
            lens.push_back(cur_len);
            rows.push_back(data);
            data += cur_len;
        }
    }
};

void registerCodecFSST(CompressionCodecFactory & factory)
{
    auto codec_builder = [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        UNUSED(arguments);
        return std::make_shared<CompressionCodecFSST>();
    };
    factory.registerCompressionCodec("FSST", static_cast<UInt8>(CompressionMethodByte::FSST), codec_builder);
}

}

#endif
