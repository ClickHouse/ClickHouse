#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

class CompressionCodecMultiple final : public ICompressionCodec
{
public:
    CompressionCodecMultiple(Codecs codecs);

    char getMethodByte() override;

    void getCodecDesc(String & codec_desc) override;

    size_t compress(char * source, size_t source_size, char * dest) override;

    size_t getCompressedReserveSize(size_t uncompressed_size) override;

    size_t decompress(char *source, size_t source_size, char *dest, size_t decompressed_size) override;

private:
    Codecs codecs;
    String codec_desc;

};

}
