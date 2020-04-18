#pragma once

#include <Compression/ICompressionCodec.h>

namespace DB
{

    class CompressionCodecGroupVarint : public ICompressionCodec
    {
    public:
        CompressionCodecGroupVarint(UInt8 data_bytes_size_);

        UInt8 getMethodByte() const override;

        String getCodecDesc() const override;

        void useInfoAboutType(DataTypePtr data_type) override;

    protected:
        void compressData(const UInt32 * source, UInt32 source_size,  UInt8 * dest);

        void decompressData(const UInt8 * source, UInt32 source_size, Uint32 * dest);

    };

    class CompressionCodecFactory;
    void registerCodecGroupVarint(CompressionCodecFactory & factory);

}
