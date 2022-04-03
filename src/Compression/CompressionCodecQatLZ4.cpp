#include "QatCodec.h"

#include <Poco/Logger.h>
#include <base/logger_useful.h>

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/BufferWithOwnMemory.h>

#pragma GCC diagnostic ignored "-Wold-style-cast"


namespace DB
{
#define _REPLACE_LZ4_
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

class CompressionCodecQatLZ4 : public  ICompressionCodec  // TODO: can be derived from CompressionCodecLZ4 to only replace compression or decompression, to let QAT only do com or dec task.
{
public:
    explicit CompressionCodecQatLZ4();
    ~CompressionCodecQatLZ4() override;

    uint8_t getMethodByte() const override;

    unsigned int getAdditionalSizeAtTheEndOfBuffer() const override { return LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER; }  //todo: can be removed?

    void updateHash(SipHash & hash) const override;
    unsigned int getMaxCompressedDataSize(unsigned int uncompressed_size) const override;

protected:
    unsigned int doCompressData(const char * source, unsigned int source_size, char * dest) const override;
    void doDecompressData(const char * source, unsigned int source_size, char * dest, unsigned int uncompressed_size) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }

private:
    Poco::Logger * log = &Poco::Logger::get("CompressionCodecQatLZ4");
};

CompressionCodecQatLZ4::CompressionCodecQatLZ4()
{
#ifdef _REPLACE_LZ4_
    setCodecDescription("LZ4");
#else
    setCodecDescription("QATLZ4");
#endif

    LOG_TRACE(log, "CompressionCodecQatLZ4() called.");
}

CompressionCodecQatLZ4::~CompressionCodecQatLZ4()
{
    LOG_TRACE(log, "~CompressionCodecQatLZ4() called.");
}

uint8_t CompressionCodecQatLZ4::getMethodByte() const
{
#ifdef _REPLACE_LZ4_
    return static_cast<uint8_t>(CompressionMethodByte::LZ4);
#else
    return static_cast<uint8_t>(CompressionMethodByte::QATLZ4);
#endif
}

void CompressionCodecQatLZ4::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}
#define CEIL_DIV(x, y) (((x) + (y)-1) / (y))

unsigned int CompressionCodecQatLZ4::getMaxCompressedDataSize(unsigned int uncompressed_size) const
{
    unsigned int max_compressed_size = qat_GetMaxCoDataSize(uncompressed_size);

    LOG_TRACE(log, "QatLz4 getMaxCompressedDataSize ,uncompressed_size {}, max compressed size {} .", uncompressed_size, max_compressed_size );

    return max_compressed_size;
}

unsigned int CompressionCodecQatLZ4::doCompressData(const char * source, unsigned int source_size, char * dest) const
{
    LOG_TRACE(log, "doCompressData called.");

    unsigned int dest_size = qat_Compress(source, source_size, dest);

    if (0 == dest_size)
    {
        LOG_WARNING(log, "QATLZ4 compress failed!");
        throw Exception("Cannot compress, compress return error", ErrorCodes::CANNOT_COMPRESS);
    }

    LOG_TRACE(log, "doCompressData called done.");
    return dest_size;
}

void CompressionCodecQatLZ4::doDecompressData(const char * source, unsigned int source_size, char * dest, unsigned int uncompressed_size) const
{
    LOG_TRACE(log, "doDecompressData called.");

    int32_t  ret = QZ_OK;
    ret = qat_Decompress(source,  source_size,  dest, uncompressed_size)
    if (ret != 0)
    {
        LOG_WARNING(log, "Cannot decompress, decompress return error code {}!", ret);
        throw Exception("Cannot decompress, decompress return error", ErrorCodes::CANNOT_DECOMPRESS);
    }
    LOG_TRACE(log, "doDecompressData called done.");
}

void registerCodecQatLZ4(CompressionCodecFactory & factory)
{
    LOG_TRACE(&Poco::Logger::get("CompressionCodecQatLZ4"), "registerCodecQatLZ4 called.");

#ifdef _REPLACE_LZ4_
    factory.registerSimpleCompressionCodec("LZ4", static_cast<UInt8>(CompressionMethodByte::LZ4), [&] ()
    {
        return std::make_shared<CompressionCodecQatLZ4>();
    });
#else
    factory.registerSimpleCompressionCodec("QATLZ4", static_cast<UInt8>(CompressionMethodByte::QATLZ4), [&] ()
    {
        return std::make_shared<CompressionCodecQatLZ4>();
    });
#endif    
}

}
