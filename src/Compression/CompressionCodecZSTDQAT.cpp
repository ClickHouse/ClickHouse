#include "config.h"

#if USE_QATLIB

#include <Common/logger_useful.h>
#include <Compression/CompressionCodecZSTD.h>
#include <Compression/CompressionFactory.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/IAST.h>
#include <Poco/Logger.h>

#include <qatseqprod.h>
#include <zstd.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

/// Hardware-accelerated ZSTD. Supports only compression so far.
class CompressionCodecZSTDQAT : public CompressionCodecZSTD
{
public:
    static constexpr auto ZSTDQAT_SUPPORTED_MIN_LEVEL = 1;
    static constexpr auto ZSTDQAT_SUPPORTED_MAX_LEVEL = 12;
    static constexpr int ZSTDQAT_DEVICE_UNINITIALIZED = 0XFFFF;

    explicit CompressionCodecZSTDQAT(int level_);

protected:
    bool isZstdQat() const override { return true; }
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

private:
    const int level;
    LoggerPtr log;
    static std::atomic<int> qat_state; /// Global initialization status of QAT device, we fall back back to software compression if uninitialized
};

std::atomic<int> CompressionCodecZSTDQAT::qat_state = ZSTDQAT_DEVICE_UNINITIALIZED;

UInt32 CompressionCodecZSTDQAT::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    if (qat_state == ZSTDQAT_DEVICE_UNINITIALIZED)
    {
        qat_state = QZSTD_startQatDevice();
        if (qat_state == QZSTD_OK)
            LOG_DEBUG(log, "Initialization of hardware-assissted ZSTD_QAT codec successful");
        else
            LOG_WARNING(log, "Initialization of hardware-assisted ZSTD_QAT codec failed, falling back to software ZSTD codec -> status: {}", qat_state);
    }

    ZSTD_CCtx * cctx = ZSTD_createCCtx();
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);

    void * sequence_producer_state = nullptr;
    if (qat_state == QZSTD_OK)
    {
        sequence_producer_state = QZSTD_createSeqProdState();
        ZSTD_registerSequenceProducer(cctx, sequence_producer_state, qatSequenceProducer);
        ZSTD_CCtx_setParameter(cctx, ZSTD_c_enableSeqProducerFallback, 1);
    }

    size_t compressed_size = ZSTD_compress2(cctx, dest, ZSTD_compressBound(source_size), source, source_size);
    QZSTD_freeSeqProdState(sequence_producer_state);
    ZSTD_freeCCtx(cctx);

    if (ZSTD_isError(compressed_size))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZSTD_QAT codec: {}", ZSTD_getErrorName(compressed_size));

    return static_cast<UInt32>(compressed_size);
}

void registerCodecZSTDQAT(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::ZSTD_QPL);
    factory.registerCompressionCodec("ZSTD_QAT", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = CompressionCodecZSTD::ZSTD_DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "ZSTD_QAT codec must have 1 parameter, given {}", arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZSTD_QAT codec argument must be integer");

            level = static_cast<int>(literal->value.safeGet<UInt64>());
            if (level < CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MIN_LEVEL || level > CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MAX_LEVEL)
                /// that's a hardware limitation
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ZSTDQAT codec doesn't support level more than {} and lower than {} , given {}",
                    CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MAX_LEVEL, CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MIN_LEVEL, level);
        }

        return std::make_shared<CompressionCodecZSTDQAT>(level);
    });
}

CompressionCodecZSTDQAT::CompressionCodecZSTDQAT(int level_)
    : CompressionCodecZSTD(level_)
    , level(level_)
    , log(getLogger("CompressionCodecZSTDQAT"))
{
    setCodecDescription("ZSTD_QAT", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
}

}

#endif
