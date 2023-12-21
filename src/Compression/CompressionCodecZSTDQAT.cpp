#ifdef ENABLE_ZSTDQAT_COMPRESSION
#include <Compression/CompressionCodecZSTD.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <zstd.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Common/typeid_cast.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>
#include <qatseqprod.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

class CompressionCodecZSTDQAT : public CompressionCodecZSTD
{
public:
    /// QAT Hardware only supports compression levels L1 to L12
    static constexpr auto ZSTDQAT_SUPPORTED_MIN_LEVEL = 1;
    static constexpr auto ZSTDQAT_SUPPORTED_MAX_LEVEL = 12;
    explicit CompressionCodecZSTDQAT(int level_);
    ~CompressionCodecZSTDQAT() override;

protected:
    /// TODO: So far, QAT hardware only support compression. For next generation in future, it will support decompression as well.
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

private:
    const int level;
    ZSTD_CCtx * cctx;
    void * sequenceProducerState;
    Poco::Logger * log;
};

UInt32 CompressionCodecZSTDQAT::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    size_t compressed_size = ZSTD_compress2(cctx, dest, ZSTD_compressBound(source_size), source, source_size);

    if (ZSTD_isError(compressed_size))
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with ZSTD codec: {}", std::string(ZSTD_getErrorName(compressed_size)));

    return static_cast<UInt32>(compressed_size);
}

void registerCodecZSTDQAT(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodec("ZSTDQAT", {}, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = CompressionCodecZSTD::ZSTD_DEFAULT_LEVEL;
        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "ZSTDQAT codec must have 1 parameter, given {}", arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "ZSTDQAT codec argument must be integer");

            level = static_cast<int>(literal->value.safeGet<UInt64>());
            if (level > CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MAX_LEVEL || level < CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MIN_LEVEL)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                    "ZSTDQAT codec doesn't support level more than {} and lower than {} , given {}",
                    CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MAX_LEVEL, CompressionCodecZSTDQAT::ZSTDQAT_SUPPORTED_MIN_LEVEL, level);
        }

        return std::make_shared<CompressionCodecZSTDQAT>(level);
    });
}

CompressionCodecZSTDQAT::CompressionCodecZSTDQAT(int level_)
    : CompressionCodecZSTD(level_), level(level_), log(&Poco::Logger::get("CompressionCodecZSTDQAT"))
{
    setCodecDescription("ZSTDQAT", {std::make_shared<ASTLiteral>(static_cast<UInt64>(level))});
    cctx = ZSTD_createCCtx();
    /// Start QAT device, start QAT device at any time before compression job started
    int res = QZSTD_startQatDevice();
    LOG_WARNING(log, "Initialization of hardware-assisted(QAT) ZSTD codec result: {} ", static_cast<UInt32>(res));
    /// Create sequence producer state for QAT sequence producer
    sequenceProducerState = QZSTD_createSeqProdState();
    /// register qatSequenceProducer
    ZSTD_registerSequenceProducer(
        cctx,
        sequenceProducerState,
        qatSequenceProducer
    );
    /// Enable sequence producer fallback
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_enableSeqProducerFallback, 1);
    ZSTD_CCtx_setParameter(cctx, ZSTD_c_compressionLevel, level);
}

CompressionCodecZSTDQAT::~CompressionCodecZSTDQAT()
{
    /// Free sequence producer state
    if (sequenceProducerState != nullptr)
    {
        QZSTD_freeSeqProdState(sequenceProducerState);
        sequenceProducerState = nullptr;
    }
    if (cctx != nullptr)
    {
        auto status = ZSTD_freeCCtx(cctx);
        if (status != 0)
            LOG_WARNING(log, "ZSTD_freeCCtx failed with status: {} ", static_cast<UInt32>(status));
        cctx = nullptr;
    }
}

}
#endif
