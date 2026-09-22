#include <Compression/CompressionFactory.h>
#include <Compression/CompressionInfo.h>
#include <Compression/ICompressionCodec.h>
#include <Compression/registerCompressionCodecs.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTFunction.h>


namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
extern const int LOGICAL_ERROR;
}

namespace
{

/// There is no standalone on-disk representation for the selector. This value is used only when the factory is
/// queried without a data type (for example by system.codecs); typed data streams are delegated to an existing codec.
class CompressionCodecTimeSeriesSamples final : public ICompressionCodec
{
public:
    /// This value is metadata-only. The selector has no byte code of its own and must be resolved with a data type.
    uint8_t getMethodByte() const override
    {
        return static_cast<uint8_t>(CompressionMethodByte::NONE);
    }

    ASTPtr getCodecDescription() const override
    {
        return makeCodecDescription("TimeSeriesSamples");
    }

    void updateHash(SipHash & hash) const override
    {
        getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
    }

protected:
    UInt32 doCompressData(const char *, UInt32, char *) const override
    {
        throwMustNotBeInvokedDirectly();
    }

    UInt32 doDecompressData(const char *, UInt32, char *, UInt32) const override
    {
        throwMustNotBeInvokedDirectly();
    }

    bool isCompression() const override { return false; }
    bool isGenericCompression() const override { return false; }
    bool isNone() const override { return true; }

    String getDescription() const override
    {
        return "Selects Delta plus T64 for timestamp streams and Gorilla for floating-point streams; no standalone on-disk codec.";
    }

private:
    [[noreturn]] static void throwMustNotBeInvokedDirectly()
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecTimeSeriesSamples must not be invoked directly: it has no standalone on-disk representation");
    }
};

CompressionCodecPtr makeCodec(CompressionCodecFactory & factory, const char * name, const IDataType * column_type)
{
    return factory.get(makeASTFunction("CODEC", makeASTFunction(name)), column_type);
}

CompressionCodecPtr makeTimestampCodec(CompressionCodecFactory & factory, const IDataType * column_type)
{
    return factory.get(
        makeASTFunction("CODEC", makeASTFunction("Delta"), makeASTFunction("T64")),
        column_type);
}

}

void registerCodecTimeSeriesSamples(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodecWithType(
        "TimeSeriesSamples",
        std::nullopt,
        [&factory](const ASTPtr & arguments, const IDataType * column_type) -> CompressionCodecPtr
        {
            if (arguments && !arguments->children.empty())
                throw Exception(
                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "TimeSeriesSamples codec must not have arguments, given {}",
                    arguments->children.size());

            if (!column_type)
                return std::make_shared<CompressionCodecTimeSeriesSamples>();

            const WhichDataType which(column_type);
            if (which.isDateTime() || which.isDateTime64())
                return makeTimestampCodec(factory, column_type);

            if (which.isFloat32() || which.isFloat64())
                return makeCodec(factory, "Gorilla", column_type);

            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Codec TimeSeriesSamples cannot be applied to column {}: expected DateTime, DateTime64, Float32 or Float64",
                column_type->getName());
        });
}

}
