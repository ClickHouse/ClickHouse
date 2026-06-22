#include <Compression/CompressionCodecQuantize.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/VectorQuantization.h>
#include <Common/SipHash.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Poco/String.h>

#include <cstring>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

CompressionCodecQuantize::CompressionCodecQuantize(const QuantizeCodecParams & params_)
    : params(params_)
{
    ASTs args;
    args.emplace_back(make_intrusive<ASTLiteral>(params.method));
    args.emplace_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(params.dimensions)));
    args.emplace_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(params.bits)));
    setCodecDescription("Quantize", args);
}

uint8_t CompressionCodecQuantize::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Quantize);
}

void CompressionCodecQuantize::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecQuantize::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    /// The full-precision data is stored verbatim (the codes live in a separate stream written by the serialization).
    memcpy(dest, source, source_size);
    return source_size;
}

UInt32 CompressionCodecQuantize::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size != uncompressed_size)
        throw Exception(decompression_error_code,
            "Wrong data for compression codec Quantize: source_size ({}) != uncompressed_size ({})",
            source_size, uncompressed_size);

    memcpy(dest, source, uncompressed_size);
    return uncompressed_size;
}

namespace
{

QuantizeCodecParams parseQuantizeCodecArguments(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() < 2 || arguments->children.size() > 3)
        throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
            "Codec Quantize requires 2 or 3 parameters: Quantize(method, dimensions[, bits])");

    const auto * method_literal = arguments->children[0]->as<ASTLiteral>();
    if (!method_literal || method_literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "First argument of codec Quantize (method) must be a string literal");

    const auto * dimensions_literal = arguments->children[1]->as<ASTLiteral>();
    if (!dimensions_literal || dimensions_literal->value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Second argument of codec Quantize (dimensions) must be an unsigned integer");

    QuantizeCodecParams params;
    params.method = method_literal->value.safeGet<String>();
    params.dimensions = dimensions_literal->value.safeGet<UInt64>();
    params.bits = 0;

    if (arguments->children.size() == 3)
    {
        const auto * bits_literal = arguments->children[2]->as<ASTLiteral>();
        if (!bits_literal || bits_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Third argument of codec Quantize (bits) must be an unsigned integer");
        params.bits = bits_literal->value.safeGet<UInt64>();
    }

    if (!VectorQuantization::isSupportedMethod(params.method))
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Unknown quantization method '{}' for codec Quantize", params.method);
    if (params.dimensions == 0)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Number of dimensions for codec Quantize must be greater than zero");
    if (VectorQuantization::bytesPerVector(params.method, params.dimensions, params.bits) == 0)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
            "Codec Quantize with method '{}' and {} dimensions produces an empty code; check that the dimensions are a multiple of 8",
            params.method, params.dimensions);

    return params;
}

}

std::optional<QuantizeCodecParams> tryExtractQuantizeCodecParams(const ASTPtr & codec_desc)
{
    if (!codec_desc)
        return {};

    const auto * func = codec_desc->as<ASTFunction>();
    if (!func || !func->arguments)
        return {};

    for (const auto & inner_codec_ast : func->arguments->children)
    {
        if (const auto * inner_func = inner_codec_ast->as<ASTFunction>())
        {
            if (Poco::toLower(inner_func->name) == "quantize")
                return parseQuantizeCodecArguments(inner_func->arguments);
        }
        else if (const auto * inner_identifier = inner_codec_ast->as<ASTIdentifier>())
        {
            if (Poco::toLower(inner_identifier->name()) == "quantize")
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "Codec Quantize requires parameters: Quantize(method, dimensions[, bits])");
        }
    }

    return {};
}

void registerCodecQuantize(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::Quantize);
    factory.registerCompressionCodec("Quantize", method_code, [](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        /// On the read path the codec is instantiated from its method byte alone (with no arguments) just to
        /// memcpy-decompress the verbatim full-precision stream; the parameters are not needed there. The semantic
        /// validation of the parameters happens when the codec is attached to a column (tryExtractQuantizeCodecParams).
        if (!arguments)
            return std::make_shared<CompressionCodecQuantize>(QuantizeCodecParams{});
        return std::make_shared<CompressionCodecQuantize>(parseQuantizeCodecArguments(arguments));
    });
}

}
