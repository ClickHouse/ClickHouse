#include <Compression/CompressionCodecQuantized.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/ProductQuantizer.h>
#include <Common/VectorQuantizer.h>
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

CompressionCodecQuantized::CompressionCodecQuantized(const QuantizedCodecParams & params_)
    : params(params_)
{
    ASTs args;
    args.emplace_back(make_intrusive<ASTLiteral>(params.method));
    args.emplace_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(params.dimensions)));
    args.emplace_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(params.bits)));
    if (params.method == "product")
        args.emplace_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(params.m)));
    setCodecDescription("Quantized", args);
}

uint8_t CompressionCodecQuantized::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::Quantized);
}

void CompressionCodecQuantized::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecQuantized::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    memcpy(dest, source, source_size);
    return source_size;
}

UInt32 CompressionCodecQuantized::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    if (source_size != uncompressed_size)
        throw Exception(decompression_error_code,
            "Wrong data for compression codec Quantized: source_size ({}) != uncompressed_size ({})",
            source_size, uncompressed_size);

    memcpy(dest, source, uncompressed_size);
    return uncompressed_size;
}

namespace
{

QuantizedCodecParams parseQuantizeCodecArguments(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.size() < 2 || arguments->children.size() > 4)
        throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
            "Codec Quantized requires 2 to 4 parameters: Quantized(method, dimensions[, bits[, m]]) ");

    const auto * method = arguments->children[0]->as<ASTLiteral>();
    if (!method || method->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "First argument of codec Quantized (method) must be a string literal");

    const auto * dimensions = arguments->children[1]->as<ASTLiteral>();
    if (!dimensions || dimensions->value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Second argument of codec Quantized (dimensions) must be an unsigned integer");

    QuantizedCodecParams params;
    params.method = method->value.safeGet<String>();
    params.dimensions = dimensions->value.safeGet<UInt64>();

    /// Sugar for the prefix method: Quantized('prefix', dimensions, leading_dimensions, 'int8'|'bf16'). It stores only the
    /// leading `leading_dimensions` of the vector, quantized to int8 (per-vector scale) or bfloat16 (useful for Matryoshka
    /// embeddings). Fold the format into the canonical method name ('prefix_int8'/'prefix_bf16') and put the prefix length
    /// in the `bits` slot, so the rest of the pipeline (serialization, planner, distance) uses it like any other method.
    if (params.method == "prefix")
    {
        if (arguments->children.size() != 4)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec Quantized method 'prefix' requires Quantized('prefix', dimensions, leading_dimensions, format)");

        const auto * leading_dimensions_literal = arguments->children[2]->as<ASTLiteral>();
        if (!leading_dimensions_literal || leading_dimensions_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                "Third argument of codec Quantized('prefix', ...) (number of leading dimensions) must be an unsigned integer");
        params.bits = leading_dimensions_literal->value.safeGet<UInt64>();

        const auto * format_literal = arguments->children[3]->as<ASTLiteral>();
        if (!format_literal || format_literal->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                "Fourth argument of codec Quantized('prefix', ...) (format) must be a string literal: 'int8' or 'bf16'");
        const String format = format_literal->value.safeGet<String>();
        if (format != "int8" && format != "bf16")
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                "Codec Quantized('prefix', ...): format must be 'int8' or 'bf16', got '{}'", format);
        params.method = "prefix_" + format;

        if (const std::string error = VectorQuantizer::validateParams(params.method, params.dimensions, params.bits); !error.empty())
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec Quantized: {}", error);
        return params;
    }

    if (arguments->children.size() >= 3)
    {
        const auto * bits_literal = arguments->children[2]->as<ASTLiteral>();
        if (!bits_literal || bits_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Third argument of codec Quantized (bits) must be an unsigned integer");
        params.bits = bits_literal->value.safeGet<UInt64>();
    }
    if (arguments->children.size() >= 4)
    {
        const auto * m_literal = arguments->children[3]->as<ASTLiteral>();
        if (!m_literal || m_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Fourth argument of codec Quantized (m, number of subspaces) must be an unsigned integer");
        params.m = m_literal->value.safeGet<UInt64>();
    }

    /// The trained 'product' (Product Quantization) method takes (dimensions, nbits, m); the data-independent methods
    /// take (dimensions[, bits]).
    if (params.method == "product")
    {
        if (arguments->children.size() != 4)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec Quantized method 'product' requires Quantized('product', dimensions, nbits, m)");
        if (const auto error = ProductQuantizer::validateParams(params.dimensions, params.m, params.bits); !error.empty())
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec Quantized: {}", error);
    }
    else
    {
        if (arguments->children.size() == 4)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec Quantized: the 4th parameter (m) is only valid for the 'product' method");
        if (const std::string error = VectorQuantizer::validateParams(params.method, params.dimensions, params.bits); !error.empty())
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec Quantized: {}", error);
    }

    return params;
}

}

std::optional<QuantizedCodecParams> tryExtractQuantizedCodecParams(const ASTPtr & codec_desc)
{
    if (!codec_desc)
        return {};

    const auto * func = codec_desc->as<ASTFunction>();
    if (!func || !func->arguments)
        return {};

    for (const auto & param_ast : func->arguments->children)
    {
        if (const auto * inner_func = param_ast->as<ASTFunction>())
        {
            if (inner_func->name == "Quantized")
                return parseQuantizeCodecArguments(inner_func->arguments);
        }
        else if (const auto * inner_identifier = param_ast->as<ASTIdentifier>())
        {
            if (inner_identifier->name() == "Quantized")
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                    "Codec Quantized requires parameters: Quantized(method, dimensions[, bits])");
        }
    }

    return {};
}

void registerCodecQuantized(CompressionCodecFactory & factory)
{
    UInt8 method_code = static_cast<UInt8>(CompressionMethodByte::Quantized);
    factory.registerCompressionCodec("Quantized", method_code, [](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        /// On the read path the codec is instantiated from its method byte alone (with no arguments) just to
        /// memcpy-decompress the verbatim full-precision stream; the parameters are not needed there. The semantic
        /// validation of the parameters happens when the codec is attached to a column (tryExtractQuantizedCodecParams).
        if (!arguments)
            return std::make_shared<CompressionCodecQuantized>(QuantizedCodecParams{});
        return std::make_shared<CompressionCodecQuantized>(parseQuantizeCodecArguments(arguments));
    });
}

}
