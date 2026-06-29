#include <Compression/CompressionCodecQuantize.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Common/ProductQuantization.h>
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
    if (params.method == "pq")
        args.emplace_back(make_intrusive<ASTLiteral>(static_cast<UInt64>(params.m)));
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
    if (!arguments || arguments->children.size() < 2 || arguments->children.size() > 4)
        throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
            "Codec Quantize requires 2 to 4 parameters: Quantize(method, dimensions[, bits[, m]]) "
            "(the trained 'pq' method uses Quantize('pq', dimensions, nbits, m))");

    const auto * method_literal = arguments->children[0]->as<ASTLiteral>();
    if (!method_literal || method_literal->value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "First argument of codec Quantize (method) must be a string literal");

    const auto * dimensions_literal = arguments->children[1]->as<ASTLiteral>();
    if (!dimensions_literal || dimensions_literal->value.getType() != Field::Types::UInt64)
        throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Second argument of codec Quantize (dimensions) must be an unsigned integer");

    QuantizeCodecParams params;
    params.method = method_literal->value.safeGet<String>();
    params.dimensions = dimensions_literal->value.safeGet<UInt64>();

    /// Sugar for the Matryoshka prefix method: Quantize('mrl', dimensions, leading_dimensions, 'int8'|'bf16'). It stores
    /// only the leading `leading_dimensions` of the vector, quantized to int8 (per-vector scale) or bfloat16. Fold the
    /// format into the canonical method name ('mrl_int8'/'mrl_bf16') and put the prefix length in the `bits` slot, so the
    /// rest of the pipeline (serialization, planner, distance) uses it like any other data-independent method.
    if (params.method == "mrl")
    {
        if (arguments->children.size() != 4)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec Quantize method 'mrl' requires Quantize('mrl', dimensions, leading_dimensions, format) "
                "where format is 'int8' or 'bf16'");

        const auto * prefix_literal = arguments->children[2]->as<ASTLiteral>();
        if (!prefix_literal || prefix_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                "Third argument of codec Quantize('mrl', ...) (number of leading dimensions) must be an unsigned integer");
        params.bits = prefix_literal->value.safeGet<UInt64>();

        const auto * format_literal = arguments->children[3]->as<ASTLiteral>();
        if (!format_literal || format_literal->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                "Fourth argument of codec Quantize('mrl', ...) (format) must be a string literal: 'int8' or 'bf16'");
        const String format = format_literal->value.safeGet<String>();
        if (format != "int8" && format != "bf16")
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER,
                "Codec Quantize('mrl', ...): format must be 'int8' or 'bf16', got '{}'", format);
        params.method = "mrl_" + format;

        if (const std::string error = VectorQuantization::validateParams(params.method, params.dimensions, params.bits); !error.empty())
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec Quantize: {}", error);
        return params;
    }

    if (arguments->children.size() >= 3)
    {
        const auto * bits_literal = arguments->children[2]->as<ASTLiteral>();
        if (!bits_literal || bits_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Third argument of codec Quantize (bits) must be an unsigned integer");
        params.bits = bits_literal->value.safeGet<UInt64>();
    }
    if (arguments->children.size() >= 4)
    {
        const auto * m_literal = arguments->children[3]->as<ASTLiteral>();
        if (!m_literal || m_literal->value.getType() != Field::Types::UInt64)
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Fourth argument of codec Quantize (m, number of subspaces) must be an unsigned integer");
        params.m = m_literal->value.safeGet<UInt64>();
    }

    /// The trained 'pq' (Product Quantization) method takes (dimensions, nbits, m); the data-independent methods take
    /// (dimensions[, bits]).
    if (params.method == "pq")
    {
        if (arguments->children.size() != 4)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec Quantize method 'pq' requires Quantize('pq', dimensions, nbits, m)");
        if (const std::string error = ProductQuantization::validateParams(params.dimensions, params.m, params.bits); !error.empty())
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec Quantize: {}", error);
    }
    else
    {
        if (arguments->children.size() == 4)
            throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE,
                "Codec Quantize: the 4th parameter (m) is only valid for the 'pq' method");
        if (const std::string error = VectorQuantization::validateParams(params.method, params.dimensions, params.bits); !error.empty())
            throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "Codec Quantize: {}", error);
    }

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
