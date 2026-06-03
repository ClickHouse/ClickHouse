#include <Compression/CompressionCodecAdaptive.h>

#include <Compression/CompressedSizeEstimator.h>
#include <Compression/CompressionFactory.h>
#include <Core/Defines.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>

#include <algorithm>
#include <array>
#include <span>
#include <string_view>


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{

struct CandidateGroup
{
    std::string_view codec_expr;
    std::span<const TypeIndex> types;
};

constexpr std::array T64_TYPES = {
    TypeIndex::Int8,   TypeIndex::Int16,     TypeIndex::Int32,     TypeIndex::Int64,      TypeIndex::UInt8,
    TypeIndex::UInt16, TypeIndex::UInt32,    TypeIndex::UInt64,    TypeIndex::Enum8,      TypeIndex::Enum16,
    TypeIndex::Date,   TypeIndex::Date32,    TypeIndex::DateTime,  TypeIndex::DateTime64, TypeIndex::Time,
    TypeIndex::Time64, TypeIndex::Decimal32, TypeIndex::Decimal64, TypeIndex::IPv4,
};

/// Candidate codecs for the adaptive pool, grouped by codec expression.
/// TODO: extend candidates as codecs as we see some proof they are faster than the default and can compress better.
/// TODO: play around with chains to see if they are worth it (could be too slow). Until then, they are banned.
/// TODO: pin the optimal T64 flavour.
constexpr std::array<CandidateGroup, 1> CANDIDATES = {{
    {"T64", T64_TYPES},
}};

/// Build the codec described by `expr` for `type` so type-aware codecs get the type they need.
/// E.g. T64 derives its type_idx from it, to compress and to predict its size.
CompressionCodecPtr buildCodecForType(std::string_view expr, const IDataType & type)
{
    ParserCodec parser;
    const String query = "(" + String(expr) + ")";
    ASTPtr ast = parseQuery(parser, query, /*max_query_size=*/0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS);
    return CompressionCodecFactory::instance().get(ast, &type);
}

[[noreturn]] void throwMustNotBeInvokedDirectly()
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "CompressionCodecAdaptive must not be invoked directly: it never appears on disk");
}

}

std::vector<CompressionCodecPtr> AdaptiveCodec::poolForType(const IDataType & type, const CompressionCodecPtr & deployment_default)
{
    /// TODO: add NONE to the pool.
    std::vector<CompressionCodecPtr> pool{deployment_default};
    const TypeIndex type_id = type.getTypeId();
    for (const auto & [codec_expr, types] : CANDIDATES)
        if (std::ranges::find(types, type_id) != types.end())
            pool.push_back(buildCodecForType(codec_expr, type));
    return pool;
}

std::vector<TypeIndex> AdaptiveCodec::candidateTypeIndexes()
{
    std::vector<TypeIndex> result;
    for (const auto & group : CANDIDATES)
        for (const TypeIndex type_id : group.types)
            if (std::ranges::find(result, type_id) == result.end()) /// distinct: a type may appear in more than one group
                result.push_back(type_id);
    return result;
}

CompressionCodecPtr AdaptiveCodec::select(const std::vector<CompressionCodecPtr> & pool, const char * source, UInt32 source_size)
{
    chassert(!pool.empty());

    PODArray<char> scratch;
    size_t best_idx = 0;
    UInt32 best_size = CompressedSizeEstimator::getCompressedBlockSize(*pool[0], source, source_size, scratch);

    for (size_t i = 1; i < pool.size(); ++i)
    {
        const UInt32 size = CompressedSizeEstimator::getCompressedBlockSize(*pool[i], source, source_size, scratch);
        const bool is_smaller = size < best_size;
        best_idx = is_smaller ? i : best_idx;
        best_size = is_smaller ? size : best_size;
    }

    return pool[best_idx];
}

CompressionCodecAdaptive::CompressionCodecAdaptive(
    const IDataType & type, const CompressionCodecPtr & deployment_default, UInt32 skip_threshold_)
    : pool(AdaptiveCodec::poolForType(type, deployment_default))
    , skip_threshold(skip_threshold_)
{
    chassert(!pool.empty());
    setCodecDescription("Adaptive");
}

UInt32 CompressionCodecAdaptive::compress(const char * source, UInt32 source_size, char * dest) const
{
    /// Below the threshold, selection is not worth its cost → write the deployment default.
    /// TODO: at tiny blocks, maybe we don't want to compress at all? We're not saving much space, but adding some cost.
    /// Also, selection might be worth it as smaller block is faster to select from.
    CompressionCodecPtr winner = (source_size < skip_threshold) ? pool.front() : AdaptiveCodec::select(pool, source, source_size);
    return winner->compress(source, source_size, dest);
}

UInt32 CompressionCodecAdaptive::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 max_reserve = 0;
    for (const auto & codec : pool)
        max_reserve = std::max(max_reserve, codec->getCompressedReserveSize(uncompressed_size));
    return max_reserve;
}

void CompressionCodecAdaptive::updateHash(SipHash & hash) const
{
    for (const auto & codec : pool)
        codec->updateHash(hash);
    hash.update(skip_threshold);
}

uint8_t CompressionCodecAdaptive::getMethodByte() const
{
    throwMustNotBeInvokedDirectly();
}

UInt32 CompressionCodecAdaptive::doCompressData(const char *, UInt32, char *) const
{
    throwMustNotBeInvokedDirectly();
}

UInt32 CompressionCodecAdaptive::doDecompressData(const char *, UInt32, char *, UInt32) const
{
    throwMustNotBeInvokedDirectly();
}

}
