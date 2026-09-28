#include <Compression/CompressionCodecAdaptive.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionFactory.h>
#include <Core/Defines.h>
#include <Core/TypeId.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/SipHash.h>


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

constexpr std::array ALP_TYPES = {TypeIndex::Float32, TypeIndex::Float64};

/// Candidate codecs for the adaptive pool. Each one is also tried followed by the deployment default if it's a general-purpose compressor.
constexpr std::array<CandidateGroup, 3> CANDIDATES = {{
    /// T64 defaults to the byte flavour (over bit). Good: same size + faster [de]compression.
    {"T64", T64_TYPES},
    /// Do not use AUTO as it picks STD or RD per block from a sample. With sampled adaptive compression, that is sample of a sample.
    /// STD before RD because STD decompressed faster (we want it in case of tie).
    {"ALP(STD)", ALP_TYPES},
    {"ALP(RD)", ALP_TYPES},
}};

/// Build the codec described by `expr` for `type` so type-aware codecs get the type they need.
/// E.g. T64 derives its type_idx from it, to compress and to calculate its size.
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

/// Hands out destinations for candidate compressions: the external buffer if it is free, else one of two scratches allocated on first use.
/// Not handed out: `best_destination` and `in_use`.
class CompressionDestinationMultiplexer
{
public:
    CompressionDestinationMultiplexer(char * external_destination_, UInt32 internal_reserve_)
        : external_destination(external_destination_)
        , internal_reserve(internal_reserve_)
    {
    }

    char * takeWriteDestination(const char * in_use = nullptr)
    {
        if (isFree(external_destination, in_use))
            return external_destination;
        if (isFree(allocated(first_scratch, internal_reserve), in_use))
            return first_scratch.data();
        return allocated(second_scratch, internal_reserve);
    }

    void setBestDestination(char * to) { best_destination = to; }
    char * getBestDestination() const { return best_destination; }

private:
    bool isFree(const char * buffer, const char * in_use) const { return buffer != best_destination && buffer != in_use; }

    static char * allocated(PODArray<char> & scratch, UInt32 size)
    {
        if (scratch.empty())
            scratch.resize_exact(size);
        return scratch.data();
    }

    char * external_destination;
    char * best_destination = nullptr;
    UInt32 internal_reserve;
    PODArray<char> first_scratch;
    PODArray<char> second_scratch;
};

}

AdaptiveCodec::Candidates AdaptiveCodec::poolForType(const DataTypePtr & type, const CompressionCodecPtr & deployment_default)
{
    /// An encrypting default must not reach here as substituting a codec would drop the encryption. Must handle this in the caller.
    if (deployment_default->isEncryption())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive codec pool must not be built from an encrypting default");

    static const CompressionCodecPtr none_codec = CompressionCodecFactory::instance().get("NONE", {});
    Candidates pool{{none_codec}, {deployment_default}};
    if (!type)
        return pool;

    const bool chain_with_default = deployment_default->isGenericCompression();
    const TypeIndex type_id = type->getTypeId();
    for (const auto & [codec_expr, types] : CANDIDATES)
    {
        if (std::ranges::find(types, type_id) == types.end())
            continue;

        Candidate candidate{buildCodecForType(codec_expr, *type)};
        if (chain_with_default)
            candidate.chain = std::make_shared<CompressionCodecMultiple>(Codecs{candidate.codec, deployment_default});
        pool.push_back(std::move(candidate));
    }
    return pool;
}

CompressionCodecAdaptive::CompressionCodecAdaptive(const DataTypePtr & type, const CompressionCodecPtr & deployment_default)
    : pool(AdaptiveCodec::poolForType(type, deployment_default))
{
    chassert(!pool.empty());
}

ASTPtr CompressionCodecAdaptive::getCodecDescription() const
{
    return makeCodecDescription("Adaptive");
}

UInt32 CompressionCodecAdaptive::compress(const char * source, UInt32 source_size, char * dest) const
{
    chassert(dest != nullptr);
    CompressionDestinationMultiplexer multiplexer(dest, getMaxCompressedDataSize(source_size));
    const ICompressionCodec * best_codec = nullptr;
    UInt32 best_size = std::numeric_limits<UInt32>::max();

    /// `block` is nullptr for a measured-only size.
    auto update_best = [&](const ICompressionCodec & codec, UInt32 size, char * block)
    {
        if (size >= best_size)
            return;
        best_size = size;
        best_codec = &codec;
        multiplexer.setBestDestination(block);
    };

    /// Try every candidate in the pool and choose best.
    for (const auto & [codec, chain] : pool)
    {
        /// A candidate without a chain that reports its size cheaply is measured rather than compressed.
        if (auto calculated = chain ? std::nullopt : codec->tryGetCompressedSize(source, source_size))
        {
            update_best(*codec, getHeaderSize() + *calculated, nullptr);
            continue;
        }

        char * block = multiplexer.takeWriteDestination();
        const UInt32 size = codec->compress(source, source_size, block);
        update_best(*codec, size, block);

        /// A chain applies only its second codec, to the block its first one (also a candidate) just produced.
        if (chain)
        {
            char * chained = multiplexer.takeWriteDestination(/*in_use=*/block);
            update_best(*chain, chain->compressRemainingStages(/*completed_stages=*/1, block, size, source_size, chained), chained);
        }
    }

    /// The winner reaches `dest` in one of three ways: a measured-only winner is compressed into it,
    /// a winner already there needs nothing, and a winner in scratch is copied over.
    char * best_compressed = multiplexer.getBestDestination();

    if (!best_compressed)
    {
        chassert(best_codec);
        const UInt32 size = best_codec->compress(source, source_size, dest);
        chassert(size == best_size);
        return size;
    }

    if (best_compressed != dest)
        memcpy(dest, best_compressed, best_size);

    return best_size;
}

UInt32 CompressionCodecAdaptive::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 max_reserve = 0;
    for (const auto & [codec, chain] : pool)
    {
        max_reserve = std::max(max_reserve, codec->getCompressedReserveSize(uncompressed_size));
        if (chain)
            max_reserve = std::max(max_reserve, chain->getCompressedReserveSize(uncompressed_size));
    }
    return max_reserve;
}

void CompressionCodecAdaptive::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/true);
    for (const auto & candidate : pool)
        candidate.codec->updateHash(hash);
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
