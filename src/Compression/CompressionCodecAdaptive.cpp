#include <Compression/CompressionCodecAdaptive.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <span>
#include <string_view>
#include <Compression/CodecSelectorSampled.h>
#include <Compression/CompressionCodecMultiple.h>
#include <Compression/CompressionCodecNone.h>
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

struct SpecializedCodec
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

/// Codecs specialized for particular types. Each one is also tried followed by the deployment default if it's a general-purpose compressor.
constexpr auto SPECIALIZED_CODECS = std::to_array<SpecializedCodec>({
    /// T64 defaults to the byte flavour (over bit). Good: same size + faster [de]compression.
    {"T64", T64_TYPES},
    {"ALP(AUTO)", ALP_TYPES},
});

/// Every codec and chain the pool offers, in pool order
Codecs candidatesOf(const AdaptiveCodec::Pool & pool)
{
    Codecs all;
    for (const auto & [codec, chain] : pool)
    {
        all.push_back(codec);
        if (chain)
            all.push_back(chain);
    }
    return all;
}

/// The default and every specialized codec with its chain must fit the selector
static_assert(2 * (1 + SPECIALIZED_CODECS.size()) <= AdaptiveCodec::CodecSelectorSampled::MAX_CANDIDATES);

/// Build the codec described by `expr` for `type` so type-aware codecs get the type they need.
/// E.g. `T64` derives its `type_idx` from it to compress.
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

const ICompressionCodec & getNoneCodec()
{
    static const CompressionCodecNone none;
    return none;
}

/// Hands out destinations for candidate compressions: the external buffer if it is free, else one of two scratches allocated on first use.
/// Not handed out: the best block's buffer and `in_use`.
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

    void setBest(char * block, UInt32 size)
    {
        best_destination = block;
        best_size = size;
    }
    char * getBestDestination() const { return best_destination; }
    UInt32 getBestSize() const { return best_size; }

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
    UInt32 best_size = 0;
    UInt32 internal_reserve;
    PODArray<char> first_scratch;
    PODArray<char> second_scratch;
};

}

AdaptiveCodec::Pool AdaptiveCodec::poolForType(const DataTypePtr & type, const CompressionCodecPtr & deployment_default)
{
    /// An encrypting default must not reach here as substituting a codec would drop the encryption. Must handle this in the caller.
    if (deployment_default->isEncryption())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive codec pool must not be built from an encrypting default");

    Pool pool{Entry{deployment_default}};
    if (!type)
        return pool;

    const bool chain_with_default = deployment_default->isGenericCompression();
    const TypeIndex type_id = type->getTypeId();
    for (const auto & [codec_expr, types] : SPECIALIZED_CODECS)
    {
        if (std::ranges::find(types, type_id) == types.end())
            continue;

        Entry entry{buildCodecForType(codec_expr, *type)};
        if (chain_with_default)
            entry.chain = std::make_shared<CompressionCodecMultiple>(Codecs{entry.codec, deployment_default});
        pool.push_back(std::move(entry));
    }
    return pool;
}

CompressionCodecAdaptive::CompressionCodecAdaptive(
    const DataTypePtr & type, const CompressionCodecPtr & deployment_default, UInt64 selection_period_)
    : pool(AdaptiveCodec::poolForType(type, deployment_default))
    , selection_period(selection_period_)
{
    chassert(!pool.empty());
}

ASTPtr CompressionCodecAdaptive::getCodecDescription() const { return makeCodecDescription("Adaptive"); }

CompressionStatePtr CompressionCodecAdaptive::createCompressionState() const
{
    return std::make_unique<AdaptiveCodec::CodecSelectorSampled>(candidatesOf(pool), selection_period);
}

UInt32 CompressionCodecAdaptive::compress(const char * source, UInt32 source_size, char * dest, CompressionState * state) const
{
    chassert(dest != nullptr);
    chassert(dynamic_cast<AdaptiveCodec::ICodecSelector *>(state) != nullptr);
    auto & selector = static_cast<AdaptiveCodec::ICodecSelector &>(*state);
    CompressionDestinationMultiplexer multiplexer(dest, getMaxCompressedDataSize(source_size));

    /// The selector ranks the run. The smallest block so far keeps its buffer.
    auto record = [&](const CompressionCodecPtr & codec, UInt32 size, char * block)
    {
        selector.record(codec, size);
        if (selector.codecToUse() == codec)
            multiplexer.setBest(block, size);
    };

    selector.beginBlock(source_size);
    for (auto codecs = selector.codecsToTry(); !codecs.empty(); codecs = selector.codecsToTry())
    {
        for (const auto & [codec, chain] : pool)
        {
            const bool with_codec = std::ranges::contains(codecs, codec);
            const bool with_chain = chain && std::ranges::contains(codecs, chain);
            if (!with_codec && !with_chain)
                continue;

            /// A chain applies the default to its codec's block. That block is in hand when the codec is the smallest so far, else the codec runs.
            char * block = multiplexer.getBestDestination();
            UInt32 size = multiplexer.getBestSize();
            if (with_codec || selector.codecToUse() != codec)
            {
                block = multiplexer.takeWriteDestination();
                size = codec->compress(source, source_size, block);
                record(codec, size, block);
            }

            if (with_chain)
            {
                char * chained = multiplexer.takeWriteDestination(block);
                record(chain, chain->compressRemainingStages(/*completed_stages=*/1, block, size, source_size, chained), chained);
            }
        }
    }

    /// Nothing beat the raw block: `NONE` writes it. Else the winner is already in `dest` or is copied over from scratch.
    char * best = multiplexer.getBestDestination();
    if (!best)
        return getNoneCodec().compress(source, source_size, dest);

    if (best != dest)
        memcpy(dest, best, multiplexer.getBestSize());
    return multiplexer.getBestSize();
}

UInt32 CompressionCodecAdaptive::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    UInt32 max_reserve = getNoneCodec().getCompressedReserveSize(uncompressed_size);
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
    for (const auto & entry : pool)
        entry.codec->updateHash(hash);
}

uint8_t CompressionCodecAdaptive::getMethodByte() const { throwMustNotBeInvokedDirectly(); }
UInt32 CompressionCodecAdaptive::doCompressData(const char *, UInt32, char *) const { throwMustNotBeInvokedDirectly(); }
UInt32 CompressionCodecAdaptive::doDecompressData(const char *, UInt32, char *, UInt32) const { throwMustNotBeInvokedDirectly(); }

}
