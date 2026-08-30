#include <Compression/CompressionCodecAdaptive.h>

#include <algorithm>
#include <array>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>
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

/// Candidate codecs for the adaptive pool, grouped by codec expression.
/// TODO: extend candidates as codecs as we see some proof they are faster than the default and can compress better.
/// TODO: play around with chains to see if they are worth it (could be too slow). Until then, they are banned.
constexpr std::array<CandidateGroup, 1> CANDIDATES = {{
    {"T64", T64_TYPES}, /// T64 defaults to the byte flavour (over bit). Good: same size + faster [de]compression.
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

/// Hands out destinations for candidate compressions over two buffers: the external one and a lazily allocated scratch.
/// The buffer holding the current best is pinned: `takeWriteDestination` never hands it out.
class CompressionDestinationMultiplexer
{
public:
    CompressionDestinationMultiplexer(char * external_destination_, UInt32 internal_reserve_)
        : external_destination(external_destination_)
        , internal_reserve(internal_reserve_)
    {
    }

    char * takeWriteDestination()
    {
        if (best_destination != external_destination)
            return external_destination;

        scratch.resize_exact(internal_reserve);
        return scratch.data();
    }

    void recordCompression(char * to) { best_destination = to; }
    void discardRecordedCompression() { best_destination = nullptr; }

    /// nullptr: the best was measured only, never materialized.
    char * getBestDestination() const { return best_destination; }

private:
    char * external_destination;
    char * best_destination = nullptr;
    UInt32 internal_reserve;
    PODArray<char> scratch;
};

}

Codecs AdaptiveCodec::poolForType(const IDataType & type, const CompressionCodecPtr & deployment_default)
{
    /// An encrypting default must not reach here as substituting a codec would drop the encryption. Must handle this in the caller.
    if (deployment_default->isEncryption())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Adaptive codec pool must not be built from an encrypting default");

    static const CompressionCodecPtr none_codec = CompressionCodecFactory::instance().get("NONE", {});
    Codecs pool{none_codec, deployment_default};
    const TypeIndex type_id = type.getTypeId();
    for (const auto & [codec_expr, types] : CANDIDATES)
        if (std::ranges::find(types, type_id) != types.end())
            pool.push_back(buildCodecForType(codec_expr, type));
    return pool;
}

CompressionCodecAdaptive::CompressionCodecAdaptive(const IDataType & type, const CompressionCodecPtr & deployment_default)
    : pool(AdaptiveCodec::poolForType(type, deployment_default))
{
    chassert(!pool.empty());
    setCodecDescription("Adaptive");
}

UInt32 CompressionCodecAdaptive::compress(const char * source, UInt32 source_size, char * dest) const
{
    /// A single pass over the pool. A candidate that reports its compressed size cheaply is measured without compressing.
    /// After the pass the winner reaches `dest` in one of three ways: a measured-only winner is compressed into it,
    /// a winner already there needs nothing, and a winner in scratch is copied over.
    chassert(dest != nullptr);
    CompressionDestinationMultiplexer multiplexer(dest, getMaxCompressedDataSize(source_size));
    const ICompressionCodec * best_codec = nullptr;
    UInt32 best_size = std::numeric_limits<UInt32>::max();

    for (const auto & codec : pool)
    {
        if (auto calculated = codec->tryGetCompressedSize(source, source_size))
        {
            const UInt32 size = getHeaderSize() + *calculated;
            if (size < best_size)
            {
                best_size = size;
                best_codec = codec.get();
                multiplexer.discardRecordedCompression();
            }
        }
        else
        {
            char * target = multiplexer.takeWriteDestination();
            const UInt32 size = codec->compress(source, source_size, target);
            if (size < best_size)
            {
                best_size = size;
                best_codec = codec.get();
                multiplexer.recordCompression(target);
            }
        }
    }

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
    for (const auto & codec : pool)
        max_reserve = std::max(max_reserve, codec->getCompressedReserveSize(uncompressed_size));
    return max_reserve;
}

void CompressionCodecAdaptive::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash, /*ignore_aliases=*/true);
    for (const auto & codec : pool)
        codec->updateHash(hash);
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
