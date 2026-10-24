#include <lz4.h>
#include <lz4hc.h>

#include <Compression/ICompressionCodec.h>
#include <Compression/CompressionInfo.h>
#include <Compression/CompressionFactory.h>
#include <Compression/registerCompressionCodecs.h>
#include <Compression/LZ4_decompress_faster.h>
#include <Compression/LZ4StatisticsHint.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <Common/Exception.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

#include <algorithm>
#include <iterator>

#pragma clang diagnostic ignored "-Wold-style-cast"


namespace LZ4
{

UInt64 & currentStatisticsHint()
{
    thread_local UInt64 hint = 0;
    return hint;
}

}


namespace DB
{

class CompressionCodecLZ4 : public ICompressionCodec
{
public:
    CompressionCodecLZ4() = default;

    uint8_t getMethodByte() const override;
    ASTPtr getCodecDescription() const override;

    UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return LZ4::ADDITIONAL_BYTES_AT_END_OF_BUFFER; }

    void updateHash(SipHash & hash) const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    bool isCompression() const override { return true; }
    bool isGenericCompression() const override { return true; }
    String getDescription() const override { return "Extremely fast; good compression; balanced speed and efficiency."; }

private:
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    mutable LZ4::PerformanceStatistics lz4_stat;
    mutable UInt64 lz4_stat_hint = 0;
};


class CompressionCodecLZ4HC : public CompressionCodecLZ4
{
public:
    explicit CompressionCodecLZ4HC(int level_);
    ASTPtr getCodecDescription() const override;

protected:
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    std::string getDescription() const override
    {
        return "LZ4 High Compression algorithm with configurable level; slower but better compression than LZ4, but decompression is still fast.";
    }


private:
    const int level;
};


namespace ErrorCodes
{
    extern const int CANNOT_COMPRESS;
    extern const int CANNOT_DECOMPRESS;
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int ILLEGAL_CODEC_PARAMETER;
}

ASTPtr CompressionCodecLZ4::getCodecDescription() const
{
    return makeCodecDescription("LZ4");
}

uint8_t CompressionCodecLZ4::getMethodByte() const
{
    return static_cast<uint8_t>(CompressionMethodByte::LZ4);
}

void CompressionCodecLZ4::updateHash(SipHash & hash) const
{
    getCodecDescription()->updateTreeHash(hash, /*ignore_aliases=*/ true);
}

UInt32 CompressionCodecLZ4::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    return LZ4_COMPRESSBOUND(uncompressed_size);
}

UInt32 CompressionCodecLZ4::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    return LZ4_compress_default(source, dest, source_size, LZ4_COMPRESSBOUND(source_size));
}

namespace
{

constexpr size_t max_columns_with_statistics = 256;
/// Measurements per variant a new read stream inherits, so that it still adapts to its own data.
constexpr double inherited_measurements = 8;

void limitMeasurements(LZ4::PerformanceStatistics::Element & element, double limit)
{
    if (element.adjustedCount() > limit)
    {
        element.sum *= limit / element.adjustedCount();
        element.count = LZ4::PerformanceStatistics::NUM_INVOCATIONS_TO_THROW_OFF + limit;
    }
}

/// Adds to `to` what one block added to a read stream's element (`before` -> `after`). An invocation the stream threw off adds no
/// measurement, so `to.sum` stays the sum of `to.adjustedCount()` measurements.
void addBlock(LZ4::PerformanceStatistics::Element & to, const LZ4::PerformanceStatistics::Element & before, const LZ4::PerformanceStatistics::Element & after)
{
    constexpr double throw_off = LZ4::PerformanceStatistics::NUM_INVOCATIONS_TO_THROW_OFF;
    const double measurements = std::max(after.count, throw_off) - std::max(before.count, throw_off);
    const double thrown_off = after.count - before.count - measurements;
    to.count = std::min(to.count + thrown_off, std::max(to.count, throw_off));
    if (measurements > 0)
    {
        to.count = std::max(to.count, throw_off) + measurements;
        to.sum += after.sum - before.sum;
    }
}

LZ4::PerformanceStatistics & sharedStatistics(UInt64 hint)
{
    thread_local UnorderedMapWithMemoryTracking<UInt64, LZ4::PerformanceStatistics> statistics;
    if (auto it = statistics.find(hint); it != statistics.end())
        return it->second;
    if (statistics.size() >= max_columns_with_statistics)
        statistics.clear();
    return statistics[hint];
}

}

UInt32 CompressionCodecLZ4::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    using Stats = LZ4::PerformanceStatistics;
    Stats * shared = nullptr;
    Stats::Element before[Stats::NUM_ELEMENTS];

    /// Smaller blocks do not use statistics, so they neither seed nor create a map entry.
    UInt64 hint = uncompressed_size >= LZ4::MIN_BLOCK_SIZE_TO_CHOOSE_VARIANT ? LZ4::currentStatisticsHint() : 0;
    if (hint)
    {
        shared = &sharedStatistics(hint);
        if (hint != lz4_stat_hint)
        {
            lz4_stat_hint = hint;
            for (size_t i = 0; i < Stats::NUM_ELEMENTS; ++i)
            {
                lz4_stat.data[i] = shared->data[i];
                limitMeasurements(lz4_stat.data[i], inherited_measurements);
            }
        }
        std::copy(std::begin(lz4_stat.data), std::end(lz4_stat.data), before);
    }

    bool success = LZ4::decompress(source, dest, source_size, uncompressed_size, lz4_stat);

    if (!success)
        throw Exception(ErrorCodes::CANNOT_DECOMPRESS, "Cannot decompress LZ4-encoded data");

    if (shared)
    {
        for (size_t i = 0; i < Stats::NUM_ELEMENTS; ++i)
            addBlock(shared->data[i], before[i], lz4_stat.data[i]);
    }

    /// LZ4::decompress only returns true when the dest buffer has been fully written (uncompressed_size)
    return uncompressed_size;
}

void registerCodecLZ4(CompressionCodecFactory & factory)
{
    factory.registerSimpleCompressionCodec("LZ4", static_cast<UInt8>(CompressionMethodByte::LZ4), [&] ()
    {
        return std::make_shared<CompressionCodecLZ4>();
    });
}

UInt32 CompressionCodecLZ4HC::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    auto success = LZ4_compress_HC(source, dest, source_size, LZ4_COMPRESSBOUND(source_size), level);

    if (!success)
        throw Exception(ErrorCodes::CANNOT_COMPRESS, "Cannot compress with LZ4 codec");

    return success;
}

void registerCodecLZ4HC(CompressionCodecFactory & factory)
{
    factory.registerCompressionCodec("LZ4HC", {}, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        int level = 0;

        if (arguments && !arguments->children.empty())
        {
            if (arguments->children.size() > 1)
                throw Exception(ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE, "LZ4HC codec must have 1 parameter, given {}", arguments->children.size());

            const auto children = arguments->children;
            const auto * literal = children[0]->as<ASTLiteral>();
            if (!literal)
                throw Exception(ErrorCodes::ILLEGAL_CODEC_PARAMETER, "LZ4HC codec argument must be integer");

            level = static_cast<int>(literal->value.safeGet<UInt64>());
        }

        return std::make_shared<CompressionCodecLZ4HC>(level);
    });
}

CompressionCodecLZ4HC::CompressionCodecLZ4HC(int level_)
    : level(level_)
{
}

ASTPtr CompressionCodecLZ4HC::getCodecDescription() const
{
    return makeCodecDescription("LZ4HC", {make_intrusive<ASTLiteral>(static_cast<UInt64>(level))});
}


CompressionCodecPtr getCompressionCodecLZ4(int level)
{
    return std::make_shared<CompressionCodecLZ4HC>(level);
}

}
