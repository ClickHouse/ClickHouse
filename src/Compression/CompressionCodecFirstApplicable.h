#pragma once

#include <Compression/ICompressionCodec.h>


namespace DB
{

class IDataType;

/// The name of the codec combinator `FirstApplicable(c1, c2, ...)`: the first of the alternatives that is applicable
/// to the data type of the (sub)column. It is not a codec family: it only selects one, and has no method byte and no
/// header of its own. An alternative is a codec, `Default`, a nested `FirstApplicable`, or `Multiple(c1, c2, ...)`,
/// which is a chain of codecs, applicable if every codec of it is. Without a data type, only the codecs that apply to
/// any data type are applicable.
static constexpr auto FIRST_APPLICABLE_CODEC_NAME = "FirstApplicable";

/// The selection of `FirstApplicable`, and the codec it makes when the data type is not known yet.
///
/// That codec is made e.g. for the `default_compression_codec` setting. It compresses with the first alternative
/// that is applicable to any data type, but keeps the description of the whole combinator, so that a caller that
/// knows the data type can resolve it again from `getCodecDescription` (as `MergeTreeDataPartWriterOnDisk::getSubstreamCodec`
/// does with the default codec of a part, and `CompressionCodecFactory::get` with `Default`, see `dependsOnDataType`).
/// It never appears on disk: every block carries the method byte of the selected codec.
class CompressionCodecFirstApplicable final : public ICompressionCodec
{
public:
    /// The codec for the chain element `element` (the combinator with its alternatives) and the data type `column_type`.
    /// With a data type: the codec, or the chain of codecs, of the selected alternative.
    /// Without one (nullptr): a `CompressionCodecFirstApplicable` that keeps the description of the combinator.
    /// `Default` stands for `current_default`.
    /// If `only_generic` (a structural substream, e.g. the sizes of an array), the selected codecs are returned without
    /// the wrapper, for the caller to keep only the generic ones of them; nullptr if no alternative is applicable without
    /// a data type, as there is no generic codec to keep, the same as for a specialized codec.
    static CompressionCodecPtr create(
        const ASTPtr & element, const IDataType * column_type, const CompressionCodecPtr & current_default, bool only_generic);

    /// True if the chain element `element` is the combinator.
    static bool isFirstApplicable(const ASTPtr & element);

    /// True if the codec chain `codec` (a `CODEC(...)` function) has the combinator, so that the codec depends on the
    /// data type of every substream.
    static bool isInChain(const ASTPtr & codec);

    /// The codec chain `ast` (a `CODEC(...)` function) with every combinator replaced by the codecs of the alternative
    /// selected for `type` (nullptr: unknown). Returns `ast` itself if it has no combinator.
    static ASTPtr resolveInChain(const ASTPtr & ast, const IDataType * type);

    [[nodiscard]] uint8_t getMethodByte() const override { return selected->getMethodByte(); }
    [[nodiscard]] ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;

    UInt32 compress(const char * source, UInt32 source_size, char * dest, CompressionState * state) const override
    {
        return selected->compress(source, source_size, dest, state);
    }
    [[nodiscard]] CompressionStatePtr createCompressionState() const override { return selected->createCompressionState(); }
    std::optional<UInt32> tryGetCompressedSize(const char * source, UInt32 source_size) const override
    {
        return selected->tryGetCompressedSize(source, source_size);
    }
    [[nodiscard]] UInt32 getCompressedReserveSize(UInt32 uncompressed_size) const override { return selected->getCompressedReserveSize(uncompressed_size); }
    [[nodiscard]] UInt32 getAdditionalSizeAtTheEndOfBuffer() const override { return selected->getAdditionalSizeAtTheEndOfBuffer(); }

    [[nodiscard]] bool isCompression() const override { return selected->isCompression(); }
    [[nodiscard]] bool isGenericCompression() const override { return selected->isGenericCompression(); }
    [[nodiscard]] bool isEncryption() const override { return selected->isEncryption(); }
    [[nodiscard]] bool isFloatingPointTimeSeriesCodec() const override { return selected->isFloatingPointTimeSeriesCodec(); }
    [[nodiscard]] bool isDeltaCompression() const override { return selected->isDeltaCompression(); }
    [[nodiscard]] bool isLossyCompression() const override { return selected->isLossyCompression(); }
    [[nodiscard]] bool requiresColumnTypeToCompress() const override { return selected->requiresColumnTypeToCompress(); }
    [[nodiscard]] bool isNone() const override { return selected->isNone(); }
    [[nodiscard]] bool dependsOnDataType() const override { return true; }

    [[nodiscard]] String getDescription() const override
    {
        return "Selects the first of the codecs given as arguments that is applicable to the data type of the column.";
    }

protected:
    [[nodiscard]] UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override { return selected->getMaxCompressedDataSize(uncompressed_size); }
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override
    {
        return selected->doCompressData(source, source_size, dest);
    }
    UInt32 doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override
    {
        return selected->doDecompressData(source, source_size, dest, uncompressed_size);
    }

private:
    CompressionCodecFirstApplicable(ASTPtr description_, CompressionCodecPtr selected_);

    /// `FirstApplicable(...)` with the alternatives as written, except that `Default` is replaced by the default codec.
    ASTPtr description;
    CompressionCodecPtr selected;
};

}
