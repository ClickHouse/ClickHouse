#pragma once

#include <Compression/ICompressionCodec.h>


namespace DB
{

/// The name of the codec combinator `FirstApplicable(c1, c2, ...)`: the first of the alternatives that is applicable
/// to the data type of the (sub)column. It is not a codec family: it only selects one, and has no method byte and no
/// header of its own. `CompressionCodecFactory` resolves it whenever the data type is known, so the selected codec is
/// used directly.
static constexpr auto FIRST_APPLICABLE_CODEC_NAME = "FirstApplicable";

/// `FirstApplicable` resolved without a data type, e.g. given in the `default_compression_codec` setting.
/// It compresses with the first alternative that is applicable to any data type, but keeps the description of the
/// whole combinator, so that a caller that knows the data type can resolve it again from `getCodecDescription`
/// (as `MergeTreeDataPartWriterOnDisk::getSubstreamCodec` does with the default codec of a part).
/// The wrapper never appears on disk: every block carries the method byte of the selected codec.
class CompressionCodecFirstApplicable final : public ICompressionCodec
{
public:
    CompressionCodecFirstApplicable(ASTPtr description_, CompressionCodecPtr selected_);

    [[nodiscard]] const CompressionCodecPtr & getSelected() const { return selected; }

    [[nodiscard]] uint8_t getMethodByte() const override { return selected->getMethodByte(); }
    [[nodiscard]] ASTPtr getCodecDescription() const override;
    void updateHash(SipHash & hash) const override;

    UInt32 compress(const char * source, UInt32 source_size, char * dest) const override { return selected->compress(source, source_size, dest); }
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
    /// `FirstApplicable(...)` with the alternatives as written, except that `Default` is replaced by the default codec.
    ASTPtr description;
    CompressionCodecPtr selected;
};

}
