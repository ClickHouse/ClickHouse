#pragma once

#include <vector>
#include <stdexcept>

/**
  * An encoded DDSketch comprises multiple contiguous blocks (sequences of bytes).
  * Each block is prefixed with a flag that indicates what the block contains and how the data is encoded in the block.
  * A flag is a single byte, which itself contains two parts:
  * - the flag type (the 2 least significant bits),
  * - the subflag (the 6 most significant bits).
  *
  * There are four flag types, for:
  * - sketch features,
  * - index mapping,
  * - positive value store,
  * - negative value store.
  *
  * The meaning of the subflag depends on the flag type:
  * - for the sketch feature flag type, it indicates what feature is encoded,
  * - for the index mapping flag type, it indicates what mapping is encoded and how,
  * - for the store flag types, it indicates how bins are encoded.
  */
namespace DB
{
class DDSketchEncoding
{
private:
    static constexpr UInt8 numBitsForType = 2;
    static constexpr UInt8 flagTypeMask = (1 << numBitsForType) - 1;
    static constexpr UInt8 subFlagMask = ~flagTypeMask;
    static constexpr UInt8 flagTypeSketchFeatures = 0b00;

public:
    class Flag
    {
    public:
        UInt8 byte;
        Flag(UInt8 t, UInt8 s) : byte(t | s) { }
        [[maybe_unused]] UInt8 Type() const { return byte & flagTypeMask; }
        [[maybe_unused]] UInt8 SubFlag() const { return byte & subFlagMask; }
    };

    // FLAG TYPES
    static constexpr UInt8 FlagTypeIndexMapping = 0b10;
    static constexpr UInt8 FlagTypePositiveStore = 0b01;
    static constexpr UInt8 FlagTypeNegativeStore = 0b11;

    // SKETCH FEATURES

    // Encoding format:
    // - [byte] flag
    // - [varfloat64] count of the zero bin
    const Flag FlagZeroCountVarFloat = Flag(flagTypeSketchFeatures, 1 << numBitsForType);

    // INDEX MAPPING
    // Encoding format:
    // - [byte] flag
    // - [float64LE] gamma
    // - [float64LE] index offset
    const Flag FlagIndexMappingBaseLogarithmic = Flag(FlagTypeIndexMapping, 0 << numBitsForType);

    // BINS
    // Encoding format:
    // - [byte] flag
    // - [uvarint64] number of bins N
    // - [varint64] index of first bin
    // - [varfloat64] count of first bin
    // - [varint64] difference between the index of the second bin and the index
    // of the first bin
    // - [varfloat64] count of second bin
    // - ...
    // - [varint64] difference between the index of the N-th bin and the index
    // of the (N-1)-th bin
    // - [varfloat64] count of N-th bin
    static constexpr UInt8 BinEncodingIndexDeltasAndCounts = 1 << numBitsForType;

    // Encoding format:
    // - [byte] flag
    // - [uvarint64] number of bins N
    // - [varint64] index of first bin
    // - [varint64] difference between the index of the second bin and the index
    // of the first bin
    // - ...
    // - [varint64] difference between the index of the N-th bin and the index
    // of the (N-1)-th bin
    static constexpr UInt8 BinEncodingIndexDeltas = 2 << numBitsForType;

    // Encoding format:
    // - [byte] flag
    // - [uvarint64] number of bins N
    // - [varint64] index of first bin
    // - [varint64] difference between two successive indexes
    // - [varfloat64] count of first bin
    // - [varfloat64] count of second bin
    // - ...
    // - [varfloat64] count of N-th bin
    static constexpr UInt8 BinEncodingContiguousCounts = 3 << numBitsForType;
};

}
