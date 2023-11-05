#pragma once

#include <vector>
#include <stdexcept>

namespace DB
{
class DDSketchEncoding
{
public:
    static constexpr UInt8 numBitsForType = 2;
    static constexpr UInt8 flagTypeMask = (1 << numBitsForType) - 1;
    static constexpr UInt8 subFlagMask = ~flagTypeMask;

    class Flag
    {
    public:
        UInt8 byte;
        Flag(UInt8 t, UInt8 s) : byte(t | s) { }
        UInt8 Type() const { return byte & flagTypeMask; }
        UInt8 SubFlag() const { return byte & subFlagMask; }
    };

    // FLAG TYPES
    static constexpr UInt8 flagTypeSketchFeatures = 0b00;
    static constexpr UInt8 FlagTypeIndexMapping = 0b10;
    static constexpr UInt8 FlagTypePositiveStore = 0b01;
    static constexpr UInt8 FlagTypeNegativeStore = 0b11;

    // SKETCH FEATURES
    const Flag FlagZeroCountVarFloat = Flag(flagTypeSketchFeatures, 1 << numBitsForType);

    // INDEX MAPPING
    const Flag FlagIndexMappingBaseLogarithmic = Flag(FlagTypeIndexMapping, 0 << numBitsForType);

    // BINS
    static constexpr UInt8 BinEncodingIndexDeltasAndCounts = 1 << numBitsForType;
    static constexpr UInt8 BinEncodingIndexDeltas = 2 << numBitsForType;
    static constexpr UInt8 BinEncodingContiguousCounts = 3 << numBitsForType;
};

} // namespace DB
