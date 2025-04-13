#pragma once

#include <unordered_map>
#include <vector>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>


namespace DB
{

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

/// The only purpose of this struct is that serialize and deserialize methods
/// they look natural here because we can fully serialize and then deserialize original DataPart class.
struct RangesInDataPartDescription
{
    MergeTreePartInfo info{};
    MarkRanges ranges{};
    size_t rows = 0;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);
};

struct RangesInDataPartsDescription: public std::deque<RangesInDataPartDescription>
{
    using std::deque<RangesInDataPartDescription>::deque;

    void serialize(WriteBuffer & out) const;
    String describe() const;
    void deserialize(ReadBuffer & in);

    void merge(const RangesInDataPartsDescription & other);
};

using PartOffsets = std::vector<UInt64>;
using VectorSearchResultDistances = std::vector<float>;

struct RangesInDataPartOptionals
{
    std::optional<PartOffsets> exact_part_offsets;
    std::optional<VectorSearchResultDistances> distances;

    bool hasPartOffsets() const { return exact_part_offsets.has_value(); }
    bool hasDistances() const { return distances.has_value(); }
};

struct RangesInDataPart
{
    DataPartPtr data_part;
    size_t part_index_in_query;
    MarkRanges ranges;
    MarkRanges exact_ranges;
    RangesInDataPartOptionals extensions;

    RangesInDataPart() = default;

    RangesInDataPart(
        const DataPartPtr & data_part_,
        const size_t part_index_in_query_,
        const MarkRanges & ranges_ = MarkRanges{})
        : data_part{data_part_}
        , part_index_in_query{part_index_in_query_}
        , ranges{ranges_}
    {
    }

    RangesInDataPartDescription getDescription() const;

    size_t getMarksCount() const;
    size_t getRowsCount() const;
};

struct RangesInDataParts: public std::vector<RangesInDataPart>
{
    using std::vector<RangesInDataPart>::vector; /// NOLINT(modernize-type-traits)

    RangesInDataPartsDescription getDescriptions() const;

    size_t getMarksCountAllParts() const;
    size_t getRowsCountAllParts() const;
};

struct DataPartInfo
{
    DataPartPtr data_part;
    AlterConversionsPtr alter_conversions;
};
using DataPartsInfo = std::unordered_map<size_t, DataPartInfo>;
using DataPartsInfoPtr = std::shared_ptr<DataPartsInfo>;

}
