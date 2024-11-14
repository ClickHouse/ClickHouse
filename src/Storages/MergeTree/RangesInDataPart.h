#pragma once

#include <vector>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Storages/MergeTree/MarkRange.h>
#include "Storages/MergeTree/AlterConversions.h"
#include "Storages/MergeTree/MergeTreePartInfo.h"


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

    void merge(RangesInDataPartsDescription & other);
};

struct RangesInDataPart
{
    DataPartPtr data_part;
    size_t part_index_in_query;
    MarkRanges ranges;
    MarkRanges exact_ranges;

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

}
