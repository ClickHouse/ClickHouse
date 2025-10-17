#pragma once

#include <unordered_map>
#include <vector>
#include <deque>

#include <IO/WriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/VectorSearchUtils.h>


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
    String projection_name;

    void serialize(WriteBuffer & out, UInt64 parallel_protocol_version) const;
    String describe() const;
    void deserialize(ReadBuffer & in, UInt64 parallel_protocol_version);
    String getPartOrProjectionName() const;
};

struct RangesInDataPartsDescription: public std::deque<RangesInDataPartDescription>
{
    using std::deque<RangesInDataPartDescription>::deque;

    void serialize(WriteBuffer & out, UInt64 parallel_protocol_version) const;
    String describe() const;
    void deserialize(ReadBuffer & in, UInt64 parallel_protocol_version);

    void merge(const RangesInDataPartsDescription & other);
};


/// A vehicle which transports additional information to optimize searches
struct RangesInDataPartReadHints
{
    /// Currently only information related to vector search
    std::optional<NearestNeighbours> vector_search_results;
};

struct RangesInDataPart
{
    DataPartPtr data_part;
    DataPartPtr parent_part;
    size_t part_index_in_query;
    size_t part_starting_offset_in_query;
    MarkRanges ranges;
    MarkRanges exact_ranges;
    RangesInDataPartReadHints read_hints;

    RangesInDataPart(
        const DataPartPtr & data_part_,
        const DataPartPtr & parent_part_,
        size_t part_index_in_query_,
        size_t part_starting_offset_in_query_,
        const MarkRanges & ranges_);

    explicit RangesInDataPart(
        const DataPartPtr & data_part_,
        const DataPartPtr & parent_part_ = nullptr,
        size_t part_index_in_query_ = 0,
        size_t part_starting_offset_in_query_ = 0);

    RangesInDataPartDescription getDescription() const;

    size_t getMarksCount() const;
    size_t getRowsCount() const;
};

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;
using DataPartsVector = std::vector<DataPartPtr>;

struct RangesInDataParts : public std::vector<RangesInDataPart>
{
    using std::vector<RangesInDataPart>::vector; /// NOLINT(modernize-type-traits)

    explicit RangesInDataParts(const DataPartsVector & parts);
    RangesInDataPartsDescription getDescriptions() const;

    size_t getMarksCountAllParts() const;
    size_t getRowsCountAllParts() const;
};

}
