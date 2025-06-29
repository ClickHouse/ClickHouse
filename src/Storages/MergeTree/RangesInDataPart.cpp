#include <Storages/MergeTree/RangesInDataPart.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include "IO/VarInt.h"

template <>
struct fmt::formatter<DB::RangesInDataPartDescription>
{
    static constexpr auto parse(format_parse_context & ctx) { return ctx.begin(); }

    template <typename FormatContext>
    auto format(const DB::RangesInDataPartDescription & range, FormatContext & ctx) const
    {
        return fmt::format_to(ctx.out(), "{}", range.describe());
    }
};

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_ARRAY_SIZE;
}


void RangesInDataPartDescription::serialize(WriteBuffer & out) const
{
    info.serialize(out);
    ranges.serialize(out);
    writeVarUInt(rows, out);
}

String RangesInDataPartDescription::describe() const
{
    String result;
    result += fmt::format("{}[{}]", info.getPartNameV1(), fmt::join(ranges, ","));
    return result;
}

void RangesInDataPartDescription::deserialize(ReadBuffer & in)
{
    info.deserialize(in);
    ranges.deserialize(in);
    readVarUInt(rows, in);
}

void RangesInDataPartsDescription::serialize(WriteBuffer & out) const
{
    writeVarUInt(this->size(), out);
    for (const auto & desc : *this)
        desc.serialize(out);
}

String RangesInDataPartsDescription::describe() const
{
    return fmt::format("{} parts: [{}]", this->size(), fmt::join(*this, ", "));
}

void RangesInDataPartsDescription::deserialize(ReadBuffer & in)
{
    size_t new_size = 0;
    readVarUInt(new_size, in);
    if (new_size > 100'000'000'000)
        throw DB::Exception(DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "The size of serialized hash table is suspiciously large: {}", new_size);

    this->resize(new_size);
    for (auto & desc : *this)
        desc.deserialize(in);
}

void RangesInDataPartsDescription::merge(const RangesInDataPartsDescription & other)
{
    for (const auto & desc : other)
        this->emplace_back(desc);
}

RangesInDataPart::RangesInDataPart(
    const DataPartPtr & data_part_,
    const DataPartPtr & parent_part_,
    size_t part_index_in_query_,
    size_t part_starting_offset_in_query_,
    const MarkRanges & ranges_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
    , ranges{ranges_}
{
}

RangesInDataPart::RangesInDataPart(
    const DataPartPtr & data_part_, const DataPartPtr & parent_part_, size_t part_index_in_query_, size_t part_starting_offset_in_query_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
{
    size_t total_marks_count = data_part->index_granularity->getMarksCountWithoutFinal();
    if (total_marks_count)
        ranges.emplace_back(0, total_marks_count);
}

RangesInDataPartDescription RangesInDataPart::getDescription() const
{
    return RangesInDataPartDescription{
        .info = data_part->info,
        .ranges = ranges,
        .rows = getRowsCount(),
    };
}

size_t RangesInDataPart::getMarksCount() const
{
    size_t total = 0;
    for (const auto & range : ranges)
        total += range.end - range.begin;

    return total;
}

size_t RangesInDataPart::getRowsCount() const
{
    return data_part->index_granularity->getRowsCountInRanges(ranges);
}

RangesInDataParts::RangesInDataParts(const DataPartsVector & parts)
{
    size_t num_parts = parts.size();
    reserve(num_parts);
    size_t starting_offset = 0;
    for (size_t i = 0; i < num_parts; ++i)
    {
        chassert(!parts[i]->isProjectionPart());
        emplace_back(parts[i], nullptr, i, starting_offset);
        starting_offset += parts[i]->rows_count;
    }
}

RangesInDataPartsDescription RangesInDataParts::getDescriptions() const
{
    RangesInDataPartsDescription result;
    for (const auto & part : *this)
        result.emplace_back(part.getDescription());
    return result;
}


size_t RangesInDataParts::getMarksCountAllParts() const
{
    size_t result = 0;
    for (const auto & part : *this)
        result += part.getMarksCount();
    return result;
}

size_t RangesInDataParts::getRowsCountAllParts() const
{
    size_t result = 0;
    for (const auto & part: *this)
        result += part.getRowsCount();
    return result;
}

}
