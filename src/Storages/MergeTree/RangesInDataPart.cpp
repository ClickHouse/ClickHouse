#include <Storages/MergeTree/RangesInDataPart.h>

#include <Core/ProtocolDefines.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <IO/VarInt.h>

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

namespace
{
    /// Cap the number of elements printed by the describe methods:
    /// an uncapped list can be megabytes of text for tables with many parts.
    constexpr size_t max_elements_to_describe = 100;
}


void RangesInDataPartDescription::serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const
{
    info.serialize(out);
    ranges.serialize(out);
    writeVarUInt(rows, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION)
        writeBinary(projection_name, out);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        writeVarUInt(min_marks_per_task, out);
}

String RangesInDataPartDescription::describe() const
{
    if (ranges.size() <= max_elements_to_describe)
        return fmt::format("{}[{}]", getPartOrProjectionName(), fmt::join(ranges, ","));

    return fmt::format(
        "{}[{} and {} more]",
        getPartOrProjectionName(),
        fmt::join(ranges.begin(), ranges.begin() + max_elements_to_describe, ","),
        ranges.size() - max_elements_to_describe);
}

String RangesInDataPartDescription::getPartOrProjectionName() const
{
    if (projection_name.empty())
        return info.getPartNameV1();

    return info.getPartNameV1() + "." + projection_name;
}

void RangesInDataPartDescription::deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version)
{
    info.deserialize(in);
    ranges.deserialize(in);
    readVarUInt(rows, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_PROJECTION)
        readBinary(projection_name, in);

    if (parallel_replicas_protocol_version >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
        readVarUInt(min_marks_per_task, in);
}

void RangesInDataPartsDescription::serialize(WriteBuffer & out, UInt64 parallel_replicas_protocol_version) const
{
    writeVarUInt(this->size(), out);
    for (const auto & desc : *this)
        desc.serialize(out, parallel_replicas_protocol_version);
}

String RangesInDataPartsDescription::describe() const
{
    if (this->size() <= max_elements_to_describe)
        return fmt::format("{} parts: [{}]", this->size(), fmt::join(*this, ", "));

    return fmt::format(
        "{} parts: [{} and {} more]",
        this->size(),
        fmt::join(this->begin(), this->begin() + max_elements_to_describe, ", "),
        this->size() - max_elements_to_describe);
}

String RangesInDataPartsDescription::describeShort() const
{
    size_t num_ranges = 0;
    size_t num_marks = 0;
    for (const auto & part : *this)
    {
        num_ranges += part.ranges.size();
        num_marks += part.ranges.getNumberOfMarks();
    }
    return fmt::format("{} parts, {} ranges, {} marks", this->size(), num_ranges, num_marks);
}

void RangesInDataPartsDescription::deserialize(ReadBuffer & in, UInt64 parallel_replicas_protocol_version)
{
    size_t new_size = 0;
    readVarUInt(new_size, in);
    if (new_size > 100'000'000'000)
        throw DB::Exception(DB::ErrorCodes::TOO_LARGE_ARRAY_SIZE, "The size of serialized parts description is suspiciously large: {}", new_size);

    this->resize(new_size);
    for (auto & desc : *this)
        desc.deserialize(in, parallel_replicas_protocol_version);
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
    const MarkRanges & ranges_,
    const RangesInDataPartReadHints & read_hints_)
    : data_part{data_part_}
    , parent_part{parent_part_}
    , part_index_in_query{part_index_in_query_}
    , part_starting_offset_in_query{part_starting_offset_in_query_}
    , ranges{ranges_}
    , read_hints{read_hints_}
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
    chassert(!data_part->isProjectionPart() || parent_part);
    return RangesInDataPartDescription{
        .info = data_part->isProjectionPart() ? parent_part->info : data_part->info,
        .ranges = ranges,
        .rows = getRowsCount(),
        .projection_name = data_part->isProjectionPart() ? data_part->name : "",
    };
}

size_t RangesInDataPart::getMarksCount() const
{
    return ranges.getNumberOfMarks();
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
