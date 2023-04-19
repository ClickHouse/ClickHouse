#include <Storages/MergeTree/RangesInDataPart.h>

#include <Storages/MergeTree/IMergeTreeDataPart.h>

#include "IO/VarInt.h"

#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>


namespace DB
{

void RangesInDataPartDescription::serialize(WriteBuffer & out) const
{
    info.serialize(out);
    ranges.serialize(out);
}

String RangesInDataPartDescription::describe() const
{
    String result;
    result += fmt::format("Part: {}, ", info.getPartNameV1());
    result += fmt::format("Ranges: [{}], ", fmt::join(ranges, ","));
    return result;
}

void RangesInDataPartDescription::deserialize(ReadBuffer & in)
{
    info.deserialize(in);
    ranges.deserialize(in);
}

void RangesInDataPartsDescription::serialize(WriteBuffer & out) const
{
    writeVarUInt(this->size(), out);
    for (const auto & desc : *this)
        desc.serialize(out);
}

String RangesInDataPartsDescription::describe() const
{
    String result;
    for (const auto & desc : *this)
        result += desc.describe() + ",";
    return result;
}

void RangesInDataPartsDescription::deserialize(ReadBuffer & in)
{
    size_t new_size = 0;
    readVarUInt(new_size, in);

    this->resize(new_size);
    for (auto & desc : *this)
        desc.deserialize(in);
}

void RangesInDataPartsDescription::merge(RangesInDataPartsDescription & other)
{
    for (const auto & desc : other)
        this->emplace_back(desc);
}

RangesInDataPartDescription RangesInDataPart::getDescription() const
{
    return RangesInDataPartDescription{
        .info = data_part->info,
        .ranges = ranges,
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
    return data_part->index_granularity.getRowsCountInRanges(ranges);
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
