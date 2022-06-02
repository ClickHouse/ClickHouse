#include <limits>
#include <base/defines.h>
#include <city.h>
#include <Common/HashTable/Hash.h>
#include <Common/logger_useful.h>
#include <Core/ColumnWithTypeAndName.h>
#include <Core/Types.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Poco/Logger.h>
#include <Storages/MergeTree/MergeTreeStatistic.h>
#include <Storages/MergeTree/MergeTreeStatisticGranuleStringHash.h>
#include <Storages/Statistics.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int LOGICAL_ERROR;
}

constexpr size_t COUNT_MIN_SKETCH_ELEMENTS = 1024;
const size_t COUNT_MIN_SKETCH_SEEDS[5] = {
    3253452652,
    4326609322,
    9063481344,
    125234653464,
    5345223442,
};

CountMinSketch::CountMinSketch()
    : data(COUNT_MIN_SKETCH_ELEMENTS, 0)
{
}

void CountMinSketch::addString(const String& str)
{
    const auto hash = getStringHash(str);
    for (const auto seed : COUNT_MIN_SKETCH_SEEDS)
    {
        ++data[getUintHash(hash, seed) % data.size()];
    }
}

size_t CountMinSketch::getStringCount(const String& str) const
{
    size_t result = std::numeric_limits<size_t>::max();
    const auto hash = getStringHash(str);
    for (const auto seed : COUNT_MIN_SKETCH_SEEDS)
    {
        result = std::min(result, static_cast<size_t>(data[getUintHash(hash, seed) % data.size()]));
    }
    return result;
}

size_t CountMinSketch::getSizeInMemory() const
{
    return sizeof(*this) + sizeof(data[0]) * data.size();
}

size_t CountMinSketch::getStringHash(const String& str)
{
    return CityHash_v1_0_2::CityHash64(str.data(), str.size());
}

size_t CountMinSketch::getUintHash(size_t hash, size_t seed)
{
    return intHash64(hash ^ seed);
}

void CountMinSketch::merge(const CountMinSketch& other)
{
    for (size_t index = 0; index < data.size(); ++index)
    {
        data[index] += other.data[index];
    }
}

void CountMinSketch::serialize(WriteBuffer& wb) const
{
    for (size_t index = 0; index < COUNT_MIN_SKETCH_ELEMENTS; ++index)
    {
        writeIntBinary(data[index], wb);
    }
}

void CountMinSketch::deserialize(ReadBuffer& rb)
{
    for (size_t index = 0; index < COUNT_MIN_SKETCH_ELEMENTS; ++index)
    {
        readIntBinary(data[index], rb);
    }
}

MergeTreeGranuleStringHashStatistic::MergeTreeGranuleStringHashStatistic(
    const String & name_,
    const String & column_name_)
    : statistic_name(name_)
    , column_name(column_name_)
    , total_granules(0)
    , is_empty(true)
{
}

MergeTreeGranuleStringHashStatistic::MergeTreeGranuleStringHashStatistic(
    const String & name_,
    const String & column_name_,
    size_t total_granules_,
    CountMinSketch&& sketch_)
    : statistic_name(name_)
    , column_name(column_name_)
    , total_granules(total_granules_)
    , sketch(sketch_)
    , is_empty(false)
{
}

const String& MergeTreeGranuleStringHashStatistic::name() const
{
    return statistic_name;
}

const String& MergeTreeGranuleStringHashStatistic::type() const
{
    static String name = "granule_string_hash";
    return name;
}

bool MergeTreeGranuleStringHashStatistic::empty() const
{
    return is_empty;
}

void MergeTreeGranuleStringHashStatistic::merge(const IStatisticPtr & other)
{
    auto other_ptr = std::dynamic_pointer_cast<MergeTreeGranuleStringHashStatistic>(other);
    // versions control???
    if (other_ptr)
    {
        is_empty &= other_ptr->is_empty;
        total_granules += other_ptr->total_granules;
        sketch.merge(other_ptr->sketch);
    }
    // Just ignore unknown sketches.
    // We can get wrong sketch during MODIFY/DROP+ADD/... mutation.
}

const String& MergeTreeGranuleStringHashStatistic::getColumnsRequiredForStatisticCalculation() const
{
    return column_name;
}

void MergeTreeGranuleStringHashStatistic::serializeBinary(WriteBuffer & ostr) const
{
    WriteBufferFromOwnString wb;
    writeIntBinary(total_granules, wb);
    sketch.serialize(wb);
    wb.finalize();

    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->serializeBinary(static_cast<size_t>(MergeTreeStringSearchStatisticType::GRANULE_COUNT_MIN_SKETCH_HASH), ostr);
    size_serialization->serializeBinary(wb.str().size(), ostr);
    ostr.write(wb.str().data(), wb.str().size());
}

bool MergeTreeGranuleStringHashStatistic::validateTypeBinary(ReadBuffer & istr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field ftype;
    size_serialization->deserializeBinary(ftype, istr);
    return ftype.get<size_t>() == static_cast<size_t>(MergeTreeStringSearchStatisticType::GRANULE_COUNT_MIN_SKETCH_HASH);
}

void MergeTreeGranuleStringHashStatistic::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field unused_size;
    size_serialization->deserializeBinary(unused_size, istr);

    readIntBinary(total_granules, istr);
    sketch.deserialize(istr);
    is_empty = false;
}

double MergeTreeGranuleStringHashStatistic::estimateStringProbability(const String& needle) const
{
    return sketch.getStringCount(needle) / static_cast<double>(total_granules);
}

std::optional<double> MergeTreeGranuleStringHashStatistic::estimateSubstringsProbability(const Strings& needles) const
{
    UNUSED(needles);
    // not supported now
    return std::nullopt;
}

size_t MergeTreeGranuleStringHashStatistic::getSizeInMemory() const
{
    return sketch.getSizeInMemory() + sizeof(*this);
}

MergeTreeGranuleStringHashStatisticCollector::MergeTreeGranuleStringHashStatisticCollector(
    const String & name_,
    const String & column_name_)
    : statistic_name(name_)
    , column_name(column_name_)
    , total_granules(0)
{
}

const String& MergeTreeGranuleStringHashStatisticCollector::name() const
{
    return statistic_name;
}

const String & MergeTreeGranuleStringHashStatisticCollector::type() const
{
    static String type = "granule_string_hash";
    return type;
}

StatisticType MergeTreeGranuleStringHashStatisticCollector::statisticType() const
{
    return StatisticType::STRING_SEARCH;
}

const String & MergeTreeGranuleStringHashStatisticCollector::column() const
{
    return column_name;
}

bool MergeTreeGranuleStringHashStatisticCollector::empty() const
{
    return total_granules == 0;
}

IStringSearchStatisticPtr MergeTreeGranuleStringHashStatisticCollector::getStringSearchStatisticAndReset()
{
    return std::make_shared<MergeTreeGranuleStringHashStatistic>(statistic_name, column_name, total_granules, std::move(sketch));
}

void MergeTreeGranuleStringHashStatisticCollector::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    const auto & column_with_type = block.getByName(column_name);
    const auto & column = column_with_type.column;
    for (size_t i = 0; i < rows_read; ++i)
    {
        const auto value = column->getDataAt((*pos) + i);
        granule_string_set.insert(String(value.data, value.size));
    }
    *pos += rows_read;
}

void MergeTreeGranuleStringHashStatisticCollector::granuleFinished()
{
    for (const auto& string : granule_string_set)
    {
        sketch.addString(string);
    }
    ++total_granules;
    granule_string_set.clear();
}

IStringSearchStatisticPtr creatorGranuleStringHashStatistic(
    const StatisticDescription & statistic, const ColumnDescription & column)
{
    validatorGranuleStringHashStatistic(statistic, column);
    if (std::find(std::begin(statistic.column_names), std::end(statistic.column_names), column.name) == std::end(statistic.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", statistic.name, column.name);
    return std::make_shared<MergeTreeGranuleStringHashStatistic>(statistic.name, column.name);
}

IMergeTreeStatisticCollectorPtr creatorGranuleStringHashStatisticCollector(
    const StatisticDescription & statistic, const ColumnDescription & column)
{
    validatorGranuleStringHashStatistic(statistic, column);
    if (std::find(std::begin(statistic.column_names), std::end(statistic.column_names), column.name) == std::end(statistic.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", statistic.name, column.name);
    return std::make_shared<MergeTreeGranuleStringHashStatisticCollector>(statistic.name, column.name);

}

void validatorGranuleStringHashStatistic(
    const StatisticDescription &, const ColumnDescription & column)
{
    if (column.type->getTypeId() != TypeIndex::String)
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic GranuleStringHash can be used only for string columns.");
    if (column.type->isNullable())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic GranuleStringHash can be used only for not nullable columns.");
}

}
