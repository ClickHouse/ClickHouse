#include <Storages/MergeTree/MergeTreeStatisticTDigest.h>

#include <cmath>
#include <Common/Exception.h>
#include "DataTypes/DataTypesNumber.h"
#include "IO/WriteBufferFromString.h"
#include "Storages/Statistics.h"
#include <limits>
#include <memory>
#include <Poco/Logger.h>
#include <string>

namespace DB
{

namespace
{
constexpr float EPS = 1e-5;
}

namespace ErrorCodes
{
extern const int INCORRECT_QUERY;
extern const int LOGICAL_ERROR;
}

MergeTreeColumnDistributionStatisticTDigest::MergeTreeColumnDistributionStatisticTDigest(
    const String & name_,
    const String & column_name_)
    : stat_name(name_)
    , column_name(column_name_)
    , is_empty(true)
{
}

MergeTreeColumnDistributionStatisticTDigest::MergeTreeColumnDistributionStatisticTDigest(
    QuantileTDigest<Float32>&& sketch_, const String & name_, const String & column_name_)
    : stat_name(name_)
    , column_name(column_name_)
    , sketch(std::move(sketch_))
    , is_empty(false)
{
}

const String& MergeTreeColumnDistributionStatisticTDigest::name() const
{
    return stat_name;
}

const String& MergeTreeColumnDistributionStatisticTDigest::type() const
{
    static String name = "tdigest";
    return name;
}

bool MergeTreeColumnDistributionStatisticTDigest::empty() const
{
    return is_empty;
}

void MergeTreeColumnDistributionStatisticTDigest::merge(const IStatisticPtr & other)
{
    auto other_ptr = std::dynamic_pointer_cast<MergeTreeColumnDistributionStatisticTDigest>(other);
    if (other_ptr)
    {
        is_empty &= other_ptr->is_empty;
        sketch.merge(other_ptr->sketch);
    }
    else
    {
        throw Exception("Unknown distribution sketch type", ErrorCodes::LOGICAL_ERROR);
    }
}

const String& MergeTreeColumnDistributionStatisticTDigest::getColumnsRequiredForStatisticCalculation() const
{
    return column_name;
}

void MergeTreeColumnDistributionStatisticTDigest::serializeBinary(WriteBuffer & ostr) const
{
    WriteBufferFromOwnString wb;
    sketch.serialize(wb);
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->serializeBinary(static_cast<size_t>(MergeTreeDistributionStatisticType::TDIGEST), ostr);
    size_serialization->serializeBinary(wb.str().size(), ostr);
    ostr.write(wb.str().data(), wb.str().size());
}

bool MergeTreeColumnDistributionStatisticTDigest::validateTypeBinary(ReadBuffer & istr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field ftype;
    size_serialization->deserializeBinary(ftype, istr);
    return ftype.get<size_t>() == static_cast<size_t>(MergeTreeDistributionStatisticType::TDIGEST);
}

void MergeTreeColumnDistributionStatisticTDigest::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field unused;
    size_serialization->deserializeBinary(unused, istr);

    sketch.deserialize(istr);
    is_empty = false;
}

namespace
{
double extractValue(const Field& value)
{
    if (value.getType() == Field::Types::UInt64)
    {
        return value.get<UInt64>();
    }
    else if (value.getType() == Field::Types::Int64)
    {
        return value.get<Int64>();
    }
    else if (value.getType() == Field::Types::Float64)
    {
        return value.get<Float64>();
    }
    else
    {
        throw Exception("Bad type for TDigest", ErrorCodes::LOGICAL_ERROR);
    }
}
}

double MergeTreeColumnDistributionStatisticTDigest::estimateQuantileLower(double threshold) const
{
    if (empty())
        throw Exception("TDigest is empty", ErrorCodes::LOGICAL_ERROR);

    if (std::isnan(threshold)
        || std::isinf(threshold)
        || threshold < std::numeric_limits<Float32>::lowest()
        || threshold > std::numeric_limits<Float32>::max())
        return 0.0;
    else
        return sketch.cdf(threshold);
}

double MergeTreeColumnDistributionStatisticTDigest::estimateQuantileUpper(double threshold) const
{
    if (empty())
        throw Exception("TDigest is empty", ErrorCodes::LOGICAL_ERROR);

    if (std::isnan(threshold)
        || std::isinf(threshold)
        || threshold < std::numeric_limits<Float32>::lowest()
        || threshold > std::numeric_limits<Float32>::max())
        return 1.0;
    else
        return sketch.cdf(threshold);
}

double MergeTreeColumnDistributionStatisticTDigest::estimateProbability(const Field& lower, const Field& upper) const
{
    // lower <= value <= upper
    // null = infty
    // Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("est " + toString(lower) + " " + toString(upper));
    // Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("emp=" + toString(empty()));
    // Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("sktch50=" + toString(sketch.getFloat(0.5)) + " " + toString(sketch.getFloat(1)));
    // Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("upper = " + (upper.isNull() ? "null" : std::to_string(estimateQuantileUpper(upper))));
    // Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("lower = " + (lower.isNull() ? "null" : std::to_string(estimateQuantileUpper(lower))));
    if (!lower.isNull() && !upper.isNull())
    {
        if (lower == upper)
            return std::max(std::min(estimateQuantileLower(extractValue(upper) + EPS) - estimateQuantileUpper(extractValue(lower) - EPS), 1.0), 0.0);
        else
            return std::max(std::min(estimateQuantileLower(extractValue(upper)) - estimateQuantileUpper(extractValue(lower)), 1.0), 0.0);
    }
    else if (!lower.isNull())
        return std::max(std::min(1.0 - estimateQuantileUpper(extractValue(lower)), 1.0), 0.0);
    else if (!upper.isNull())
        return std::max(std::min(estimateQuantileLower(extractValue(upper)), 1.0), 0.0);
    else
        return 1.0 - 0.0;
}

size_t MergeTreeColumnDistributionStatisticTDigest::getSizeInMemory() const
{
    return sketch.getAllocatedBytes();
}

MergeTreeColumnDistributionStatisticCollectorTDigest::MergeTreeColumnDistributionStatisticCollectorTDigest(
    const String & name_,
    const String & column_name_)
    : stat_name(name_)
    , column_name(column_name_)
{
}

const String & MergeTreeColumnDistributionStatisticCollectorTDigest::name() const
{
    return stat_name;
}

const String & MergeTreeColumnDistributionStatisticCollectorTDigest::type() const
{
    static String name = "tdigest";
    return name;
}

const String & MergeTreeColumnDistributionStatisticCollectorTDigest::column() const
{
    return column_name;
}

bool MergeTreeColumnDistributionStatisticCollectorTDigest::empty() const
{
    return !sketch.has_value();
}

StatisticType MergeTreeColumnDistributionStatisticCollectorTDigest::statisticType() const
{
    return StatisticType::NUMERIC_COLUMN_DISRIBUTION;
}

IDistributionStatisticPtr MergeTreeColumnDistributionStatisticCollectorTDigest::getDistributionStatisticAndReset()
{
    if (empty())
        throw Exception("TDigest collector is empty", ErrorCodes::LOGICAL_ERROR);
    std::optional<QuantileTDigest<Float32>> res;
    sketch.swap(res);
    res->compress();
    return std::make_shared<MergeTreeColumnDistributionStatisticTDigest>(*std::move(res), stat_name, column_name);
}

void MergeTreeColumnDistributionStatisticCollectorTDigest::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    if (!sketch)
        sketch.emplace();

    const size_t rows_read = std::min(limit, block.rows() - *pos);
    const auto & column_with_type = block.getByName(column_name);
    const auto & column = column_with_type.column;
    for (size_t i = 0; i < rows_read; ++i)
    {
        const auto value = column->getFloat32((*pos) + i);
        sketch->add(value);
    }
    *pos += rows_read;
}

void MergeTreeColumnDistributionStatisticCollectorTDigest::granuleFinished()
{
    // do nothing
}

IStatisticPtr creatorColumnDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column)
{
    validatorColumnDistributionStatisticTDigest(stat, column);
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column.name) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column.name);
    return std::make_shared<MergeTreeColumnDistributionStatisticTDigest>(stat.name, column.name);
}

IMergeTreeStatisticCollectorPtr creatorColumnDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const ColumnDescription & column)
{
    validatorColumnDistributionStatisticTDigest(stat, column);
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column.name) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column.name);
    return std::make_shared<MergeTreeColumnDistributionStatisticCollectorTDigest>(stat.name, column.name);
}

void validatorColumnDistributionStatisticTDigest(
    const StatisticDescription &, const ColumnDescription & column)
{
    if (!column.type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic TDIGEST can be used only for numeric columns.");
    if (column.type->isNullable())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic TDIGEST can be used only for not nullable columns.");
}

}
