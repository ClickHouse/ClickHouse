#include <Storages/MergeTree/MergeTreeStatisticGranuleTDigest.h>

#include <cmath>
#include <Common/Exception.h>
#include <DataTypes/DataTypesNumber.h>
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

MergeTreeGranuleDistributionStatisticTDigest::MergeTreeGranuleDistributionStatisticTDigest(
    const String & name_,
    const String & column_name_)
    : stat_name(name_)
    , column_name(column_name_)
    , is_empty(true)
{
}

MergeTreeGranuleDistributionStatisticTDigest::MergeTreeGranuleDistributionStatisticTDigest(
    QuantileTDigest<Float32>&& min_sketch_,
    QuantileTDigest<Float32>&& max_sketch_,
    const String & name_,
    const String & column_name_)
    : stat_name(name_)
    , column_name(column_name_)
    , min_sketch(std::move(min_sketch_))
    , max_sketch(std::move(max_sketch_))
    , is_empty(false)
{
}

const String& MergeTreeGranuleDistributionStatisticTDigest::name() const
{
    return stat_name;
}

const String& MergeTreeGranuleDistributionStatisticTDigest::type() const
{
    static String name = "granule_tdigest";
    return name;
}

bool MergeTreeGranuleDistributionStatisticTDigest::empty() const
{
    return is_empty;
}

void MergeTreeGranuleDistributionStatisticTDigest::merge(const IStatisticPtr & other)
{
    auto other_ptr = std::dynamic_pointer_cast<MergeTreeGranuleDistributionStatisticTDigest>(other);
    if (other_ptr)
    {
        is_empty &= other_ptr->is_empty;
        min_sketch.merge(other_ptr->min_sketch);
        max_sketch.merge(other_ptr->max_sketch);
    }
    // Just ignore unknown sketches.
    // We can get wrong sketch during MODIFY/DROP+ADD/... mutation.
}

const String& MergeTreeGranuleDistributionStatisticTDigest::getColumnsRequiredForStatisticCalculation() const
{
    return column_name;
}

void MergeTreeGranuleDistributionStatisticTDigest::serializeBinary(WriteBuffer & ostr) const
{
    WriteBufferFromOwnString wb;
    min_sketch.serialize(wb);
    max_sketch.serialize(wb);
    wb.finalize();

    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    size_serialization->serializeBinary(static_cast<size_t>(MergeTreeDistributionStatisticType::GRANULE_TDIGEST), ostr);
    size_serialization->serializeBinary(wb.str().size(), ostr);
    ostr.write(wb.str().data(), wb.str().size());
}

bool MergeTreeGranuleDistributionStatisticTDigest::validateTypeBinary(ReadBuffer & istr) const
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field ftype;
    size_serialization->deserializeBinary(ftype, istr);
    return ftype.get<size_t>() == static_cast<size_t>(MergeTreeDistributionStatisticType::GRANULE_TDIGEST);
}

void MergeTreeGranuleDistributionStatisticTDigest::deserializeBinary(ReadBuffer & istr)
{
    const auto & size_type = DataTypePtr(std::make_shared<DataTypeUInt64>());
    auto size_serialization = size_type->getDefaultSerialization();
    Field unused_size;
    size_serialization->deserializeBinary(unused_size, istr);

    min_sketch.deserialize(istr);
    max_sketch.deserialize(istr);
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
        throw Exception("Bad type for BlockTDigest", ErrorCodes::LOGICAL_ERROR);
    }
}
}

double MergeTreeGranuleDistributionStatisticTDigest::estimateQuantileLower(double threshold) const
{
    if (empty())
        throw Exception("BlockTDigest is empty", ErrorCodes::LOGICAL_ERROR);

    if (std::isnan(threshold)
        || std::isinf(threshold)
        || threshold < std::numeric_limits<Float32>::lowest()
        || threshold > std::numeric_limits<Float32>::max())
    {
        return 0.0;
    }
    else
        return min_sketch.cdf(threshold);
}

double MergeTreeGranuleDistributionStatisticTDigest::estimateQuantileUpper(double threshold) const
{
    if (empty())
        throw Exception("BlockTDigest is empty", ErrorCodes::LOGICAL_ERROR);

    if (std::isnan(threshold)
        || std::isinf(threshold)
        || threshold < std::numeric_limits<Float32>::lowest()
        || threshold > std::numeric_limits<Float32>::max())
    {
        return 1.0;
    }
    else
        return max_sketch.cdf(threshold);
}

double MergeTreeGranuleDistributionStatisticTDigest::estimateProbability(const Field& lower, const Field& upper) const
{
    // lower <= value <= upper
    // null = infty
    // Poco::Logger::get("MergeTreeGranuleDistributionStatisticTDigest").information(column_name);
    // Poco::Logger::get("MergeTreeGranuleDistributionStatisticTDigest").information(std::to_string(min_sketch.get(0.5)) + " " + std::to_string(min_sketch.get(1.0)));
    // Poco::Logger::get("MergeTreeGranuleDistributionStatisticTDigest").information(std::to_string(max_sketch.get(0.5)) + " " + std::to_string(max_sketch.get(1.0)));
    // Poco::Logger::get("MergeTreeGranuleDistributionStatisticTDigest").information("est " + toString(lower) + " " + toString(upper) + " emp=" + toString(empty()));
    // Poco::Logger::get("MergeTreeGranuleDistributionStatisticTDigest").information("lo = " + (lower.isNull() ? "null" : (std::to_string(estimateQuantileLower(lower)) + " " + std::to_string(estimateQuantileUpper(lower)))));
    // Poco::Logger::get("MergeTreeGranuleDistributionStatisticTDigest").information("up = " + (upper.isNull() ? "null" : (std::to_string(estimateQuantileLower(upper)) + " " + std::to_string(estimateQuantileUpper(upper)))));
    if (!lower.isNull() && !upper.isNull())
        return std::max(std::min(estimateQuantileLower(extractValue(upper)) - estimateQuantileUpper(extractValue(lower)), 1.0), 0.0);
    else if (!lower.isNull())
        return std::max(std::min(1.0 - estimateQuantileUpper(extractValue(lower)), 1.0), 0.0);
    else if (!upper.isNull())
        return std::max(std::min(estimateQuantileLower(extractValue(upper)), 1.0), 0.0);
    else
        return 1.0 - 0.0;
}

size_t MergeTreeGranuleDistributionStatisticTDigest::getSizeInMemory() const
{
    return min_sketch.getAllocatedBytes() + max_sketch.getAllocatedBytes();
}

MergeTreeGranuleDistributionStatisticCollectorTDigest::MergeTreeGranuleDistributionStatisticCollectorTDigest(
    const String & name_,
    const String & column_name_)
    : stat_name(name_)
    , column_name(column_name_)
{
}

const String & MergeTreeGranuleDistributionStatisticCollectorTDigest::name() const
{
    return stat_name;
}

const String & MergeTreeGranuleDistributionStatisticCollectorTDigest::type() const
{
    static String type = "granule_tdigest";
    return type;
}

const String & MergeTreeGranuleDistributionStatisticCollectorTDigest::column() const
{
    return column_name;
}

bool MergeTreeGranuleDistributionStatisticCollectorTDigest::empty() const
{
    return !min_sketch.has_value() || !max_sketch.has_value();
}

StatisticType MergeTreeGranuleDistributionStatisticCollectorTDigest::statisticType() const
{
    return StatisticType::NUMERIC_COLUMN_DISRIBUTION;
}

IDistributionStatisticPtr MergeTreeGranuleDistributionStatisticCollectorTDigest::getDistributionStatisticAndReset()
{
    if (empty())
        throw Exception("GranuleTDigest collector is empty", ErrorCodes::LOGICAL_ERROR);
    std::optional<QuantileTDigest<Float32>> min_res;
    min_sketch.swap(min_res);
    min_res->compress();
    std::optional<QuantileTDigest<Float32>> max_res;
    max_sketch.swap(max_res);
    max_res->compress();
    return std::make_shared<MergeTreeGranuleDistributionStatisticTDigest>(
        *std::move(min_res),
        *std::move(max_res),
        stat_name,
        column_name);
}

void MergeTreeGranuleDistributionStatisticCollectorTDigest::update(const Block & block, size_t * pos, size_t limit)
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
        const auto value = column->getFloat32((*pos) + i);
        if (!min_current || *min_current > value + EPS)
            min_current = value;
        if (!max_current || *max_current < value - EPS)
            max_current = value;
    }
    *pos += rows_read;
}

void MergeTreeGranuleDistributionStatisticCollectorTDigest::granuleFinished()
{
    if (!min_current || !max_current)
        return;

    if (!min_sketch)
        min_sketch.emplace();
    if (!max_sketch)
        max_sketch.emplace();

    min_sketch->add(*min_current);
    max_sketch->add(*max_current);
    min_current.reset();
    max_current.reset();
}

IStatisticPtr creatorGranuleDistributionStatisticTDigest(
    const StatisticDescription & stat, const ColumnDescription & column)
{
    validatorGranuleDistributionStatisticTDigest(stat, column);
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column.name) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column.name);
    return std::make_shared<MergeTreeGranuleDistributionStatisticTDigest>(stat.name, column.name);
}

IMergeTreeStatisticCollectorPtr creatorGranuleDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const ColumnDescription & column)
{
    validatorGranuleDistributionStatisticTDigest(stat, column);
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column.name) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column.name);
    return std::make_shared<MergeTreeGranuleDistributionStatisticCollectorTDigest>(stat.name, column.name);
}

void validatorGranuleDistributionStatisticTDigest(
    const StatisticDescription &, const ColumnDescription & column)
{
    if (!column.type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic GranuleTDigest can be used only for numeric columns.");
    if (column.type->isNullable())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic GranuleTDigest can be used only for not nullable columns.");
}

}
