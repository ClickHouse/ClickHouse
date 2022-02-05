#include <Storages/MergeTree/MergeTreeStatisticBlockTDigest.h>

#include <cmath>
#include <Common/Exception.h>
#include <limits>
#include <memory>
#include <Poco/Logger.h>
#include <string>

namespace DB
{

namespace ErrorCodes {
extern int INCORRECT_QUERY;
}

MergeTreeColumnDistributionStatisticBlockTDigest::MergeTreeColumnDistributionStatisticBlockTDigest(
    const String & column_name_)
    : column_name(column_name_)
    , is_empty(true)
{
}

MergeTreeColumnDistributionStatisticBlockTDigest::MergeTreeColumnDistributionStatisticBlockTDigest(
    QuantileTDigest<Float32>&& sketch_, const String & column_name_)
    : column_name(column_name_)
    , sketch(std::move(sketch_))
    , is_empty(false)
{
}

const String& MergeTreeColumnDistributionStatisticBlockTDigest::name() const
{
    static String name = "block_tdigest";
    return name;
}

bool MergeTreeColumnDistributionStatisticBlockTDigest::empty() const
{
    return is_empty;
}

void MergeTreeColumnDistributionStatisticBlockTDigest::merge(const IStatisticPtr & other)
{
    auto other_ptr = std::dynamic_pointer_cast<MergeTreeColumnDistributionStatisticBlockTDigest>(other);
    // versions control???
    if (other_ptr)
    {
        is_empty &= other_ptr->is_empty;
        sketch.merge(other_ptr->sketch);
        Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information("MERGED emp=" + std::to_string(empty()));
    }
    else
    {
        throw Exception("Unknown distribution sketch type", ErrorCodes::LOGICAL_ERROR);
    }
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information(
            "MERGE: 50% = " + std::to_string(sketch.getFloat(0.5))
            + " 90% = " + std::to_string(sketch.getFloat(0.9))
            + " 1% = " + std::to_string(sketch.getFloat(0.01)));
}

const String& MergeTreeColumnDistributionStatisticBlockTDigest::getColumnsRequiredForStatisticCalculation() const
{
    return column_name;
}

void MergeTreeColumnDistributionStatisticBlockTDigest::serializeBinary(WriteBuffer & ostr) const
{
    sketch.serialize(ostr);
}

void MergeTreeColumnDistributionStatisticBlockTDigest::deserializeBinary(ReadBuffer & istr)
{
    sketch.deserialize(istr);
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information(
        "LOAD: 50% = " + std::to_string(sketch.getFloat(0.5))
        + " 90% = " + std::to_string(sketch.getFloat(0.9))
        + " 1% = " + std::to_string(sketch.getFloat(0.01)));
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

double MergeTreeColumnDistributionStatisticBlockTDigest::estimateQuantileLower(const Field& value) const
{
    if (empty())
        throw Exception("BlockTDigest is empty", ErrorCodes::LOGICAL_ERROR);
    // TODO: t-digest grows O(log n)
    // TODO: try ddsketch???

    double threshold = extractValue(value);
    if (std::isnan(threshold)
        || std::isinf(threshold)
        || threshold < std::numeric_limits<Float32>::min()
        || threshold > std::numeric_limits<Float32>::max())
        return 0.0;
    else
        return sketch.cdf(threshold).first;
}

double MergeTreeColumnDistributionStatisticBlockTDigest::estimateQuantileUpper(const Field& value) const
{
    if (empty())
        throw Exception("BlockTDigest is empty", ErrorCodes::LOGICAL_ERROR);

    double threshold = extractValue(value);
    if (std::isnan(threshold)
        || std::isinf(threshold)
        || threshold < std::numeric_limits<Float32>::min()
        || threshold > std::numeric_limits<Float32>::max())
        return 1.0;
    else
        return sketch.cdf(threshold).second;
}

double MergeTreeColumnDistributionStatisticBlockTDigest::estimateProbability(const Field& lower, const Field& upper) const
{
    // lower <= value <= upper
    // null = infty
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information("est " + toString(lower) + " " + toString(upper));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information("emp=" + toString(empty()));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information("sktch50=" + toString(sketch.getFloat(0.5)) + " " + toString(sketch.getFloat(1)));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information("upper = " + (upper.isNull() ? "null" : std::to_string(estimateQuantileUpper(upper))));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticBlockTDigest").information("lower = " + (lower.isNull() ? "null" : std::to_string(estimateQuantileUpper(lower))));
    if (!lower.isNull() && !upper.isNull())
        return std::max(std::min(estimateQuantileUpper(upper) - estimateQuantileLower(lower), 1.0), 0.0);
    else if (!lower.isNull())
        return std::max(std::min(1.0 - estimateQuantileLower(lower), 1.0), 0.0);
    else if (!upper.isNull())
        return std::max(std::min(estimateQuantileUpper(upper) - 0.0, 1.0), 0.0);
    else
        return 1.0 - 0.0;
}

MergeTreeColumnDistributionStatisticCollectorBlockTDigest::MergeTreeColumnDistributionStatisticCollectorBlockTDigest(const String & column_name_)
    : column_name(column_name_)
{
}

const String & MergeTreeColumnDistributionStatisticCollectorBlockTDigest::name() const
{
    static String name = "block_tdigest";
    return name;
}

const String & MergeTreeColumnDistributionStatisticCollectorBlockTDigest::column() const
{
    return column_name;
}

bool MergeTreeColumnDistributionStatisticCollectorBlockTDigest::empty() const
{
    return !sketch.has_value();
}

IColumnDistributionStatisticPtr MergeTreeColumnDistributionStatisticCollectorBlockTDigest::getStatisticAndReset()
{
    if (empty())
        throw Exception("BlockTDigest collector is empty", ErrorCodes::LOGICAL_ERROR);
    std::optional<QuantileTDigest<Float32>> res;
    sketch.swap(res);
    res->compress();
    return std::make_shared<MergeTreeColumnDistributionStatisticBlockTDigest>(*std::move(res), column_name);
}

void MergeTreeColumnDistributionStatisticCollectorBlockTDigest::update(const Block & block, size_t * pos, size_t limit)
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

void MergeTreeColumnDistributionStatisticCollectorBlockTDigest::granuleFinished()
{
    // do nothing
}

IColumnDistributionStatisticPtr creatorColumnDistributionStatisticBlockTDigest(
    const StatisticDescription & stat, const ColumnDescription & column)
{
    validatorColumnDistributionStatisticBlockTDigest(stat, column);
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column.name) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column.name);
    return std::make_shared<MergeTreeColumnDistributionStatisticBlockTDigest>(column.name);
}

IMergeTreeColumnDistributionStatisticCollectorPtr creatorColumnDistributionStatisticCollectorBlockTDigest(
    const StatisticDescription & stat, const ColumnDescription & column)
{
    validatorColumnDistributionStatisticBlockTDigest(stat, column);
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column.name) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column.name);
    return std::make_shared<MergeTreeColumnDistributionStatisticCollectorBlockTDigest>(column.name);
}

void validatorColumnDistributionStatisticBlockTDigest(
    const StatisticDescription &, const ColumnDescription & column)
{
    if (!column.type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic BlockTDigest can be used only for numeric columns.");
    if (column.type->isNullable())
        throw Exception(ErrorCodes::INCORRECT_QUERY, "Statistic BlockTDigest can be used only for not nullable columns.");
}

}
