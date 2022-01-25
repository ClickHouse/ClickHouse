#include <cmath>
#include <memory>
#include <string>
#include <Storages/MergeTree/MergeTreeStatisticTDigest.h>
#include <Poco/Logger.h>
#include <Common/Exception.h>

namespace DB
{

MergeTreeColumnDistributionStatisticTDigest::MergeTreeColumnDistributionStatisticTDigest(
    const String & column_name_)
    : column_name(column_name_)
    , is_empty(true)
{
}

MergeTreeColumnDistributionStatisticTDigest::MergeTreeColumnDistributionStatisticTDigest(
    QuantileTDigest<Float32>&& sketch_, const String & column_name_)
    : column_name(column_name_)
    , sketch(std::move(sketch_))
    , is_empty(false)
{
}

const String& MergeTreeColumnDistributionStatisticTDigest::name() const
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
        Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("MERGED emp=" + std::to_string(empty()));
    }
    else
    {
        throw Exception("Unknown distribution sketch type", ErrorCodes::LOGICAL_ERROR);
    }
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information(
            "MERGE: 50% = " + std::to_string(sketch.getFloat(0.5))
            + " 90% = " + std::to_string(sketch.getFloat(0.9))
            + " 1% = " + std::to_string(sketch.getFloat(0.01)));
}

const String& MergeTreeColumnDistributionStatisticTDigest::getColumnsRequiredForStatisticCalculation() const
{
    return column_name;
}

void MergeTreeColumnDistributionStatisticTDigest::serializeBinary(WriteBuffer & ostr) const
{
    sketch.serialize(ostr);
}

void MergeTreeColumnDistributionStatisticTDigest::deserializeBinary(ReadBuffer & istr)
{
    sketch.deserialize(istr);
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information(
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
        throw Exception("Bad type for TDigest", ErrorCodes::LOGICAL_ERROR);
    }
}
}

double MergeTreeColumnDistributionStatisticTDigest::estimateQuantileLower(const Field& value) const
{
    if (empty())
        throw Exception("TDigest is empty", ErrorCodes::LOGICAL_ERROR);
    // TODO: t-digest grows O(log n)
    // TODO: try ddsketch???

    // TODO: process corner cases float32 vs float64
    double threshold = extractValue(value);
    return sketch.cdf(threshold).first;
}

double MergeTreeColumnDistributionStatisticTDigest::estimateQuantileUpper(const Field& value) const
{
    if (empty())
        throw Exception("TDigest is empty", ErrorCodes::LOGICAL_ERROR);

    double threshold = extractValue(value);
    return sketch.cdf(threshold).second;
}

double MergeTreeColumnDistributionStatisticTDigest::estimateProbability(const Field& lower, const Field& upper) const
{
    // lower <= value <= upper
    // null = infty
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("est " + toString(lower) + " " + toString(upper));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("emp=" + toString(empty()));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("sktch50=" + toString(sketch.getFloat(0.5)) + " " + toString(sketch.getFloat(1)));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("upper = " + (upper.isNull() ? "null" : std::to_string(estimateQuantileUpper(upper))));
    Poco::Logger::get("MergeTreeColumnDistributionStatisticTDigest").information("lower = " + (lower.isNull() ? "null" : std::to_string(estimateQuantileUpper(lower))));
    if (!lower.isNull() && !upper.isNull())
        return estimateQuantileUpper(upper) - estimateQuantileLower(lower);
    else if (!lower.isNull())
        return 1.0 - estimateQuantileLower(lower);
    else if (!upper.isNull())
        return estimateQuantileUpper(upper) - 0.0;
    else
        return 1.0 - 0.0;
}

MergeTreeColumnDistributionStatisticCollectorTDigest::MergeTreeColumnDistributionStatisticCollectorTDigest(const String & column_name_)
    : column_name(column_name_)
{
}

const String & MergeTreeColumnDistributionStatisticCollectorTDigest::name() const
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

IColumnDistributionStatisticPtr MergeTreeColumnDistributionStatisticCollectorTDigest::getStatisticAndReset()
{
    if (empty())
        throw Exception("TDigest collector is empty", ErrorCodes::LOGICAL_ERROR);
    std::optional<QuantileTDigest<Float32>> res;
    sketch.swap(res);
    res->compress();
    return std::make_shared<MergeTreeColumnDistributionStatisticTDigest>(*std::move(res), column_name);
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
    //Poco::Logger::get("KEK2").information("COLUMN + " + column_name);
    const auto & column_with_type = block.getByName(column_name);
    const auto & column = column_with_type.column;
    for (size_t i = 0; i < rows_read; ++i)
    {
        // support only numeric columns
        // TODO: process coner cases float32 vs float64
        const auto value = column->getFloat32((*pos) + i);
        sketch->add(value);
    }
    *pos += rows_read;
}

void MergeTreeColumnDistributionStatisticCollectorTDigest::granuleFinished()
{
    // do nothing
}

IColumnDistributionStatisticPtr creatorColumnDistributionStatisticTDigest(
    const StatisticDescription & stat, const String & column)
{
    // TODO: check column is numeric
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column);
    return std::make_shared<MergeTreeColumnDistributionStatisticTDigest>(column);
}

IMergeTreeColumnDistributionStatisticCollectorPtr creatorColumnDistributionStatisticCollectorTDigest(
    const StatisticDescription & stat, const String & column)
{
    if (std::find(std::begin(stat.column_names), std::end(stat.column_names), column) == std::end(stat.column_names))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistic {} hasn't column {}.", stat.name, column);
    return std::make_shared<MergeTreeColumnDistributionStatisticCollectorTDigest>(column);
}

}
