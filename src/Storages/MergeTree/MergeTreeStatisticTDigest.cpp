#include <memory>
#include <Storages/MergeTree/MergeTreeStatisticTDigest.h>
#include <Common/Exception.h>

namespace DB
{

namespace
{
constexpr float EPS = 1e-7;
}

MergeTreeColumnDistributionStatisticTDigest::MergeTreeColumnDistributionStatisticTDigest(
    const String & column_name_)
    : column_name({column_name_})
    , is_empty(true)
{
}

MergeTreeColumnDistributionStatisticTDigest::MergeTreeColumnDistributionStatisticTDigest(
    QuantileTDigest<Float32>&& sketch_, const String & column_name_)
    : column_name({column_name_})
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

const Names& MergeTreeColumnDistributionStatisticTDigest::getColumnsRequiredForStatisticCalculation() const
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
    is_empty = false;
}

double MergeTreeColumnDistributionStatisticTDigest::estimateQuantileLower(const Field& value) const
{
    if (empty())
        throw Exception("TDigest is empty", ErrorCodes::LOGICAL_ERROR);
    // TODO: normal implementation using t-digest sketch centroids
    // TODO: try ddsketch???
    constexpr auto step = 0.01;
    float answer = 0;
    float cur = 0;
    while (cur < 1) {
        if (sketch.getFloat(cur) + EPS < value.get<float>())
        {
            answer = cur;
        }
        cur += step;
    }
    return answer;
}

double MergeTreeColumnDistributionStatisticTDigest::estimateQuantileUpper(const Field& value) const
{
    if (empty())
        throw Exception("TDigest is empty", ErrorCodes::LOGICAL_ERROR);

    constexpr auto step = 0.01;
    float cur = 0;
    while (cur < 1) {
        if (sketch.getFloat(cur) > value.get<float>() + EPS)
        {
            return cur;
        }
        cur += step;
    }
    return 1.0;
}

double MergeTreeColumnDistributionStatisticTDigest::estimateProbability(const Field& lower, const Field& upper) const
{
    // lower <= value <= upper
    // null = infty
    if (lower.isNull() && upper.isNull())
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

bool MergeTreeColumnDistributionStatisticCollectorTDigest::empty() const
{
    return !sketch.has_value();
}

IMergeTreeColumnDistributionStatisticPtr MergeTreeColumnDistributionStatisticCollectorTDigest::getStatisticAndReset()
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

    size_t rows_read = std::min(limit, block.rows() - *pos);
    const auto & column_with_type = block.getByName(column_name);
    const auto & column = column_with_type.column;
    for (size_t i = 0; i < rows_read; ++i, ++(*pos))
    {
        // support only numeric columns
        auto value = column->getFloat32(*pos);
        sketch->add(value);
    }
}

void MergeTreeColumnDistributionStatisticCollectorTDigest::granuleFinished()
{
    // do nothing
}

}
