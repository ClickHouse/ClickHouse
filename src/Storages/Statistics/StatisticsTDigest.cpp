#include <Storages/Statistics/StatisticsTDigest.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
extern const int LOGICAL_ERROR;
}

StatisticsTDigest::StatisticsTDigest(const SingleStatisticsDescription & stat_)
    : IStatistics(stat_)
{
}

void StatisticsTDigest::update(const ColumnPtr & column)
{
    size_t rows = column->size();
    for (size_t row = 0; row < rows; ++row)
    {
        Field field;
        column->get(row, field);

        if (field.isNull())
            continue;

        if (auto field_as_float = StatisticsUtils::tryConvertToFloat64(field))
            t_digest.add(*field_as_float, 1);
    }
}

void StatisticsTDigest::serialize(WriteBuffer & buf)
{
    t_digest.serialize(buf);
}

void StatisticsTDigest::deserialize(ReadBuffer & buf)
{
    t_digest.deserialize(buf);
}

Float64 StatisticsTDigest::estimateLess(const Field & val) const
{
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val);
    if (val_as_float)
        return t_digest.getCountLessThan(*val_as_float);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistics 'tdigest' does not support estimating value of type {}", val.getTypeName());
}

Float64 StatisticsTDigest::estimateEqual(const Field & val) const
{
    auto val_as_float = StatisticsUtils::tryConvertToFloat64(val);
    if (val_as_float)
        return t_digest.getCountEqual(*val_as_float);
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistics 'tdigest' does not support estimating value of type {}", val.getTypeName());
}

void tdigestValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'tdigest' do not support type {}", data_type->getName());
}

StatisticsPtr tdigestCreator(const SingleStatisticsDescription & stat, DataTypePtr)
{
    return std::make_shared<StatisticsTDigest>(stat);
}

}
