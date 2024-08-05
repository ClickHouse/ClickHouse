#include <Storages/Statistics/StatisticsTDigest.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/FieldVisitorConvertToNumber.h>

namespace DB
{
namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}

StatisticsTDigest::StatisticsTDigest(const SingleStatisticsDescription & statistics_description, DataTypePtr data_type_)
    : IStatistics(statistics_description)
    , data_type(data_type_)
{
}

void StatisticsTDigest::update(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;

        auto data = column->getFloat64(row);
        t_digest.add(data, 1);
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
    Field val_converted = convertFieldToType(val, *data_type);
    if (val_converted.isNull())
        return 0;

    auto val_as_float = applyVisitor(FieldVisitorConvertToNumber<Float64>(), val_converted);
    return t_digest.getCountLessThan(val_as_float);
}

Float64 StatisticsTDigest::estimateEqual(const Field & val) const
{
    Field val_converted = convertFieldToType(val, *data_type);
    if (val_converted.isNull())
        return 0;

    auto val_as_float = applyVisitor(FieldVisitorConvertToNumber<Float64>(), val_converted);
    return t_digest.getCountEqual(val_as_float);
}

void tdigestStatisticsValidator(const SingleStatisticsDescription & /*statistics_description*/, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber())
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'tdigest' do not support type {}", data_type->getName());
}

StatisticsPtr tdigestStatisticsCreator(const SingleStatisticsDescription & statistics_description, DataTypePtr data_type)
{
    return std::make_shared<StatisticsTDigest>(statistics_description, data_type);
}

}
