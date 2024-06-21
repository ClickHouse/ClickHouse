#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Statistics/CMSketchStatistics.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}


CMSketchStatistics::CMSketchStatistics(const SingleStatisticsDescription & stat_, DataTypePtr data_type_)
    : IStatistics(stat_), data(CMSKETCH_HASH_COUNT, CMSKETCH_BUCKET_COUNT), data_type(data_type_)
{
}

Float64 CMSketchStatistics::estimateEqual(const Field & value) const
{
    if (auto float_val = getFloat64(value))
        return data.get_estimate(&float_val.value(), 8);
    if (auto string_val = getString(value))
        return data.get_estimate(string_val->data(), string_val->size());
    UNREACHABLE();
}

void CMSketchStatistics::serialize(WriteBuffer & buf)
{
    auto bytes = data.serialize();
    writeIntBinary(static_cast<UInt64>(bytes.size()), buf);
    buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void CMSketchStatistics::deserialize(ReadBuffer & buf)
{
    UInt64 size;
    readIntBinary(size, buf);
    String s;
    s.reserve(size);
    buf.readStrict(s.data(), size); /// Extra copy can be avoided by implementing count_min_sketch<Float64>::deserialize with ReadBuffer
    auto read_sketch = datasketches::count_min_sketch<Float64>::deserialize(s.data(), size, datasketches::DEFAULT_SEED);
    data.merge(read_sketch);
}

void CMSketchStatistics::update(const ColumnPtr & column)
{
    size_t size = column->size();

    for (size_t i = 0; i < size; ++i)
    {
        Field f;
        column->get(i, f);
        if (f.isNull())
            continue;
        if (auto float_val = getFloat64(f))
            data.update(&float_val.value(), 8, 1.0);
        else if (auto string_val = getString(f))
            data.update(*string_val, 1.0);
    }
}

void CMSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber() && !isStringOrFixedString(data_type))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'cmsketch' does not support type {}", data_type->getName());
}

StatisticsPtr CMSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<CMSketchStatistics>(stat, data_type);
}

}

#endif
