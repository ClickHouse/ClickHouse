#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Statistics/CountMinSketchStatistics.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}


CountMinSketchStatistics::CountMinSketchStatistics(const SingleStatisticsDescription & stat_, DataTypePtr data_type_)
    : IStatistics(stat_), data(HASH_COUNT, BUCKET_COUNT), data_type(data_type_)
{
}

Float64 CountMinSketchStatistics::estimateEqual(const Field & value) const
{
    if (auto float_val = getFloat64(value))
        return data.get_estimate(&float_val.value(), 8);
    if (auto string_val = getString(value))
        return data.get_estimate(string_val->data(), string_val->size());
    UNREACHABLE();
}

void CountMinSketchStatistics::serialize(WriteBuffer & buf)
{
    auto bytes = data.serialize();
    writeIntBinary(static_cast<UInt64>(bytes.size()), buf);
    buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void CountMinSketchStatistics::deserialize(ReadBuffer & buf)
{
    UInt64 size;
    readIntBinary(size, buf);
    String s;
    s.reserve(size);
    buf.readStrict(s.data(), size); /// Extra copy can be avoided by implementing count_min_sketch<Float64>::deserialize with ReadBuffer
    auto read_sketch = datasketches::count_min_sketch<Float64>::deserialize(s.data(), size, datasketches::DEFAULT_SEED);
    data.merge(read_sketch);
}

void CountMinSketchStatistics::update(const ColumnPtr & column)
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

void CountMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber() && !isStringOrFixedString(data_type))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'countminsketch' does not support type {}", data_type->getName());
}

StatisticsPtr CountMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<CountMinSketchStatistics>(stat, data_type);
}

}

#endif
