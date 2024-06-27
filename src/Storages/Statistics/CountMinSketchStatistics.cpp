#include <Storages/Statistics/CountMinSketchStatistics.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_STATISTICS;
}


CountMinSketchStatistics::CountMinSketchStatistics(const SingleStatisticsDescription & stat_, DataTypePtr data_type_)
    : IStatistics(stat_)
    , sketch(HASH_COUNT, BUCKET_COUNT)
    , data_type(data_type_)
{
}

Float64 CountMinSketchStatistics::estimateEqual(const Field & value) const
{
    if (auto float_val = IStatistics::getFloat64(value))
        return sketch.get_estimate(&float_val.value(), 8);
    if (auto string_val = IStatistics::getString(value))
        return sketch.get_estimate(string_val->data(), string_val->size());
    UNREACHABLE();
}

bool CountMinSketchStatistics::checkType(const Field & f)
{
    switch (f.getType())
    {
        case Field::Types::Int64:
        case Field::Types::UInt64:
        case Field::Types::Float64:
        case Field::Types::String:
            return true;
        default:
            return false;
    }
}

void CountMinSketchStatistics::serialize(WriteBuffer & buf)
{
    Sketch::vector_bytes bytes = sketch.serialize();
    writeIntBinary(static_cast<UInt64>(bytes.size()), buf);
    buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void CountMinSketchStatistics::deserialize(ReadBuffer & buf)
{
    UInt64 size;
    readIntBinary(size, buf);

    Sketch::vector_bytes bytes;
    bytes.reserve(size);
    buf.readStrict(reinterpret_cast<char *>(bytes.data()), size);

    sketch = datasketches::count_min_sketch<Float64>::deserialize(bytes.data(), size, datasketches::DEFAULT_SEED);
}

void CountMinSketchStatistics::update(const ColumnPtr & column)
{
    size_t size = column->size();

    for (size_t i = 0; i < size; ++i)
    {
        Field f;
        column->get(i, f);
        if (checkType(f))
        {
            if (auto float_val = IStatistics::getFloat64(f))
                sketch.update(&float_val.value(), 8, 1.0);
            else if (auto string_val = IStatistics::getString(f))
                sketch.update(*string_val, 1.0);
        }
    }
}

void CountMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber() && !isStringOrFixedString(data_type))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'countmin' does not support type {}", data_type->getName());
}

StatisticsPtr CountMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<CountMinSketchStatistics>(stat, data_type);
}

}

#endif
