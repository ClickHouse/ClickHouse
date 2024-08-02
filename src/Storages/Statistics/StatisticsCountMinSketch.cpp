
#include <Storages/Statistics/StatisticsCountMinSketch.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/convertFieldToType.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
extern const int ILLEGAL_STATISTICS;
}

/// Constants chosen based on rolling dices.
/// The values provides:
///     1. an error tolerance of 0.1% (ε = 0.001)
///     2. a confidence level of 99.9% (δ = 0.001).
/// And sketch the size is 152kb.
static constexpr auto num_hashes = 7uz;
static constexpr auto num_buckets = 2718uz;

StatisticsCountMinSketch::StatisticsCountMinSketch(const SingleStatisticsDescription & stat_, DataTypePtr data_type_)
    : IStatistics(stat_)
    , sketch(num_hashes, num_buckets)
    , data_type(data_type_)
{
}

Float64 StatisticsCountMinSketch::estimateEqual(const Field & val) const
{
    /// Try to convert field to data_type. Converting string to proper data types such as: number, date, datetime, IPv4, Decimal etc.
    /// Return null if val larger than the range of data_type
    ///
    /// For example: if data_type is Int32:
    ///     1. For 1.0, 1, '1', return Field(1)
    ///     2. For 1.1, max_value_int64, return null
    Field val_converted = convertFieldToType(val, *data_type);
    if (val_converted.isNull())
        return 0;

    if (data_type->isValueRepresentedByNumber())
        return sketch.get_estimate(&val_converted, data_type->getSizeOfValueInMemory());

    if (isStringOrFixedString(data_type))
        return sketch.get_estimate(val.get<String>());

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistics 'count_min' does not support estimate data type of {}", data_type->getName());
}

void StatisticsCountMinSketch::update(const ColumnPtr & column)
{
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;
        auto data = column->getDataAt(row);
        sketch.update(data.data, data.size, 1);
    }
}

void StatisticsCountMinSketch::serialize(WriteBuffer & buf)
{
    Sketch::vector_bytes bytes = sketch.serialize();
    writeIntBinary(static_cast<UInt64>(bytes.size()), buf);
    buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void StatisticsCountMinSketch::deserialize(ReadBuffer & buf)
{
    UInt64 size;
    readIntBinary(size, buf);

    Sketch::vector_bytes bytes;
    bytes.resize(size); /// To avoid 'container-overflow' in AddressSanitizer checking
    buf.readStrict(reinterpret_cast<char *>(bytes.data()), size);

    sketch = Sketch::deserialize(bytes.data(), size);
}


void countMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    if (!data_type->isValueRepresentedByNumber() && !isStringOrFixedString(data_type))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'count_min' does not support type {}", data_type->getName());
}

StatisticsPtr countMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
    return std::make_shared<StatisticsCountMinSketch>(stat, data_type);
}

}

#endif
