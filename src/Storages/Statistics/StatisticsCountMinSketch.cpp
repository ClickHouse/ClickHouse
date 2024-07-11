#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/Statistics/StatisticsCountMinSketch.h>
#include <Interpreters/convertFieldToType.h>
#include <DataTypes/FieldToDataType.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

/// Constants chosen based on rolling dices, which provides an error tolerance of 0.1% (ε = 0.001) and a confidence level of 99.9% (δ = 0.001).
/// And sketch the size is 152kb.
static constexpr auto num_hashes = 7uz;
static constexpr auto num_buckets = 2718uz;

StatisticsCountMinSketch::StatisticsCountMinSketch(const SingleStatisticsDescription & stat_, DataTypePtr data_type_)
    : IStatistics(stat_), sketch(num_hashes, num_buckets), data_type(data_type_)
{
}

Float64 StatisticsCountMinSketch::estimateEqual(const Field & val) const
{
    if (data_type->isValueRepresentedByNumber())
    {
        /// 'val' maybe number or string, method 'convertFieldToType' will
        ///     1. convert string to number, date, datetime, IPv4, Decimal etc
        ///     2. return null if val larger than the range of data_type
        auto val_converted = convertFieldToType(val, *data_type);
        if (val_converted.isNull())
            return 0;

        /// We will get the proper data type of val_converted, for example, Int8 for 1, Int16 for 257.
        auto data_type_converted = applyVisitor(FieldToDataType<LeastSupertypeOnError::Null>(), val_converted);
        DataTypes data_types = {data_type, data_type_converted};
        auto super_type = tryGetLeastSupertype(data_types);

        /// If data_type is UInt8 but val_typed is UInt16, we should return 0.
        if (!super_type->equals(*data_type))
            return 0;

        return sketch.get_estimate(&val_converted, data_type->getSizeOfValueInMemory());
    }

    if (isStringOrFixedString(data_type))
    {
        return sketch.get_estimate(val.get<String>());
    }

    throw Exception(
        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
        "Statistics 'count_min' does not support estimate constant value of type {}",
        val.getTypeName());
}

void StatisticsCountMinSketch::update(const ColumnPtr & column)
{
    size_t size = column->size();
    for (size_t row = 0; row < size; ++row)
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

}

#endif


namespace DB
{

namespace ErrorCodes
{
extern const int FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME;
extern const int ILLEGAL_STATISTICS;
}

void CountMinSketchValidator(const SingleStatisticsDescription &, DataTypePtr data_type)
{
    data_type = removeNullable(data_type);
    data_type = removeLowCardinalityAndNullable(data_type);
    /// Data types of Numeric, String family, IPv4, IPv6, Date family, Enum family are supported.
    if (!data_type->isValueRepresentedByNumber() && !isStringOrFixedString(data_type))
        throw Exception(ErrorCodes::ILLEGAL_STATISTICS, "Statistics of type 'count_min' does not support type {}", data_type->getName());
}

StatisticsPtr CountMinSketchCreator(const SingleStatisticsDescription & stat, DataTypePtr data_type)
{
#if USE_DATASKETCHES
    return std::make_shared<StatisticsCountMinSketch>(stat, data_type);
#else
    throw Exception(ErrorCodes::FEATURE_IS_NOT_ENABLED_AT_BUILD_TIME, "Statistics of type 'count_min' is not supported in this build, to enable it turn on USE_DATASKETCHES when building.");
#endif
}

}
