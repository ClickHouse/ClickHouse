#include <Storages/Statistics/StatisticsCountMinSketch.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/HashTable/HashMap.h>
#include <Common/assert_cast.h>

#if USE_DATASKETCHES

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Constants chosen based on rolling dices.
/// The values provides:
///     1. an error tolerance of 0.1% (ε = 0.001)
///     2. a confidence level of 99.9% (δ = 0.001).
/// And sketch the size is 152kb.
static constexpr auto num_hashes = 7uz;
static constexpr auto num_buckets = 2718uz;

namespace
{

using DictionaryCounts = ColumnUInt64::Container;

void updateSketchFromDictionaryCounts(
    datasketches::count_min_sketch<UInt64> & sketch,
    const IColumnUnique & dictionary,
    const DictionaryCounts & counts)
{
    for (size_t dictionary_row = 0, size = counts.size(); dictionary_row < size; ++dictionary_row)
    {
        UInt64 frequency = counts[dictionary_row];
        if (frequency == 0 || dictionary.isNullAt(dictionary_row))
            continue;

        auto data = dictionary.getDataAt(dictionary_row);
        sketch.update(data.data(), data.size(), frequency);
    }
}

using DictionaryIndexCounts = HashMap<UInt64, UInt64>;

template <typename IndexColumn>
void countTouchedDictionaryIndexes(const IColumn & indexes_column, DictionaryIndexCounts & counts)
{
    const auto & indexes = assert_cast<const IndexColumn &>(indexes_column).getData();
    for (auto index : indexes)
        ++counts[index];
}

template <typename Sketch>
void updateSketchFromTouchedDictionaryIndexes(Sketch & sketch, const ColumnLowCardinality & column)
{
    DictionaryIndexCounts counts;
    const IColumn & indexes = column.getIndexes();

    switch (column.getSizeOfIndexType())
    {
        case sizeof(UInt8): countTouchedDictionaryIndexes<ColumnUInt8>(indexes, counts); break;
        case sizeof(UInt16): countTouchedDictionaryIndexes<ColumnUInt16>(indexes, counts); break;
        case sizeof(UInt32): countTouchedDictionaryIndexes<ColumnUInt32>(indexes, counts); break;
        case sizeof(UInt64): countTouchedDictionaryIndexes<ColumnUInt64>(indexes, counts); break;
        default: throwUnexpectedLowCardinalityIndexType(column.getSizeOfIndexType());
    }

    const auto & dictionary = column.getDictionary();
    for (const auto & count : counts)
    {
        UInt64 dictionary_row = count.getKey();
        UInt64 frequency = count.getMapped();
        if (dictionary.isNullAt(dictionary_row))
            continue;

        auto data = dictionary.getDataAt(dictionary_row);
        sketch.update(data.data(), data.size(), frequency);
    }
}

}

StatisticsCountMinSketch::StatisticsCountMinSketch(const SingleStatisticsDescription & description, const DataTypePtr & data_type_)
    : IStatistics(description)
    , sketch(num_hashes, num_buckets)
    , data_type(removeLowCardinalityAndNullable(data_type_))
{
}

std::optional<Float64> StatisticsCountMinSketch::estimateEqual(const Field & val) const
{
    /// Coerce the comparison field to data_type (e.g. parse '5' into a number). `val` may have an
    /// unrelated type on paths that do not pre-coerce it, such as `col IN (subquery)`.
    /// No from_type_hint: a hint equal to data_type short-circuits convertFieldToType into a no-op.
    /// The try-variant returns null (-> zero selectivity) instead of throwing on an out-of-range or
    /// unconvertible field, mirroring how TDigest/MinMax guard via tryConvertToFloat64.
    Field val_converted = tryConvertFieldToType(val, *data_type);
    if (val_converted.isNull())
        return 0;

    if (data_type->isValueRepresentedByNumber())
    {
        /// Cannot use &val_converted directly: Field stores small types in a wider NearestFieldType
        /// (e.g. Float32 → Float64, Int8 → Int64), so the bit pattern differs from what the column
        /// stores. Insert into a temporary column to get the same byte representation as build().
        auto temp_col = data_type->createColumn();
        temp_col->insert(val_converted);
        auto data = temp_col->getDataAt(0);
        return static_cast<Float64>(sketch.get_estimate(data.data(), data.size()));
    }

    if (isStringOrFixedString(data_type))
        return static_cast<Float64>(sketch.get_estimate(val_converted.safeGet<String>()));

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Statistics 'countmin' does not support estimate data type of {}", data_type->getName());
}

void StatisticsCountMinSketch::build(const ColumnPtr & column)
{
    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
    {
        const auto & dictionary = column_low_cardinality->getDictionary();
        if (dictionary.size() <= column_low_cardinality->size())
        {
            const auto counts_column = column_low_cardinality->countKeys();
            const auto & counts = assert_cast<const ColumnUInt64 &>(*counts_column).getData();
            updateSketchFromDictionaryCounts(sketch, dictionary, counts);
        }
        else
            updateSketchFromTouchedDictionaryIndexes(sketch, *column_low_cardinality);

        return;
    }

    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;
        auto data = column->getDataAt(row);
        sketch.update(data.data(), data.size(), 1);
    }
}

void StatisticsCountMinSketch::merge(const StatisticsPtr & other_stats)
{
    const StatisticsCountMinSketch * other = typeid_cast<const StatisticsCountMinSketch *>(other_stats.get());
    sketch.merge(other->sketch);
}

void StatisticsCountMinSketch::serialize(WriteBuffer & buf)
{
    Sketch::vector_bytes bytes = sketch.serialize();
    writeIntBinary(static_cast<UInt64>(bytes.size()), buf);
    buf.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
}

void StatisticsCountMinSketch::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    UInt64 size = 0;
    readIntBinary(size, buf);

    Sketch::vector_bytes bytes;
    bytes.resize(size); /// To avoid 'container-overflow' in AddressSanitizer checking
    buf.readStrict(reinterpret_cast<char *>(bytes.data()), size);

    sketch = Sketch::deserialize(bytes.data(), size);
}

bool countMinSketchStatisticsValidator(const SingleStatisticsDescription & /*description*/, const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeLowCardinalityAndNullable(data_type);
    return inner_data_type->isValueRepresentedByNumber() || isStringOrFixedString(inner_data_type);
}

StatisticsPtr countMinSketchStatisticsCreator(const SingleStatisticsDescription & description, const DataTypePtr & data_type)
{
    return std::make_shared<StatisticsCountMinSketch>(description, data_type);
}

}

#endif
