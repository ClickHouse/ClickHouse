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
    extern const int ILLEGAL_STATISTICS;
    extern const int LOGICAL_ERROR;
}

/// Constants chosen based on rolling dices.
/// The values provides:
///     1. an error tolerance of 0.1% (ε = 0.001)
///     2. a confidence level of 99.9% (δ = 0.001).
/// And sketch the size is 152kb.
static constexpr auto num_hashes = 7uz;
static constexpr auto num_buckets = 2718uz;

/// The sizes of the sketches that `serialize` writes: two preamble longs, then, unless the sketch is empty,
/// the total weight and the counters.
static constexpr UInt64 serialized_size_empty = 2 * sizeof(UInt64);
static constexpr UInt64 serialized_size_full = serialized_size_empty + (1 + num_hashes * num_buckets) * sizeof(UInt64);

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
    for (size_t row = 0; row < column->size(); ++row)
    {
        if (column->isNullAt(row))
            continue;
        auto data = column->getDataAt(row);
        sketch.update(data.data(), data.size(), 1);
    }
}

size_t StatisticsCountMinSketch::allocatedBytes() const
{
    /// The counters, one row of buckets per hash function, and the seeds of the hash functions.
    const size_t sketch_num_hashes = sketch.get_num_hashes();
    return sizeof(*this) + (sketch_num_hashes * sketch.get_num_buckets() + sketch_num_hashes) * sizeof(UInt64);
}

void StatisticsCountMinSketch::merge(const StatisticsPtr & other_stats)
{
    const StatisticsCountMinSketch * other = typeid_cast<const StatisticsCountMinSketch *>(other_stats.get());
    sketch.merge(other->sketch);
}

void StatisticsCountMinSketch::serialize(WriteBuffer & buf)
{
    const size_t sketch_size = sketch.get_serialized_size_bytes();
    writeIntBinary(static_cast<UInt64>(sketch_size), buf);

    /// Stream the sketch directly into the destination buffer, avoiding an intermediate copy.
    const size_t written = sketch.serialize_to(
        [&buf](const void * data, size_t size) { buf.write(static_cast<const char *>(data), size); });

    if (written != sketch_size)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Statistics 'countmin' serialization wrote {} bytes but declared {} bytes",
            written, sketch_size);
}

std::optional<UInt64> StatisticsCountMinSketch::getSerializedSize() const
{
    return sizeof(UInt64) + sketch.get_serialized_size_bytes();
}

void StatisticsCountMinSketch::deserialize(ReadBuffer & buf, StatisticsFileVersion /*version*/)
{
    UInt64 size = 0;
    readIntBinary(size, buf);

    /// Check the size before allocating: a garbage size made `resize` throw `std::length_error` (#91206).
    /// `datasketches` also does not fully check the size of the buffer, so a truncated sketch could be read out of bounds.
    if (size != serialized_size_empty && size != serialized_size_full)
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Statistics 'countmin' has serialized size {}, expected {} or {}; rebuild with ALTER TABLE ... MATERIALIZE STATISTICS.",
            size,
            serialized_size_empty,
            serialized_size_full);

    Sketch::vector_bytes bytes;
    bytes.resize(size); /// To avoid 'container-overflow' in AddressSanitizer checking
    buf.readStrict(reinterpret_cast<char *>(bytes.data()), size);

    try
    {
        Sketch loaded = Sketch::deserialize(bytes.data(), size);

        /// Other dimensions cannot be merged with the sketches of other parts, and some of them parse into a sketch
        /// with fewer counters than `num_hashes` x `num_buckets` (their product overflows 32 bits).
        if (loaded.get_num_hashes() != num_hashes || loaded.get_num_buckets() != num_buckets)
            throw Exception(
                ErrorCodes::ILLEGAL_STATISTICS,
                "Statistics 'countmin' has {} hashes and {} buckets, expected {} and {}; "
                "rebuild with ALTER TABLE ... MATERIALIZE STATISTICS.",
                static_cast<UInt32>(loaded.get_num_hashes()),
                loaded.get_num_buckets(),
                num_hashes,
                num_buckets);

        sketch = std::move(loaded);
    }
    catch (const Exception &)
    {
        throw;
    }
    catch (const std::bad_alloc &)
    {
        throw;
    }
    catch (const std::exception & e)
    {
        /// `datasketches` throws `std::invalid_argument` and `std::out_of_range` on malformed input. Translate them:
        /// an escaping `std::logic_error` loses the part and column context and aborts debug and sanitizer builds.
        throw Exception(
            ErrorCodes::ILLEGAL_STATISTICS,
            "Cannot deserialize statistics 'countmin': {}; rebuild with ALTER TABLE ... MATERIALIZE STATISTICS.",
            e.what());
    }
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
