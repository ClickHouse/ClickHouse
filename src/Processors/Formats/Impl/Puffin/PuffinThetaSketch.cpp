#include <Processors/Formats/Impl/Puffin/PuffinThetaSketch.h>
#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>

#include "config.h"

#include <Core/Block.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>

#if USE_DATASKETCHES
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/castColumn.h>
#include <base/unaligned.h>

#include <theta_sketch.hpp>
#include <theta_union.hpp>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

#if USE_DATASKETCHES

constexpr UInt32 ICEBERG_TIMESTAMP_SCALE = 6;

std::optional<DataTypePtr> getCastType(const DataTypePtr & type)
{
    switch (type->getTypeId())
    {
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Date32:
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::UUID:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return DataTypePtr{};
        case TypeIndex::UInt8:
            if (isBool(type))
                return DataTypePtr{};
            return std::make_shared<DataTypeInt32>();
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            return std::make_shared<DataTypeInt32>();
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
            return std::make_shared<DataTypeInt64>();
        case TypeIndex::DateTime:
            return std::make_shared<DataTypeDateTime64>(ICEBERG_TIMESTAMP_SCALE);
        case TypeIndex::DateTime64:
        {
            const auto scale = assert_cast<const DataTypeDateTime64 &>(*type).getScale();
            if (scale > ICEBERG_TIMESTAMP_SCALE)
                return {};
            if (scale == ICEBERG_TIMESTAMP_SCALE)
                return DataTypePtr{};
            return std::make_shared<DataTypeDateTime64>(ICEBERG_TIMESTAMP_SCALE);
        }
        default:
            return {};
    }
}

template <typename NativeType>
size_t storeMinimalBigEndian(char * buf, NativeType value)
{
    UInt8 bytes[sizeof(NativeType)];
    for (size_t i = 0; i < sizeof(NativeType); ++i)
        bytes[sizeof(NativeType) - 1 - i] = static_cast<UInt8>(static_cast<UInt64>((value >> (8 * i)) & NativeType(0xFF)));

    size_t first = 0;
    while (first + 1 < sizeof(NativeType)
           && ((bytes[first] == 0x00 && (bytes[first + 1] & 0x80) == 0) || (bytes[first] == 0xFF && (bytes[first + 1] & 0x80) != 0)))
        ++first;

    const size_t size = sizeof(NativeType) - first;
    memcpy(buf, bytes + first, size);
    return size;
}

template <typename Decimal>
ColumnPtr encodeDecimals(const IColumn & column)
{
    const auto & data = assert_cast<const ColumnDecimal<Decimal> &>(column).getData();
    auto result = ColumnString::create();
    result->reserve(data.size());
    char buf[sizeof(typename Decimal::NativeType)];
    for (const auto & value : data)
        result->insertData(buf, storeMinimalBigEndian(buf, value.value));
    return result;
}

ColumnPtr encodeUUIDs(const IColumn & column)
{
    const auto & data = assert_cast<const ColumnUUID &>(column).getData();
    auto result = ColumnFixedString::create(sizeof(UUID));
    auto & chars = result->getChars();
    chars.resize(data.size() * sizeof(UUID));
    for (size_t i = 0; i < data.size(); ++i)
    {
        unalignedStoreBigEndian<UInt64>(&chars[i * sizeof(UUID)], UUIDHelpers::getHighBytes(data[i]));
        unalignedStoreBigEndian<UInt64>(&chars[i * sizeof(UUID) + sizeof(UInt64)], UUIDHelpers::getLowBytes(data[i]));
    }
    return result;
}

#endif

}

#if USE_DATASKETCHES

struct PuffinColumnThetaSketch::Impl
{
    Impl(DataTypePtr type_, DataTypePtr cast_to_)
        : type(std::move(type_))
        , cast_to(std::move(cast_to_))
        , sketch(datasketches::update_theta_sketch::builder().build())
    {
    }

    [[nodiscard]] ColumnPtr encode(const ColumnPtr & column) const
    {
        if (cast_to)
            return castColumn({column, type, ""}, cast_to);
        switch (type->getTypeId())
        {
            case TypeIndex::Decimal32: return encodeDecimals<Decimal32>(*column);
            case TypeIndex::Decimal64: return encodeDecimals<Decimal64>(*column);
            case TypeIndex::Decimal128: return encodeDecimals<Decimal128>(*column);
            case TypeIndex::Decimal256: return encodeDecimals<Decimal256>(*column);
            case TypeIndex::UUID: return encodeUUIDs(*column);
            default: return column;
        }
    }

    void update(const ColumnPtr & column, const NullMap * null_map)
    {
        const auto encoded = encode(column);
        for (size_t i = 0; i < encoded->size(); ++i)
        {
            if (null_map && (*null_map)[i])
                continue;
            const auto value = encoded->getDataAt(i);
            if (!value.empty())
                sketch.update(value.data(), value.size());
        }
    }

    [[nodiscard]] datasketches::compact_theta_sketch compact() const
    {
        auto sketch_union = datasketches::theta_union::builder().build();
        sketch_union.update(sketch);
        return sketch_union.get_result();
    }

    DataTypePtr type;
    DataTypePtr cast_to;
    datasketches::update_theta_sketch sketch;
};

bool PuffinColumnThetaSketch::isSupportedType(const DataTypePtr & type)
{
    return getCastType(removeNullable(removeLowCardinality(type))).has_value();
}

PuffinColumnThetaSketch::PuffinColumnThetaSketch(const DataTypePtr & type_)
{
    auto type = removeNullable(removeLowCardinality(type_));
    auto cast_to = getCastType(type);
    if (!cast_to)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "Cannot build a Theta sketch for type {}: it has no Iceberg single-value serialization", type_->getName());
    impl = std::make_unique<Impl>(std::move(type), std::move(*cast_to));
}

void PuffinColumnThetaSketch::update(const IColumn & column)
{
    const ColumnPtr full_column = column.convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
    ColumnPtr nested = full_column;
    const NullMap * null_map = nullptr;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(full_column.get()))
    {
        null_map = &nullable->getNullMapData();
        nested = nullable->getNestedColumnPtr();
    }
    impl->update(nested, null_map);
}

namespace
{

PuffinColumnThetaSketch::Serialized serializeCompact(const datasketches::compact_theta_sketch & compact)
{
    const auto bytes = compact.serialize();
    PuffinColumnThetaSketch::Serialized result{
        .bytes = String(reinterpret_cast<const char *>(bytes.data()), bytes.size()), .ndv = static_cast<UInt64>(compact.get_estimate())};
    if (static_cast<UInt8>(result.bytes[0]) >= 2)
        unalignedStoreLittleEndian<Float32>(result.bytes.data() + 12, 1.0f);
    return result;
}

datasketches::compact_theta_sketch deserializeThetaSketch(std::string_view bytes)
{
    try
    {
        auto sketch = datasketches::compact_theta_sketch::deserialize(bytes.data(), bytes.size());
        /// A zero theta, or hashes not below theta, give an estimate the UInt64 `ndv` property cannot hold.
        const double estimate = sketch.get_estimate();
        if (!(estimate < 0x1p64))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid Theta sketch: theta {}, {} hashes, estimate {}",
                sketch.get_theta64(), sketch.get_num_retained(), estimate);
        return sketch;
    }
    catch (const std::logic_error & e)
    {
        /// `datasketches` throws `std::logic_error` on malformed input, which `tryLogCurrentException` reports as a bug.
        throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot deserialize Theta sketch: {}", e.what());
    }
}

}

PuffinColumnThetaSketch::Serialized PuffinColumnThetaSketch::serialize() const
{
    return serializeCompact(impl->compact());
}

PuffinColumnThetaSketch::Serialized unionThetaSketches(const VectorWithMemoryTracking<std::string_view> & sketches)
{
    /// A union keeps at most the default number of hashes, which would drop precision from a single larger sketch.
    if (sketches.size() == 1)
        return serializeCompact(deserializeThetaSketch(sketches.front()));
    auto sketch_union = datasketches::theta_union::builder().build();
    for (const auto bytes : sketches)
        sketch_union.update(deserializeThetaSketch(bytes));
    return serializeCompact(sketch_union.get_result());
}

void PuffinColumnThetaSketch::reset()
{
    impl->sketch.reset();
}

#else

struct PuffinColumnThetaSketch::Impl
{
};

bool PuffinColumnThetaSketch::isSupportedType(const DataTypePtr &)
{
    return false;
}

PuffinColumnThetaSketch::PuffinColumnThetaSketch(const DataTypePtr &)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Theta sketches are disabled, ClickHouse was built without DataSketches");
}

void PuffinColumnThetaSketch::update(const IColumn &)
{
}

PuffinColumnThetaSketch::Serialized PuffinColumnThetaSketch::serialize() const
{
    return {};
}

PuffinColumnThetaSketch::Serialized unionThetaSketches(const VectorWithMemoryTracking<std::string_view> &)
{
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Theta sketches are disabled, ClickHouse was built without DataSketches");
}

void PuffinColumnThetaSketch::reset()
{
}

#endif

PuffinColumnThetaSketch::PuffinColumnThetaSketch(PuffinColumnThetaSketch && other) noexcept = default;
PuffinColumnThetaSketch & PuffinColumnThetaSketch::operator=(PuffinColumnThetaSketch && other) noexcept = default;
PuffinColumnThetaSketch::~PuffinColumnThetaSketch() = default;

PuffinBlob makeThetaSketchBlobMetadata(Int32 field_id, Int64 snapshot_id, Int64 sequence_number, UInt64 ndv)
{
    PuffinBlob blob;
    blob.type = PUFFIN_DATASKETCHES_THETA_BLOB_TYPE;
    blob.fields = {field_id};
    blob.snapshot_id = snapshot_id;
    blob.sequence_number = sequence_number;
    blob.properties.emplace("ndv", toString(ndv));
    return blob;
}

#if USE_DATASKETCHES

namespace
{

void checkThetaSketchColumnType(const String & column_name, const DataTypePtr & type)
{
    if (PuffinColumnThetaSketch::isSupportedType(type))
        return;

    const auto nested_type = removeNullable(removeLowCardinality(type));
    if (const auto * datetime64 = typeid_cast<const DataTypeDateTime64 *>(nested_type.get());
        datetime64 && datetime64->getScale() > ICEBERG_TIMESTAMP_SCALE)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Cannot write a Theta sketch for column {} of type {}: Iceberg timestamps have microsecond precision, "
            "cast the column to DateTime64(6)",
            column_name,
            type->getName());

    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Cannot write a Theta sketch for column {} of type {}: the type has no Iceberg single-value serialization",
        column_name,
        type->getName());
}

class PuffinThetaSketchBuilder final : public IPuffinBlobBuilder
{
public:
    PuffinThetaSketchBuilder(const Block & header, PuffinThetaSketchBlobOptions options_)
        : options(std::move(options_))
    {
        if (options.field_ids.size() != header.columns())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Got {} field ids for {} columns of Theta sketches: {}",
                options.field_ids.size(),
                header.columns(),
                header.dumpNames());

        sketches.reserve(header.columns());
        for (const auto & column : header)
        {
            checkThetaSketchColumnType(column.name, column.type);
            sketches.emplace_back(column.type);
        }
    }

    void consume(const Chunk & chunk) override
    {
        const auto & columns = chunk.getColumns();
        for (size_t i = 0; i < columns.size(); ++i)
            sketches[i].update(*columns[i]);
    }

    void write(PuffinWriter & writer) override
    {
        for (size_t i = 0; i < sketches.size(); ++i)
        {
            const auto serialized = sketches[i].serialize();
            writer.addBlob(
                makeThetaSketchBlobMetadata(options.field_ids[i], options.snapshot_id, options.sequence_number, serialized.ndv),
                serialized.bytes);
        }
    }

    void clear() override
    {
        for (auto & sketch : sketches)
            sketch.reset();
    }

private:
    VectorWithMemoryTracking<PuffinColumnThetaSketch> sketches;
    PuffinThetaSketchBlobOptions options;
};

}

#endif

PuffinBlobBuilderPtr createPuffinThetaSketchBuilder(const Block & header, PuffinThetaSketchBlobOptions options)
{
    if (header.columns() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Writing Theta sketches requires at least one column");
#if USE_DATASKETCHES
    return std::make_unique<PuffinThetaSketchBuilder>(header, std::move(options));
#else
    UNUSED(options);
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED, "Writing Theta sketches to Puffin files is disabled, ClickHouse was built without DataSketches");
#endif
}

}
