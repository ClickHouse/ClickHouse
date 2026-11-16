#include <Processors/Formats/Impl/Puffin/PuffinThetaSketch.h>

#include "config.h"

#include <Common/Exception.h>

#if USE_DATASKETCHES

#include <Processors/Formats/Impl/Puffin/PuffinCommon.h>
#include <Processors/Formats/Impl/Puffin/PuffinWriter.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnsDateTime.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesDecimal.h>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Processors/Chunk.h>
#include <base/unaligned.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>

#include <theta_sketch.hpp>
#include <theta_union.hpp>

#include <unordered_set>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

constexpr UInt32 ICEBERG_TIMESTAMP_SCALE = 6;

template <typename ColumnType, typename F>
void forEachValue(const IColumn & column, const NullMap * null_map, F && f)
{
    const auto & data = assert_cast<const ColumnType &>(column).getData();
    for (size_t i = 0; i < data.size(); ++i)
        if (!null_map || !(*null_map)[i])
            f(data[i]);
}

template <typename T>
void storeLittleEndian(char * buf, T value)
{
    unalignedStoreLittleEndian<T>(buf, value);
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

VectorWithMemoryTracking<Int32> parseFieldIds(const String & setting, const Block & header)
{
    if (header.columns() == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Puffin output format requires at least one column to write Theta sketches");

    VectorWithMemoryTracking<Int32> field_ids;
    if (setting.empty())
    {
        for (size_t i = 0; i < header.columns(); ++i)
            field_ids.push_back(static_cast<Int32>(i + 1));
        return field_ids;
    }

    std::vector<String> parts;
    boost::split(parts, setting, boost::is_any_of(","));
    std::unordered_set<Int32> seen;
    for (auto & part : parts)
    {
        boost::trim(part);
        Int32 field_id = 0;
        if (!tryParse(field_id, part) || field_id <= 0)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Setting `output_format_puffin_field_ids` must be a comma-separated list of positive integers, got '{}'",
                setting);
        if (!seen.insert(field_id).second)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `output_format_puffin_field_ids` contains field id {} twice", field_id);
        field_ids.push_back(field_id);
    }

    if (field_ids.size() != header.columns())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Setting `output_format_puffin_field_ids` has {} field ids, but {} columns are written: {}",
            field_ids.size(),
            header.columns(),
            header.dumpNames());
    return field_ids;
}

datasketches::compact_theta_sketch compactThetaSketch(const datasketches::update_theta_sketch & sketch)
{
    auto sketch_union = datasketches::theta_union::builder().build();
    sketch_union.update(sketch);
    return sketch_union.get_result();
}

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

class PuffinThetaSketchBuilder final : public IPuffinBlobBuilder
{
public:
    PuffinThetaSketchBuilder(const Block & header, const FormatSettings & format_settings);

    void consume(const Chunk & chunk) override;
    void write(PuffinWriter & writer) override;
    void reset() override;

private:
    class ColumnSketch
    {
    public:
        ColumnSketch(const String & column_name, const DataTypePtr & type);

        void add(const IColumn & column);
        const datasketches::update_theta_sketch & get() const { return sketch; }
        void reset() { sketch.reset(); }

    private:
        void update(const void * data, size_t size);

        DataTypePtr type;
        datasketches::update_theta_sketch sketch;
    };

    VectorWithMemoryTracking<ColumnSketch> sketches;
    VectorWithMemoryTracking<Int32> field_ids;
    Int64 snapshot_id = -1;
    Int64 sequence_number = -1;
};

}

PuffinThetaSketchBuilder::ColumnSketch::ColumnSketch(const String & column_name, const DataTypePtr & type_)
    : type(removeNullable(removeLowCardinality(type_)))
    , sketch(datasketches::update_theta_sketch::builder().build())
{
    switch (type->getTypeId())
    {
        case TypeIndex::UInt8:
        case TypeIndex::Int8:
        case TypeIndex::UInt16:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::UInt32:
        case TypeIndex::Int64:
        case TypeIndex::UInt64:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::String:
        case TypeIndex::FixedString:
        case TypeIndex::UUID:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
            return;
        case TypeIndex::DateTime64:
            if (assert_cast<const DataTypeDateTime64 &>(*type).getScale() > ICEBERG_TIMESTAMP_SCALE)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot write a Theta sketch for column {} of type {}: Iceberg timestamps have microsecond precision, "
                    "cast the column to DateTime64(6)",
                    column_name,
                    type_->getName());
            return;
        default:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot write a Theta sketch for column {} of type {}: the type has no Iceberg single-value serialization",
                column_name,
                type_->getName());
    }
}

void PuffinThetaSketchBuilder::ColumnSketch::update(const void * data, size_t size)
{
    if (size != 0)
        sketch.update(data, size);
}

void PuffinThetaSketchBuilder::ColumnSketch::add(const IColumn & column_)
{
    ColumnPtr column = column_.convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
    const NullMap * null_map = nullptr;
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(column.get()))
    {
        null_map = &nullable->getNullMapData();
        column = nullable->getNestedColumnPtr();
    }

    char buf[sizeof(Int256)];
    auto add_int32 = [&](auto value)
    {
        storeLittleEndian<Int32>(buf, static_cast<Int32>(value));
        update(buf, sizeof(Int32));
    };
    auto add_int64 = [&](auto value)
    {
        storeLittleEndian<Int64>(buf, static_cast<Int64>(value));
        update(buf, sizeof(Int64));
    };
    auto add_decimal = [&](auto value) { update(buf, storeMinimalBigEndian(buf, value.value)); };

    switch (type->getTypeId())
    {
        case TypeIndex::UInt8:
            if (isBool(type))
                forEachValue<ColumnUInt8>(*column, null_map, [&](UInt8 value)
                {
                    buf[0] = value != 0;
                    update(buf, 1);
                });
            else
                forEachValue<ColumnUInt8>(*column, null_map, add_int32);
            break;
        case TypeIndex::Int8:
            forEachValue<ColumnInt8>(*column, null_map, add_int32);
            break;
        case TypeIndex::UInt16:
        case TypeIndex::Date:
            forEachValue<ColumnUInt16>(*column, null_map, add_int32);
            break;
        case TypeIndex::Int16:
            forEachValue<ColumnInt16>(*column, null_map, add_int32);
            break;
        case TypeIndex::Int32:
        case TypeIndex::Date32:
            forEachValue<ColumnInt32>(*column, null_map, add_int32);
            break;
        case TypeIndex::UInt32:
            forEachValue<ColumnUInt32>(*column, null_map, add_int64);
            break;
        case TypeIndex::Int64:
            forEachValue<ColumnInt64>(*column, null_map, add_int64);
            break;
        case TypeIndex::UInt64:
            forEachValue<ColumnUInt64>(*column, null_map, add_int64);
            break;
        case TypeIndex::DateTime:
            forEachValue<ColumnUInt32>(*column, null_map, [&](UInt32 seconds) { add_int64(static_cast<Int64>(seconds) * 1'000'000); });
            break;
        case TypeIndex::DateTime64:
        {
            const Int64 multiplier = DecimalUtils::scaleMultiplier<Int64>(
                ICEBERG_TIMESTAMP_SCALE - assert_cast<const DataTypeDateTime64 &>(*type).getScale());
            forEachValue<ColumnDateTime64>(*column, null_map, [&](DateTime64 value) { add_int64(value.value * multiplier); });
            break;
        }
        case TypeIndex::Float32:
            forEachValue<ColumnFloat32>(*column, null_map, [&](Float32 value)
            {
                storeLittleEndian<Float32>(buf, value);
                update(buf, sizeof(Float32));
            });
            break;
        case TypeIndex::Float64:
            forEachValue<ColumnFloat64>(*column, null_map, [&](Float64 value)
            {
                storeLittleEndian<Float64>(buf, value);
                update(buf, sizeof(Float64));
            });
            break;
        case TypeIndex::UUID:
            forEachValue<ColumnUUID>(*column, null_map, [&](const UUID & value)
            {
                unalignedStoreBigEndian<UInt64>(buf, UUIDHelpers::getHighBytes(value));
                unalignedStoreBigEndian<UInt64>(buf + sizeof(UInt64), UUIDHelpers::getLowBytes(value));
                update(buf, sizeof(UUID));
            });
            break;
        case TypeIndex::Decimal32:
            forEachValue<ColumnDecimal<Decimal32>>(*column, null_map, add_decimal);
            break;
        case TypeIndex::Decimal64:
            forEachValue<ColumnDecimal<Decimal64>>(*column, null_map, add_decimal);
            break;
        case TypeIndex::Decimal128:
            forEachValue<ColumnDecimal<Decimal128>>(*column, null_map, add_decimal);
            break;
        case TypeIndex::Decimal256:
            forEachValue<ColumnDecimal<Decimal256>>(*column, null_map, add_decimal);
            break;
        case TypeIndex::String:
        case TypeIndex::FixedString:
            for (size_t i = 0; i < column->size(); ++i)
            {
                if (null_map && (*null_map)[i])
                    continue;
                const std::string_view value = column->getDataAt(i);
                update(value.data(), value.size());
            }
            break;
        default:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot write a Theta sketch for type {}", type->getName());
    }
}

PuffinThetaSketchBuilder::PuffinThetaSketchBuilder(const Block & header, const FormatSettings & format_settings)
    : field_ids(parseFieldIds(format_settings.puffin.output_field_ids, header))
    , snapshot_id(format_settings.puffin.output_snapshot_id)
    , sequence_number(format_settings.puffin.output_sequence_number)
{
    sketches.reserve(header.columns());
    for (const auto & column : header)
        sketches.emplace_back(column.name, column.type);
}

void PuffinThetaSketchBuilder::consume(const Chunk & chunk)
{
    const auto & columns = chunk.getColumns();
    for (size_t i = 0; i < columns.size(); ++i)
        sketches[i].add(*columns[i]);
}

void PuffinThetaSketchBuilder::write(PuffinWriter & writer)
{
    for (size_t i = 0; i < sketches.size(); ++i)
    {
        const auto compact = compactThetaSketch(sketches[i].get());
        const auto bytes = compact.serialize();
        writer.addBlob(
            makeThetaSketchBlobMetadata(field_ids[i], snapshot_id, sequence_number, static_cast<UInt64>(compact.get_estimate())),
            std::string_view(reinterpret_cast<const char *>(bytes.data()), bytes.size()));
    }
}

void PuffinThetaSketchBuilder::reset()
{
    for (auto & sketch : sketches)
        sketch.reset();
}

}

#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

PuffinBlobBuilderPtr createPuffinThetaSketchBuilder(const Block & header, const FormatSettings & format_settings)
{
#if USE_DATASKETCHES
    return std::make_unique<PuffinThetaSketchBuilder>(header, format_settings);
#else
    UNUSED(header, format_settings);
    throw Exception(
        ErrorCodes::SUPPORT_IS_DISABLED, "Writing Theta sketches to Puffin files is disabled, ClickHouse was built without DataSketches");
#endif
}

}
