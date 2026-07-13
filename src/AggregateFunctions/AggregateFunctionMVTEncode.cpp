#include <algorithm>
#include <cmath>
#include <limits>
#include <string>
#include <string_view>
#include <vector>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/MVTProtobuf.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadHelpers.h>
#include <IO/VarInt.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{
struct Settings;

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

namespace
{

/// Global discriminator order of the `Geometry` Variant: alternatives are sorted alphabetically by type name.
namespace GeoDisc
{
    constexpr UInt8 LineString = 0;
    constexpr UInt8 MultiLineString = 1;
    constexpr UInt8 MultiPolygon = 2;
    constexpr UInt8 Point = 3;
    constexpr UInt8 Polygon = 4;
    constexpr UInt8 Ring = 5;
}

/// Mapbox Vector Tile feature geometry types.
namespace MVTGeomType
{
    constexpr UInt8 Point = 1;
    constexpr UInt8 LineString = 2;
    constexpr UInt8 Polygon = 3;
}

/// MVT geometry command ids (a CommandInteger is `(id & 0x7) | (count << 3)`).
namespace MVTCommand
{
    constexpr UInt32 MoveTo = 1;
    constexpr UInt32 LineTo = 2;
    constexpr UInt32 ClosePath = 7;
}

/// Exterior rings are emitted so that their shoelace area in the (y-down) tile coordinate system is positive
/// (clockwise on screen), interior rings negative — the orientation required by the MVT specification.
constexpr bool mvt_exterior_area_positive = true;

/// Which Mapbox Vector Tile `Value` variant a property column is encoded as.
enum class MVTValueKind : UInt8
{
    String, /// string_value (field 1)
    FixedString, /// string_value (field 1), with FixedString NUL padding trimmed
    Float, /// float_value (field 2)
    Double, /// double_value (field 3)
    Sint, /// sint_value (field 6, zigzag)
    UInt, /// uint_value (field 5)
    Bool, /// bool_value (field 7)
    StringifiedText, /// string_value (field 1), the value's text form (opt-in fallback for otherwise-unsupported types)
};

struct MVTProperty
{
    String key;
    MVTValueKind kind;
    size_t column_index; /// index of this element in the properties tuple (the feature-id element is excluded)
    SerializationPtr serialization; /// set only for StringifiedText, to render the value as text
};

/// Variable-length aggregate state held in the arena: a flat buffer of self-contained feature records.
/// Each record is `[UInt8 geometry_type][varint geometry_length][geometry command bytes]` followed, for every
/// property column in tuple order, by a presence byte and (when present) a varint-length-prefixed pre-rendered MVT
/// `Value` message. Records are self-contained and order-independent, so `merge` is a plain buffer concatenation.
/// Value interning into the layer's shared value pool, and the final protobuf assembly, are deferred to
/// `insertResultInto`, which is where MVT property sharing happens.
struct AggregateFunctionMVTEncodeData
{
    UInt64 data_size = 0;
    UInt64 allocated_size = 0;
    char * data = nullptr;
    UInt64 num_features = 0;

    void reserveForAppend(UInt64 add, Arena * arena)
    {
        if (data_size + add > allocated_size)
        {
            size_t old_size = allocated_size;
            allocated_size = std::max<UInt64>(2 * allocated_size, data_size + add);
            data = arena->realloc(data, old_size, allocated_size);
        }
    }

    void append(const char * src, UInt64 size, Arena * arena)
    {
        reserveForAppend(size, arena);
        memcpy(data + data_size, src, size);
        data_size += size;
    }
};

class AggregateFunctionMVTEncode final
    : public IAggregateFunctionDataHelper<AggregateFunctionMVTEncodeData, AggregateFunctionMVTEncode>
{
private:
    using Base = IAggregateFunctionDataHelper<AggregateFunctionMVTEncodeData, AggregateFunctionMVTEncode>;

    const String layer_name;
    const UInt32 extent;
    const bool has_properties;
    VectorWithMemoryTracking<MVTProperty> properties;
    /// Optional MVT Feature `id` (PostGIS `feature_id_name`): a property-tuple element emitted as the feature id
    /// (uint64) instead of as a tag. Set when the third parameter names an unsigned-integer element of the properties tuple.
    bool has_feature_id = false;
    size_t feature_id_column_index = 0;

    /// Returns the MVT value kind for a directly-supported property type, or nullopt for an unsupported type
    /// (the caller either throws or falls back to StringifiedText).
    static std::optional<MVTValueKind> kindForType(const DataTypePtr & element_type)
    {
        const DataTypePtr base = removeNullable(recursiveRemoveLowCardinality(element_type));
        WhichDataType which(base);

        if (which.isString())
            return MVTValueKind::String;
        if (which.isFixedString())
            return MVTValueKind::FixedString;
        if (which.isFloat32() || which.isBFloat16())
            return MVTValueKind::Float;
        if (which.isFloat64())
            return MVTValueKind::Double;
        if (isBool(base))
            return MVTValueKind::Bool;
        if (which.isNativeInt() || which.isDate32())
            return MVTValueKind::Sint;
        if (which.isNativeUInt() || which.isDate() || which.isDateTime())
            return MVTValueKind::UInt;

        return std::nullopt;
    }

    /// Render one property value as the body of an MVT `Value` message into `out`.
    void renderValue(const MVTProperty & property, const IColumn & column, size_t row, String & out) const
    {
        switch (property.kind)
        {
            case MVTValueKind::String:
            {
                const std::string_view value = column.getDataAt(row);
                MVT::writeLengthDelimitedField(out, 1, value);
                return;
            }
            case MVTValueKind::FixedString:
            {
                /// FixedString right-pads with NUL bytes; the logical value (and the equivalent String) has the
                /// trailing NULs trimmed, matching ClickHouse's FixedString-to-String conversion.
                std::string_view value = column.getDataAt(row);
                const size_t last = value.find_last_not_of('\0');
                value = last == std::string_view::npos ? std::string_view{} : value.substr(0, last + 1);
                MVT::writeLengthDelimitedField(out, 1, value);
                return;
            }
            case MVTValueKind::Float:
                MVT::writeFloatField(out, 2, column.getFloat32(row));
                return;
            case MVTValueKind::Double:
                MVT::writeDoubleField(out, 3, column.getFloat64(row));
                return;
            case MVTValueKind::Sint:
                MVT::writeVarintField(out, 6, encodeZigZag(column.getInt(row)));
                return;
            case MVTValueKind::UInt:
                MVT::writeVarintField(out, 5, column.getUInt(row));
                return;
            case MVTValueKind::Bool:
                MVT::writeVarintField(out, 7, column.getUInt(row) != 0 ? 1 : 0);
                return;
            case MVTValueKind::StringifiedText:
            {
                /// Opt-in fallback: render the value's text form (e.g. big integers, UUID, Decimal) as a string_value.
                WriteBufferFromOwnString buf;
                property.serialization->serializeText(column, row, buf, FormatSettings{});
                MVT::writeLengthDelimitedField(out, 1, buf.str());
                return;
            }
        }
    }

    /// ── Geometry command-stream encoding ────────────────────────────────────────────────────────────────────────
    /// MVT geometry is a packed stream of command/parameter integers walking the vertices with a running cursor.

    static Int32 toTileCoordinate(Float64 value, const String & function_name)
    {
        if (!std::isfinite(value))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} received a non-finite geometry coordinate", function_name);
        /// llrint rounds ties to even (matching PostGIS), unlike llround which rounds ties away from zero.
        const Int64 rounded = std::llrint(value);
        if (rounded < std::numeric_limits<Int32>::min() || rounded > std::numeric_limits<Int32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function {} received a geometry coordinate ({}) outside the representable MVT range; "
                "use MVTEncodeGeom to project and clip the geometry to a tile first",
                function_name,
                value);
        return static_cast<Int32>(rounded);
    }

    static void readPoint(const Field & point_field, Int32 & x, Int32 & y, const String & function_name)
    {
        const Tuple & tuple = point_field.safeGet<Tuple>();
        if (tuple.size() != 2)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} expects each point to have two coordinates", function_name);
        x = toTileCoordinate(tuple[0].safeGet<Float64>(), function_name);
        y = toTileCoordinate(tuple[1].safeGet<Float64>(), function_name);
    }

    using TilePoints = VectorWithMemoryTracking<std::pair<Int32, Int32>>;

    static TilePoints readPointSequence(const Field & array_field, const String & function_name)
    {
        const Array & array = array_field.safeGet<Array>();
        TilePoints points;
        points.reserve(array.size());
        for (const Field & point_field : array)
        {
            Int32 x = 0;
            Int32 y = 0;
            readPoint(point_field, x, y, function_name);
            points.emplace_back(x, y);
        }
        return points;
    }

    /// MVT geometry parameters are zig-zagged deltas packed into a `uint32` field, so each delta must fit in `Int32`
    /// (the zig-zag of any `Int32` fits in `uint32`). Two individually in-range vertices can still differ by more than
    /// `Int32` can hold, so validate the delta before encoding it.
    static UInt64 zigZagDelta(Int64 delta)
    {
        if (delta < std::numeric_limits<Int32>::min() || delta > std::numeric_limits<Int32>::max())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Aggregate function MVTEncode produced a geometry delta ({}) outside the range of the MVT command "
                "stream; reduce the extent or clip the geometry to the tile with MVTEncodeGeom",
                delta);
        return encodeZigZag(delta);
    }

    /// Remove consecutive duplicate vertices, which arise when nearby vertices round to the same pixel. Adjacent
    /// duplicates would otherwise emit a zero-delta LineTo, which the MVT command stream forbids.
    static void removeConsecutiveDuplicates(TilePoints & points)
    {
        points.erase(std::unique(points.begin(), points.end()), points.end());
    }

    /// Emit MoveTo + LineTo for an open path; advances the cursor. A path with fewer than two distinct vertices is
    /// degenerate and emits nothing.
    static void emitLineTo(String & out, TilePoints points, Int64 & cursor_x, Int64 & cursor_y)
    {
        removeConsecutiveDuplicates(points);
        if (points.size() < 2)
            return;
        MVT::writeVarint(out, (MVTCommand::MoveTo & 0x7) | (1u << 3));
        MVT::writeVarint(out, zigZagDelta(points[0].first - cursor_x));
        MVT::writeVarint(out, zigZagDelta(points[0].second - cursor_y));
        cursor_x = points[0].first;
        cursor_y = points[0].second;
        MVT::writeVarint(out, (MVTCommand::LineTo & 0x7) | (static_cast<UInt32>(points.size() - 1) << 3));
        for (size_t i = 1; i < points.size(); ++i)
        {
            MVT::writeVarint(out, zigZagDelta(points[i].first - cursor_x));
            MVT::writeVarint(out, zigZagDelta(points[i].second - cursor_y));
            cursor_x = points[i].first;
            cursor_y = points[i].second;
        }
    }

    /// Twice the signed area (the shoelace sum), computed exactly. Each Int32 * Int32 product fits in Int64 and the
    /// sum is accumulated in 128-bit, so the orientation and the zero-area (degenerate) test stay correct even at
    /// coordinates near Int32::max, where a double accumulator would lose a small ring's area to cancellation.
    static Int128 doubledSignedArea(const TilePoints & points)
    {
        Int128 area = 0;
        const size_t n = points.size();
        for (size_t i = 0; i < n; ++i)
        {
            const auto & a = points[i];
            const auto & b = points[(i + 1) % n];
            area += static_cast<Int128>(a.first) * b.second - static_cast<Int128>(b.first) * a.second;
        }
        return area;
    }

    /// Emit a closed ring (MoveTo + LineTo + ClosePath) with the orientation MVT requires; advances the cursor.
    /// Returns false (emitting nothing) for a degenerate ring — fewer than three distinct vertices, or zero area
    /// after quantization to the pixel grid — which the MVT specification forbids.
    static bool emitRing(String & out, TilePoints points, bool exterior, Int64 & cursor_x, Int64 & cursor_y)
    {
        if (points.size() >= 2 && points.front() == points.back())
            points.pop_back();
        removeConsecutiveDuplicates(points);
        if (points.size() < 3)
            return false;

        const Int128 area = doubledSignedArea(points);
        if (area == 0)
            return false;

        const bool want_positive = exterior == mvt_exterior_area_positive;
        if ((area > 0) != want_positive)
            std::reverse(points.begin(), points.end());

        MVT::writeVarint(out, (MVTCommand::MoveTo & 0x7) | (1u << 3));
        MVT::writeVarint(out, zigZagDelta(points[0].first - cursor_x));
        MVT::writeVarint(out, zigZagDelta(points[0].second - cursor_y));
        cursor_x = points[0].first;
        cursor_y = points[0].second;
        MVT::writeVarint(out, (MVTCommand::LineTo & 0x7) | (static_cast<UInt32>(points.size() - 1) << 3));
        for (size_t i = 1; i < points.size(); ++i)
        {
            MVT::writeVarint(out, zigZagDelta(points[i].first - cursor_x));
            MVT::writeVarint(out, zigZagDelta(points[i].second - cursor_y));
            cursor_x = points[i].first;
            cursor_y = points[i].second;
        }
        MVT::writeVarint(out, (MVTCommand::ClosePath & 0x7) | (1u << 3));
        return true;
    }

    /// Emit a polygon (exterior ring followed by holes). If the exterior ring is degenerate and dropped, the whole
    /// polygon — including its holes — is skipped, so the output never contains holes without an enclosing ring.
    static void emitPolygon(String & out, const Field & rings_field, Int64 & cursor_x, Int64 & cursor_y, const String & function_name)
    {
        const Array & rings = rings_field.safeGet<Array>();
        for (size_t i = 0; i < rings.size(); ++i)
        {
            const bool exterior = i == 0;
            const bool emitted = emitRing(out, readPointSequence(rings[i], function_name), exterior, cursor_x, cursor_y);
            if (exterior && !emitted)
                return;
        }
    }

    /// Encode one geometry (identified by its Variant discriminator) into the MVT command stream `out`,
    /// returning the MVT feature geometry type.
    UInt8 encodeGeometry(UInt8 discriminator, const Field & geometry, String & out) const
    {
        Int64 cursor_x = 0;
        Int64 cursor_y = 0;
        switch (discriminator)
        {
            case GeoDisc::Point:
            {
                Int32 x = 0;
                Int32 y = 0;
                readPoint(geometry, x, y, getName());
                MVT::writeVarint(out, (MVTCommand::MoveTo & 0x7) | (1u << 3));
                MVT::writeVarint(out, zigZagDelta(x));
                MVT::writeVarint(out, zigZagDelta(y));
                return MVTGeomType::Point;
            }
            case GeoDisc::LineString:
                emitLineTo(out, readPointSequence(geometry, getName()), cursor_x, cursor_y);
                return MVTGeomType::LineString;
            case GeoDisc::MultiLineString:
            {
                const Array & lines = geometry.safeGet<Array>();
                for (const Field & line : lines)
                    emitLineTo(out, readPointSequence(line, getName()), cursor_x, cursor_y);
                return MVTGeomType::LineString;
            }
            case GeoDisc::Ring:
                emitRing(out, readPointSequence(geometry, getName()), /*exterior=*/true, cursor_x, cursor_y);
                return MVTGeomType::Polygon;
            case GeoDisc::Polygon:
                emitPolygon(out, geometry, cursor_x, cursor_y, getName());
                return MVTGeomType::Polygon;
            case GeoDisc::MultiPolygon:
            {
                const Array & polygons = geometry.safeGet<Array>();
                for (const Field & polygon : polygons)
                    emitPolygon(out, polygon, cursor_x, cursor_y, getName());
                return MVTGeomType::Polygon;
            }
            default:
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Aggregate function {} received an unsupported geometry", getName());
        }
    }

public:
    AggregateFunctionMVTEncode(
        const DataTypes & argument_types_, const Array & parameters_, String layer_name_, UInt32 extent_,
        const String & feature_id_name_, bool stringify_)
        : IAggregateFunctionDataHelper<AggregateFunctionMVTEncodeData, AggregateFunctionMVTEncode>(
              argument_types_, parameters_, std::make_shared<DataTypeString>())
        , layer_name(std::move(layer_name_))
        , extent(extent_)
        , has_properties(argument_types_.size() == 2)
    {
        if (argument_types_[0]->getName() != "Geometry")
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The geometry (first) argument of aggregate function {} must be of type Geometry (e.g. the result of "
                "MVTEncodeGeom), got {}",
                getName(),
                argument_types_[0]->getName());

        if (has_properties)
        {
            const auto * properties_type = typeid_cast<const DataTypeTuple *>(argument_types_[1].get());
            if (!properties_type)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The properties (second) argument of aggregate function {} must be a named tuple", getName());
            if (!properties_type->hasExplicitNames())
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The properties (second) argument of aggregate function {} must be a named tuple; its element names "
                    "become the vector tile feature attribute keys. Aliases inside tuple(...) are not propagated to "
                    "element names, so name them with a cast, e.g. tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)",
                    getName());

            const auto & names = properties_type->getElementNames();
            const auto & elements = properties_type->getElements();

            /// Resolve the optional feature-id element (PostGIS `feature_id_name`): it must name an unsigned-integer
            /// element, which is emitted as the MVT Feature `id` and excluded from the property tags. Signed integers
            /// are rejected because the MVT id is a `uint64` and a signed column would silently drop negative ids.
            std::optional<size_t> id_index;
            if (!feature_id_name_.empty())
            {
                for (size_t i = 0; i < names.size(); ++i)
                {
                    if (names[i] == feature_id_name_)
                    {
                        id_index = i;
                        break;
                    }
                }
                if (!id_index)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "The feature_id_name '{}' of aggregate function {} is not an element of the properties tuple",
                        feature_id_name_, getName());

                const DataTypePtr id_base = removeNullable(recursiveRemoveLowCardinality(elements[*id_index]));
                const WhichDataType id_which(id_base);
                if (!id_which.isNativeUInt())
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "The feature id element '{}' of aggregate function {} must be an unsigned integer type, got {}",
                        feature_id_name_, getName(), elements[*id_index]->getName());

                has_feature_id = true;
                feature_id_column_index = *id_index;
            }

            properties.reserve(id_index ? elements.size() - 1 : elements.size());
            for (size_t i = 0; i < elements.size(); ++i)
            {
                if (id_index && i == *id_index)
                    continue; /// emitted as the feature id, not as a property tag

                if (const auto kind = kindForType(elements[i]))
                    properties.push_back({names[i], *kind, i, nullptr});
                else if (stringify_)
                    properties.push_back({names[i], MVTValueKind::StringifiedText, i, elements[i]->getDefaultSerialization()});
                else
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Property of type {} is not supported by aggregate function {}. Supported types are String, "
                        "FixedString, Bool, Float32, Float64, BFloat16, (U)Int8/16/32/64, Date, Date32 and DateTime "
                        "(optionally Nullable and/or LowCardinality); pass the stringify_unsupported parameter to encode "
                        "other types as their text form, or cast with toString()",
                        elements[i]->getName(), getName());
            }
        }
        else if (!feature_id_name_.empty())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The feature_id_name parameter of aggregate function {} requires a properties tuple to name the id element from",
                getName());
        }
    }

    String getName() const override { return "MVTEncode"; }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & geometry = assert_cast<const ColumnVariant &>(*columns[0]);
        const UInt8 discriminator = geometry.globalDiscriminatorAt(row_num);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
            return;

        Field geometry_field;
        geometry.get(row_num, geometry_field);

        String geometry_bytes;
        const UInt8 geometry_type = encodeGeometry(discriminator, geometry_field, geometry_bytes);
        if (geometry_bytes.empty())
            return;

        String record;
        record.push_back(static_cast<char>(geometry_type));
        MVT::writeVarint(record, geometry_bytes.size());
        record.append(geometry_bytes);

        if (has_properties)
        {
            const auto & properties_column = assert_cast<const ColumnTuple &>(*columns[1]);

            if (has_feature_id)
            {
                const IColumn & id_column = properties_column.getColumn(feature_id_column_index);
                const bool present = !id_column.isNullAt(row_num);
                record.push_back(present ? 1 : 0);
                if (present)
                    MVT::writeVarint(record, id_column.getUInt(row_num));
            }

            for (const auto & property : properties)
            {
                const IColumn & column = properties_column.getColumn(property.column_index);
                if (column.isNullAt(row_num))
                {
                    record.push_back(0);
                    continue;
                }
                record.push_back(1);
                String value;
                renderValue(property, column, row_num, value);
                MVT::writeVarint(record, value.size());
                record.append(value);
            }
        }

        auto & state = Base::data(place);
        state.append(record.data(), record.size(), arena);
        ++state.num_features;
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        const auto & rhs_state = Base::data(rhs);
        if (rhs_state.data_size == 0)
            return;
        auto & state = Base::data(place);
        state.append(rhs_state.data, rhs_state.data_size, arena);
        state.num_features += rhs_state.num_features;
    }

    /// Version of the serialized aggregate state layout, written as the first byte. The state is persisted
    /// (e.g. by AggregatingMergeTree and the -State combinator) and exchanged between servers of different
    /// versions, so tagging it lets the record format evolve later (e.g. interning values earlier to shrink
    /// the state) without breaking states written by other versions.
    static constexpr UInt8 state_format_version = 0;

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        const auto & state = Base::data(place);
        writeBinary(state_format_version, buf);
        writeVarUInt(state.num_features, buf);
        writeVarUInt(state.data_size, buf);
        buf.write(state.data, state.data_size);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        auto & state = Base::data(place);
        if (state.data_size != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "MVTEncode deserialize() expects an empty state");

        UInt8 version = 0;
        readBinary(version, buf);
        if (version != state_format_version)
            throw Exception(
                ErrorCodes::INCORRECT_DATA, "Unknown MVTEncode aggregate state version {}", static_cast<UInt16>(version));

        readVarUInt(state.num_features, buf);

        UInt64 size = 0;
        readVarUInt(size, buf);
        static constexpr UInt64 max_data_size = UInt64{1} << 48;
        if (size > max_data_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid MVTEncode state: data size {} is too large", size);

        /// A record is at least one geometry-type byte plus a one-byte (zero-length) geometry, plus the feature-id
        /// presence byte (when configured) and one presence byte per property, so reject payloads too small to hold
        /// the claimed feature count (overflow-safe).
        const UInt64 min_record_size = 2 + (has_feature_id ? 1 : 0) + properties.size();
        if (state.num_features != 0 && state.num_features > size / min_record_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid MVTEncode state: payload too small for {} features", state.num_features);

        state.reserveForAppend(size, arena);
        buf.readStrict(state.data, size);
        state.data_size = size;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = Base::data(place);
        if (state.num_features == 0)
        {
            to.insertDefault();
            return;
        }

        VectorWithMemoryTracking<String> value_pool;
        UnorderedMapWithMemoryTracking<String, UInt32> value_index;

        String features;
        const char * pos = state.data;
        const char * end = state.data + state.data_size;

        for (UInt64 feature = 0; feature < state.num_features; ++feature)
        {
            if (pos >= end)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted MVTEncode aggregate state: truncated feature");
            const UInt8 geometry_type = static_cast<UInt8>(*pos++);

            UInt64 geometry_length = 0;
            pos = readVarUInt(geometry_length, pos, end - pos);
            if (geometry_length > static_cast<UInt64>(end - pos))
                throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted MVTEncode aggregate state: truncated geometry");
            std::string_view geometry(pos, geometry_length);
            pos += geometry_length;

            UInt64 feature_id = 0;
            bool has_id = false;
            if (has_feature_id)
            {
                if (pos >= end)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted MVTEncode aggregate state: truncated feature id");
                const bool present = *pos++ != 0;
                if (present)
                {
                    pos = readVarUInt(feature_id, pos, end - pos);
                    has_id = true;
                }
            }

            String tags;
            for (size_t i = 0; i < properties.size(); ++i)
            {
                if (pos >= end)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted MVTEncode aggregate state: truncated property");
                const bool present = *pos++ != 0;
                if (!present)
                    continue;

                UInt64 length = 0;
                pos = readVarUInt(length, pos, end - pos);
                /// Compare sizes rather than pointers: `length` is read from the (possibly corrupted) state and could
                /// be huge, so `pos + length` would overflow the pointer and the check could be bypassed.
                if (length > static_cast<UInt64>(end - pos))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted MVTEncode aggregate state: truncated value");
                std::string_view value(pos, length);
                pos += length;

                auto [it, inserted] = value_index.try_emplace(String(value), static_cast<UInt32>(value_pool.size()));
                if (inserted)
                    value_pool.emplace_back(value);

                MVT::writeVarint(tags, i); /// key index (keys follow tuple element order)
                MVT::writeVarint(tags, it->second); /// value index into the shared value pool
            }

            String feature_message;
            if (has_id)
                MVT::writeVarintField(feature_message, 1, feature_id); /// Feature.id (field 1)
            MVT::writeVarintField(feature_message, 3, geometry_type);
            MVT::writeLengthDelimitedField(feature_message, 2, tags);
            MVT::writeLengthDelimitedField(feature_message, 4, geometry);

            MVT::writeLengthDelimitedField(features, 2, feature_message);
        }

        String layer;
        MVT::writeVarintField(layer, 15, 2); /// version = 2
        MVT::writeLengthDelimitedField(layer, 1, layer_name);
        layer.append(features);
        for (const auto & property : properties)
            MVT::writeLengthDelimitedField(layer, 3, property.key);
        for (const auto & value : value_pool)
            MVT::writeLengthDelimitedField(layer, 4, value);
        MVT::writeVarintField(layer, 5, extent);

        String tile;
        MVT::writeLengthDelimitedField(tile, 3, layer);

        assert_cast<ColumnString &>(to).insertData(tile.data(), tile.size());
    }
};

AggregateFunctionPtr createAggregateFunctionMVTEncode(
    const std::string & name, const DataTypes & argument_types, const Array & parameters, const Settings *)
{
    if (argument_types.empty() || argument_types.size() > 2)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires 1 or 2 arguments (geometry[, properties]), got {}",
            name,
            argument_types.size());

    if (parameters.empty())
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Aggregate function {} requires a layer name parameter, e.g. MVTEncode('layer')(geometry, properties)",
            name);
    if (parameters.size() > 4)
        throw Exception(
            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
            "Aggregate function {} accepts at most 4 parameters (layer_name[, extent[, feature_id_name[, stringify_unsupported]]]), got {}",
            name,
            parameters.size());

    if (parameters[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first parameter (layer_name) of aggregate function {} must be a string", name);
    const String layer_name = parameters[0].safeGet<String>();

    UInt32 extent = 4096;
    if (parameters.size() >= 2)
    {
        const auto type = parameters[1].getType();
        static constexpr UInt64 max_extent = std::numeric_limits<Int32>::max();
        if (type == Field::Types::UInt64)
        {
            const UInt64 value = parameters[1].safeGet<UInt64>();
            if (value == 0 || value > max_extent)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second parameter (extent) of aggregate function {} must be in the range [1, 2147483647], got {}", name, value);
            extent = static_cast<UInt32>(value);
        }
        else if (type == Field::Types::Int64)
        {
            const Int64 value = parameters[1].safeGet<Int64>();
            if (value <= 0 || value > static_cast<Int64>(max_extent))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second parameter (extent) of aggregate function {} must be in the range [1, 2147483647], got {}", name, value);
            extent = static_cast<UInt32>(value);
        }
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second parameter (extent) of aggregate function {} must be a positive integer", name);
        }
    }

    String feature_id_name;
    if (parameters.size() >= 3)
    {
        if (parameters[2].getType() != Field::Types::String)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The third parameter (feature_id_name) of aggregate function {} must be a string naming an unsigned "
                "integer element of the properties tuple",
                name);
        feature_id_name = parameters[2].safeGet<String>();
    }

    bool stringify_unsupported = false;
    if (parameters.size() == 4)
    {
        const auto type = parameters[3].getType();
        if (type == Field::Types::UInt64)
            stringify_unsupported = parameters[3].safeGet<UInt64>() != 0;
        else if (type == Field::Types::Int64)
            stringify_unsupported = parameters[3].safeGet<Int64>() != 0;
        else
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "The fourth parameter (stringify_unsupported) of aggregate function {} must be 0 or 1",
                name);
    }

    return std::make_shared<AggregateFunctionMVTEncode>(
        argument_types, parameters, layer_name, extent, feature_id_name, stringify_unsupported);
}

}

void registerAggregateFunctionMVTEncode(AggregateFunctionFactory & factory);

void registerAggregateFunctionMVTEncode(AggregateFunctionFactory & factory)
{
    const AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description description = R"(
Encodes a group of features into a binary [Mapbox Vector Tile](https://github.com/mapbox/vector-tile-spec) layer.

This is the aggregate counterpart of the scalar function `MVTEncodeGeom`. Each input row becomes one feature. The
geometry argument is a `Geometry` of tile-space coordinates, typically produced by `MVTEncodeGeom`; the optional
properties argument is a named tuple whose element names become the feature attribute keys and whose element types
determine the vector tile value types. Point, line and polygon geometry are supported.

The properties tuple must have explicit element names. Column aliases inside `tuple(...)` are not propagated to tuple
element names, so name the elements with a cast, for example `tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)`.

Identical property values are interned into the layer's shared value pool, so the emitted tile does not duplicate them.
If `feature_id_name` is given, the named unsigned-integer property element is emitted as the MVT Feature `id` (a `uint64`)
and excluded from the tags; a `NULL` id is omitted for that feature.
The result is the raw bytes of a single-layer tile, which can be returned directly over the HTTP interface with
`FORMAT RawBLOB`. This function is the analogue of PostGIS `ST_AsMVT`.
    )";
    FunctionDocumentation::Syntax syntax
        = "MVTEncode(layer_name[, extent[, feature_id_name[, stringify_unsupported]]])(geometry[, properties])";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "Tile-space geometry, e.g. from `MVTEncodeGeom`.", {"Geometry"}},
        {"properties", "Optional named tuple of feature attributes. Element names become attribute keys.", {"Tuple"}},
    };
    FunctionDocumentation::Parameters parameters = {
        {"layer_name", "Name of the vector tile layer.", {"String"}},
        {"extent", "Tile extent in pixels per side. Defaults to `4096`.", {"UInt32"}},
        {"feature_id_name",
         "Optional name of an unsigned-integer element of the properties tuple to emit as the Feature `id` instead of as a tag. "
         "Parameters are positional, so `extent` must also be given to use it.",
         {"String"}},
        {"stringify_unsupported",
         "Optional flag (`0`/`1`, default `0`); when `1`, property types not directly supported (e.g. big integers, "
         "`UUID`, `Decimal`) are encoded as their text `string_value` instead of raising an error.",
         {"UInt8"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the binary contents of a single-layer Mapbox Vector Tile.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Encode a clustered tile of points",
            R"(
SELECT MVTEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT MVTEncodeGeom((lon, lat)::Point, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
)
SETTINGS allow_suspicious_types_in_group_by = 1;
            )",
            "",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::GeoPolygon;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("MVTEncode", {createAggregateFunctionMVTEncode, documentation, properties});
    factory.registerAlias("ST_AsMVT", "MVTEncode");
}

}
