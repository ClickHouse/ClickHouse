#include <cmath>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/MVTProtobuf.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/IDataType.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/Arena.h>
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

/// Which Mapbox Vector Tile `Value` variant a property column is encoded as.
enum class MvtValueKind : UInt8
{
    String, /// string_value (field 1)
    Float, /// float_value (field 2)
    Double, /// double_value (field 3)
    Int, /// int_value (field 4)
    UInt, /// uint_value (field 5)
};

struct MvtProperty
{
    String key;
    MvtValueKind kind;
};

/// Variable-length aggregate state held in the arena: a flat buffer of self-contained feature records.
/// Each record is `[Int32 pixel_x][Int32 pixel_y]` followed, for every property column in tuple order, by a
/// presence byte and (when present) a varint-length-prefixed pre-rendered MVT `Value` message. Because records
/// are self-contained and order-independent, `merge` is a plain buffer concatenation. Value interning and the
/// final protobuf assembly are deferred to `insertResultInto`.
struct AggregateFunctionMvtEncodeData
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

class AggregateFunctionMvtEncode final
    : public IAggregateFunctionDataHelper<AggregateFunctionMvtEncodeData, AggregateFunctionMvtEncode>
{
private:
    const String layer_name;
    const UInt32 extent;
    const bool has_clip;
    /// Inclusive pixel bounds of the clip window [-buffer, extent + buffer]; only used when has_clip.
    const Int64 clip_lower;
    const Int64 clip_upper;
    const bool has_properties;
    std::vector<MvtProperty> properties;

    static MvtValueKind kindForType(const DataTypePtr & element_type, const String & function_name)
    {
        const DataTypePtr base = removeNullable(recursiveRemoveLowCardinality(element_type));
        WhichDataType which(base);

        if (which.isStringOrFixedString())
            return MvtValueKind::String;
        if (which.isFloat32())
            return MvtValueKind::Float;
        if (which.isFloat64())
            return MvtValueKind::Double;
        if (which.isNativeInt())
            return MvtValueKind::Int;
        if (which.isNativeUInt() || which.isDate() || which.isDateTime())
            return MvtValueKind::UInt;

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Property of type {} is not supported by aggregate function {}. Supported types are String, FixedString, "
            "Float32, Float64, (U)Int8/16/32/64, Date and DateTime (optionally Nullable and/or LowCardinality); cast "
            "other types with toString() or a numeric cast",
            element_type->getName(),
            function_name);
    }

    static void appendFixedInt32(String & out, Int32 value)
    {
        const UInt32 bits = static_cast<UInt32>(value);
        for (size_t i = 0; i < sizeof(bits); ++i)
            out.push_back(static_cast<char>((bits >> (8 * i)) & 0xFF));
    }

    static Int32 readFixedInt32(const char *& pos, const char * end)
    {
        if (sizeof(UInt32) > static_cast<size_t>(end - pos))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted mvtEncode aggregate state: truncated coordinate");
        UInt32 bits = 0;
        for (size_t i = 0; i < sizeof(bits); ++i)
            bits |= static_cast<UInt32>(static_cast<unsigned char>(*pos++)) << (8 * i);
        return static_cast<Int32>(bits);
    }

    static UInt64 readVarint(const char *& pos, const char * end)
    {
        UInt64 result = 0;
        int shift = 0;
        while (true)
        {
            if (pos >= end || shift >= 64)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted mvtEncode aggregate state: truncated varint");
            const unsigned char byte = static_cast<unsigned char>(*pos++);
            result |= static_cast<UInt64>(byte & 0x7F) << shift;
            if (!(byte & 0x80))
                break;
            shift += 7;
        }
        return result;
    }

    /// Render one property value as the body of an MVT `Value` message into `out`.
    void renderValue(const MvtProperty & property, const IColumn & column, size_t row, String & out) const
    {
        switch (property.kind)
        {
            case MvtValueKind::String:
            {
                const std::string_view value = column.getDataAt(row);
                MVT::writeLengthDelimitedField(out, 1, value);
                return;
            }
            case MvtValueKind::Float:
                MVT::writeFloatField(out, 2, column.getFloat32(row));
                return;
            case MvtValueKind::Double:
                MVT::writeDoubleField(out, 3, column.getFloat64(row));
                return;
            case MvtValueKind::Int:
                MVT::writeVarintField(out, 4, static_cast<UInt64>(column.getInt(row)));
                return;
            case MvtValueKind::UInt:
                MVT::writeVarintField(out, 5, column.getUInt(row));
                return;
        }
    }

public:
    AggregateFunctionMvtEncode(
        const DataTypes & argument_types_, const Array & parameters_, String layer_name_, UInt32 extent_, bool has_clip_, UInt32 buffer_)
        : IAggregateFunctionDataHelper<AggregateFunctionMvtEncodeData, AggregateFunctionMvtEncode>(
              argument_types_, parameters_, std::make_shared<DataTypeString>())
        , layer_name(std::move(layer_name_))
        , extent(extent_)
        , has_clip(has_clip_)
        , clip_lower(-static_cast<Int64>(buffer_))
        , clip_upper(static_cast<Int64>(extent_) + static_cast<Int64>(buffer_))
        , has_properties(argument_types_.size() == 2)
    {
        const auto * geometry_type = typeid_cast<const DataTypeTuple *>(argument_types_[0].get());
        if (!geometry_type || geometry_type->getElements().size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The geometry (first) argument of aggregate function {} must be a tuple of two numbers (tile-space "
                "pixel coordinates), e.g. the result of mvtEncodeGeom",
                getName());
        for (const auto & element : geometry_type->getElements())
        {
            if (!isNumber(removeNullable(recursiveRemoveLowCardinality(element))))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "The geometry (first) argument of aggregate function {} must be a tuple of two numbers",
                    getName());
        }

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
            properties.reserve(elements.size());
            for (size_t i = 0; i < elements.size(); ++i)
                properties.push_back({names[i], kindForType(elements[i], getName())});
        }
    }

    String getName() const override { return "mvtEncode"; }

    bool allocatesMemoryInArena() const override { return true; }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        const auto & geometry = assert_cast<const ColumnTuple &>(*columns[0]);
        const Int32 pixel_x = static_cast<Int32>(std::lround(geometry.getColumn(0).getFloat64(row_num)));
        const Int32 pixel_y = static_cast<Int32>(std::lround(geometry.getColumn(1).getFloat64(row_num)));

        /// Clip to the tile expanded by the buffer (in pixel/extent units); points outside it are dropped.
        if (has_clip && (pixel_x < clip_lower || pixel_x > clip_upper || pixel_y < clip_lower || pixel_y > clip_upper))
            return;

        String record;
        appendFixedInt32(record, pixel_x);
        appendFixedInt32(record, pixel_y);

        if (has_properties)
        {
            const auto & properties_column = assert_cast<const ColumnTuple &>(*columns[1]);
            for (size_t i = 0; i < properties.size(); ++i)
            {
                const IColumn & column = properties_column.getColumn(i);
                if (column.isNullAt(row_num))
                {
                    record.push_back(0);
                    continue;
                }
                record.push_back(1);
                String value;
                renderValue(properties[i], column, row_num, value);
                MVT::writeVarint(record, value.size());
                record.append(value);
            }
        }

        auto & state = this->data(place);
        state.append(record.data(), record.size(), arena);
        ++state.num_features;
    }

    void merge(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        const auto & rhs_state = this->data(rhs);
        if (rhs_state.data_size == 0)
            return;
        auto & state = this->data(place);
        state.append(rhs_state.data, rhs_state.data_size, arena);
        state.num_features += rhs_state.num_features;
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        const auto & state = this->data(place);
        writeVarUInt(state.num_features, buf);
        writeVarUInt(state.data_size, buf);
        buf.write(state.data, state.data_size);
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        auto & state = this->data(place);
        if (state.data_size != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA, "mvtEncode deserialize() expects an empty state");

        readVarUInt(state.num_features, buf);

        UInt64 size = 0;
        readVarUInt(size, buf);
        static constexpr UInt64 max_data_size = UInt64{1} << 48;
        if (size > max_data_size)
            throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid mvtEncode state: data size {} is too large", size);

        state.reserveForAppend(size, arena);
        buf.readStrict(state.data, size);
        state.data_size = size;
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        auto & state = this->data(place);
        if (state.num_features == 0)
        {
            to.insertDefault();
            return;
        }

        std::vector<String> value_pool;
        std::unordered_map<String, UInt32> value_index;

        String features;
        const char * pos = state.data;
        const char * end = state.data + state.data_size;

        for (UInt64 feature = 0; feature < state.num_features; ++feature)
        {
            const Int32 pixel_x = readFixedInt32(pos, end);
            const Int32 pixel_y = readFixedInt32(pos, end);

            String tags;
            for (size_t i = 0; i < properties.size(); ++i)
            {
                if (pos >= end)
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted mvtEncode aggregate state: truncated property");
                const bool present = *pos++ != 0;
                if (!present)
                    continue;

                const UInt64 length = readVarint(pos, end);
                /// Compare sizes rather than pointers: `length` is read from the (possibly corrupted) state and could be
                /// huge, so `pos + length` would overflow the pointer and the check could be bypassed.
                if (length > static_cast<UInt64>(end - pos))
                    throw Exception(ErrorCodes::INCORRECT_DATA, "Corrupted mvtEncode aggregate state: truncated value");
                std::string_view value(pos, length);
                pos += length;

                auto [it, inserted] = value_index.try_emplace(String(value), static_cast<UInt32>(value_pool.size()));
                if (inserted)
                    value_pool.emplace_back(value);

                MVT::writeVarint(tags, i); /// key index (keys follow tuple element order)
                MVT::writeVarint(tags, it->second); /// value index
            }

            /// A single point: MoveTo (command id 1, count 1) -> (1 << 3) | 1 == 9, then the zigzagged delta from (0, 0).
            String geometry;
            MVT::writeVarint(geometry, 9);
            MVT::writeVarint(geometry, MVT::zigzag(pixel_x));
            MVT::writeVarint(geometry, MVT::zigzag(pixel_y));

            String feature_message;
            MVT::writeVarintField(feature_message, 3, 1); /// type = POINT
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

AggregateFunctionPtr createAggregateFunctionMvtEncode(
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
            "Aggregate function {} requires a layer name parameter, e.g. mvtEncode('layer')(geometry, properties)",
            name);
    if (parameters.size() > 3)
        throw Exception(
            ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
            "Aggregate function {} accepts at most 3 parameters (layer_name[, extent[, buffer]]), got {}",
            name,
            parameters.size());

    if (parameters[0].getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first parameter (layer_name) of aggregate function {} must be a string", name);
    const String layer_name = parameters[0].safeGet<String>();

    UInt32 extent = 4096;
    if (parameters.size() >= 2)
    {
        const auto type = parameters[1].getType();
        if (type != Field::Types::UInt64 && type != Field::Types::Int64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second parameter (extent) of aggregate function {} must be a positive integer", name);
        const Int64 value = type == Field::Types::UInt64 ? static_cast<Int64>(parameters[1].safeGet<UInt64>()) : parameters[1].safeGet<Int64>();
        if (value <= 0 || value > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The second parameter (extent) of aggregate function {} must be in the range [1, 4294967295], got {}", name, value);
        extent = static_cast<UInt32>(value);
    }

    /// The optional third parameter enables clipping: features whose tile-pixel coordinates fall outside
    /// [-buffer, extent + buffer] are dropped during aggregation. When absent, no clipping is performed.
    bool has_clip = false;
    UInt32 buffer = 0;
    if (parameters.size() == 3)
    {
        const auto type = parameters[2].getType();
        if (type != Field::Types::UInt64 && type != Field::Types::Int64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The third parameter (buffer) of aggregate function {} must be a non-negative integer", name);
        const Int64 value = type == Field::Types::UInt64 ? static_cast<Int64>(parameters[2].safeGet<UInt64>()) : parameters[2].safeGet<Int64>();
        if (value < 0 || value > std::numeric_limits<UInt32>::max())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The third parameter (buffer) of aggregate function {} must be in the range [0, 4294967295], got {}", name, value);
        has_clip = true;
        buffer = static_cast<UInt32>(value);
    }

    return std::make_shared<AggregateFunctionMvtEncode>(argument_types, parameters, layer_name, extent, has_clip, buffer);
}

}

void registerAggregateFunctionMvtEncode(AggregateFunctionFactory & factory)
{
    const AggregateFunctionProperties properties = {.returns_default_when_only_null = false, .is_order_dependent = true};

    FunctionDocumentation::Description description = R"(
Encodes a group of point features into a binary [Mapbox Vector Tile](https://github.com/mapbox/vector-tile-spec) layer.

This is the aggregate counterpart of the scalar function `mvtEncodeGeom`. Each input
row becomes one point feature. The geometry argument must be a tuple of tile-space pixel coordinates `(pixel_x, pixel_y)`
(typically produced by `mvtEncodeGeom`); the optional properties argument is a named tuple whose element names become the
feature attribute keys and whose element types determine the vector tile value types.

The properties tuple must have explicit element names. Column aliases inside `tuple(...)` are not propagated to tuple
element names, so name the elements with a cast, for example `tuple(count(), any(id))::Tuple(cluster_count UInt64, id String)`.

Clustering is expressed in SQL, not by the function: aggregate the rows that share a pixel in a subquery (for example with
`count()` and `any()` grouped by the pixel coordinates), then pass one row per cluster to `mvtEncode`. The result is the raw
bytes of a single-layer tile, which can be returned directly over the HTTP interface with `FORMAT RawBLOB`.

When the optional `buffer` parameter is given, the tile is clipped: features whose pixel coordinates fall outside the tile
expanded by `buffer` pixels (the range `[-buffer, extent + buffer]`) are dropped. This lets the `WHERE` clause use a coarse
bounding-box prefilter (see `mvtTileBBox`) for performance while the exact tile boundary is enforced here.

Supported property element types are String, FixedString, Float32, Float64, (U)Int8/16/32/64, Date and DateTime,
optionally wrapped in Nullable and/or LowCardinality; a NULL value omits that attribute for the feature. Only point
geometry is supported.
    )";
    FunctionDocumentation::Syntax syntax = "mvtEncode(layer_name[, extent[, buffer]])(geometry[, properties])";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "Tile-space pixel coordinates of the point as a tuple `(pixel_x, pixel_y)`, e.g. from `mvtEncodeGeom`.", {"Tuple(Float64, Float64)"}},
        {"properties", "Optional named tuple of feature attributes. Element names become attribute keys.", {"Tuple"}},
    };
    FunctionDocumentation::Parameters parameters = {
        {"layer_name", "Name of the vector tile layer.", {"String"}},
        {"extent", "Tile extent in pixels per side. Defaults to `4096`.", {"UInt32"}},
        {"buffer", "Optional clip buffer in pixels. When given, features outside `[-buffer, extent + buffer]` are dropped.", {"UInt32"}},
    };
    FunctionDocumentation::ReturnedValue returned_value
        = {"Returns the binary contents of a single-layer Mapbox Vector Tile.", {"String"}};
    FunctionDocumentation::Examples examples = {
        {
            "Encode a clustered tile",
            R"(
SELECT mvtEncode('points')(geom, tuple(cluster_count)::Tuple(cluster_count UInt64)) AS tile
FROM
(
    SELECT mvtEncodeGeom(lon, lat, 10, 550, 335) AS geom, count() AS cluster_count
    FROM points
    GROUP BY geom
);
            )",
            "",
        },
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 7};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;
    FunctionDocumentation documentation = {description, syntax, arguments, parameters, returned_value, examples, introduced_in, category};

    factory.registerFunction("mvtEncode", {createAggregateFunctionMvtEncode, documentation, properties});
}

}
