#include <Columns/ColumnArray.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Common/SipHash.h>
#include <Common/checkStackSize.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Formats/FormatFactory.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunction.h>
#include <Functions/geometry.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/writeJSONNumber.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteBufferFromVector.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/Impl/GeoJSONRowInputFormat.h>
#include <Processors/Formats/Impl/GeoJSONRowOutputFormat.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/// Writes a `JSON` or `Tuple` value as text to be read as GeoJSON, with every floating-point number as `Float64`,
/// at any depth, as a cast does: the shortest text of a `Float32` reads back as another `Float64`.
/// Other values are written by their serialization. Typed paths of `JSON` are written as their text, as a cast
/// between `JSON` types does.
void writeValueAsJSON(const IColumn & column, const DataTypePtr & type, size_t row, WriteBuffer & out, const FormatSettings & settings)
{
    /// The nesting of `Dynamic` values is not limited by the type.
    checkStackSize();

    /// A type with its own text, such as `Bool`, is written as it is.
    if (type->getCustomSerialization())
    {
        type->getDefaultSerialization()->serializeTextJSON(column, row, out, settings);
    }
    else if (const auto * float32 = typeid_cast<const ColumnFloat32 *>(&column))
    {
        writeJSONNumber(static_cast<Float64>(float32->getData()[row]), out, settings);
    }
    else if (const auto * bfloat16 = typeid_cast<const ColumnBFloat16 *>(&column))
    {
        writeJSONNumber(static_cast<Float64>(bfloat16->getData()[row]), out, settings);
    }
    else if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (nullable->isNullAt(row))
            writeCString("null", out);
        else
            writeValueAsJSON(nullable->getNestedColumn(), removeNullable(type), row, out, settings);
    }
    else if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
    {
        writeValueAsJSON(
            *low_cardinality->getDictionary().getNestedColumn(),
            assert_cast<const DataTypeLowCardinality &>(*type).getDictionaryType(),
            low_cardinality->getIndexes().getUInt(row),
            out,
            settings);
    }
    else if (const auto * array = typeid_cast<const ColumnArray *>(&column))
    {
        const auto & nested_type = assert_cast<const DataTypeArray &>(*type).getNestedType();
        const auto & offsets = array->getOffsets();
        writeChar('[', out);
        for (size_t i = offsets[row - 1]; i < offsets[row]; ++i)
        {
            if (i != offsets[row - 1])
                writeChar(',', out);
            writeValueAsJSON(array->getData(), nested_type, i, out, settings);
        }
        writeChar(']', out);
    }
    else if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
        const bool as_object = tuple_type.hasExplicitNames();
        writeChar(as_object ? '{' : '[', out);
        for (size_t i = 0; i < tuple->tupleSize(); ++i)
        {
            if (i != 0)
                writeChar(',', out);
            if (as_object)
            {
                writeJSONString(tuple_type.getElementNames()[i], out, settings);
                writeChar(':', out);
            }
            writeValueAsJSON(tuple->getColumn(i), tuple_type.getElement(i), row, out, settings);
        }
        writeChar(as_object ? '}' : ']', out);
    }
    else if (const auto * variant = typeid_cast<const ColumnVariant *>(&column))
    {
        const auto discriminator = variant->globalDiscriminatorAt(row);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
            writeCString("null", out);
        else
            writeValueAsJSON(
                variant->getVariantByGlobalDiscriminator(discriminator),
                assert_cast<const DataTypeVariant &>(*type).getVariant(discriminator),
                variant->offsetAt(row),
                out,
                settings);
    }
    else if (const auto * dynamic = typeid_cast<const ColumnDynamic *>(&column))
    {
        const auto & variant_column = dynamic->getVariantColumn();
        const auto discriminator = variant_column.globalDiscriminatorAt(row);
        if (discriminator == ColumnVariant::NULL_DISCRIMINATOR)
        {
            writeCString("null", out);
        }
        else if (discriminator == dynamic->getSharedVariantDiscriminator())
        {
            /// A value of a type beyond the limit of `Dynamic` is stored as its type and value in binary.
            ReadBufferFromMemory in(dynamic->getSharedVariant().getDataAt(variant_column.offsetAt(row)));
            const auto value_type = decodeDataType(in);
            if (isNothing(value_type))
            {
                writeCString("null", out);
                return;
            }
            auto value = value_type->createColumn();
            value_type->getDefaultSerialization()->deserializeBinary(*value, in, FormatSettings{});
            writeValueAsJSON(*value, value_type, 0, out, settings);
        }
        else
        {
            writeValueAsJSON(
                variant_column.getVariantByGlobalDiscriminator(discriminator),
                assert_cast<const DataTypeVariant &>(*dynamic->getVariantInfo().variant_type).getVariant(discriminator),
                variant_column.offsetAt(row),
                out,
                settings);
        }
    }
    else
    {
        type->getDefaultSerialization()->serializeTextJSON(column, row, out, settings);
    }
}

/// The geometries are read and written by the code of the `GeoJSON` format, so they follow the same rules and settings.
class FunctionReadGeoJSON final : public IFunction
{
public:
    static constexpr auto name = "readGeoJSON";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionReadGeoJSON>(context); }

    explicit FunctionReadGeoJSON(ContextPtr context)
        : format_settings(getFormatSettings(context))
        , format_settings_hash(getFormatSettingsHash(context->getSettingsRef()))
    {
        /// A `JSON` or `Tuple` value is converted to text to be read, as an object with numbers for coordinates.
        /// The conversion must not lose anything, so it does not use the settings of the session, which are meant
        /// for output, such as `output_format_float_precision`.
        text_settings.json.quote_64bit_integers = false;
        text_settings.json.quote_64bit_floats = false;
        text_settings.json.quote_decimals = false;
        text_settings.json.write_named_tuples_as_objects = true;
    }

    [[nodiscard]] String getName() const override { return name; }

    /// The captured settings decide the result for the same value, see `IFunctionBase::updateHash`.
    void updateHash(SipHash & hash) const override { hash.update(format_settings_hash); }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    /// An empty string is not a geometry.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }

    /// A NULL row must give a NULL geometry, without parsing the default value of the nested column.
    [[nodiscard]] bool useDefaultImplementationForNulls() const override { return false; }

    /// A `geometry` field of a `JSON` column is `Dynamic` if it holds a string, and the adaptor gives NULL for NULL rows.
    [[nodiscard]] bool useDefaultImplementationForDynamic() const override { return true; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto type = removeNullable(arguments[0]);
        /// Schema inference gives a named `Tuple` for a `geometry` object in JSON Lines.
        const auto * tuple = typeid_cast<const DataTypeTuple *>(type.get());
        if (!isString(type) && !isObject(type) && !isNothing(type) && !(tuple && tuple->hasExplicitNames()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} must be String, JSON or a named Tuple, got {}", getName(), arguments[0]->getName());

        return DataTypeFactory::instance().get("Geometry");
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return DataTypeFactory::instance().get("Geometry");
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (arguments[0].type->onlyNull())
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        const IColumn * column = arguments[0].column.get();
        const NullMap * null_map = nullptr;
        if (const auto * nullable = typeid_cast<const ColumnNullable *>(column))
        {
            column = &nullable->getNestedColumn();
            null_map = &nullable->getNullMapData();
        }

        const auto type = removeNullable(arguments[0].type);
        const bool is_string = isString(type);
        const auto discriminators = getGeoJSONGeometryDiscriminators(result_type);

        auto result = result_type->createColumn();
        result->reserve(input_rows_count);

        String serialized;
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            if (null_map && (*null_map)[row])
            {
                result->insertDefault();
                continue;
            }

            std::string_view text;
            if (is_string)
            {
                text = column->getDataAt(row);
            }
            else
            {
                WriteBufferFromString out(serialized);
                writeValueAsJSON(*column, type, row, out, text_settings);
                out.finalize();
                text = serialized;

                /// This is what a subcolumn of a `JSON` column gives for a missing or null field.
                if (text == "{}")
                {
                    result->insertDefault();
                    continue;
                }
            }

            ReadBufferFromMemory in(text);
            readGeoJSONGeometry(in, result.get(), discriminators, format_settings);
            skipWhitespaceIfAny(in);
            assertEOF(in);
        }

        return result;
    }

private:
    /// The settings of the session, used to read the geometry.
    const FormatSettings format_settings;
    /// The hash of the session settings `format_settings` was derived from (see `getFormatSettingsHash`).
    const UInt64 format_settings_hash;
    /// The settings to convert a `JSON` or `Tuple` value to text.
    FormatSettings text_settings;
};

class FunctionGeoJSON final : public IFunction
{
public:
    static constexpr auto name = "geoJSON";

    static FunctionPtr create(ContextPtr context) { return std::make_shared<FunctionGeoJSON>(context); }

    explicit FunctionGeoJSON(ContextPtr context)
        : format_settings(getGeoJSONNumberSettings(getFormatSettings(context)))
        , format_settings_hash(getFormatSettingsHash(context->getSettingsRef()))
    {
    }

    [[nodiscard]] String getName() const override { return name; }

    /// The captured settings decide the result for the same value, see `IFunctionBase::updateHash`.
    void updateHash(SipHash & hash) const override { hash.update(format_settings_hash); }

    [[nodiscard]] size_t getNumberOfArguments() const override { return 1; }

    [[nodiscard]] bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    [[nodiscard]] bool useDefaultImplementationForConstants() const override { return true; }

    /// An empty line or polygon is rejected by `format_geojson_validate_geometry`.
    [[nodiscard]] bool canBeExecutedOnDefaultArguments() const override { return false; }

    [[nodiscard]] DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        /// Throws if the argument is not a geometry.
        (void)getKind(arguments[0]);
        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    [[nodiscard]] ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto kind = getKind(arguments[0].type);

        auto result = ColumnString::create();
        ColumnString::Offsets & offsets = result->getOffsets();
        offsets.resize(input_rows_count);

        WriteBufferFromVector<ColumnString::Chars> out(result->getChars());
        for (size_t row = 0; row < input_rows_count; ++row)
        {
            writeGeoJSONGeometry(kind, *arguments[0].column, row, out, format_settings);
            offsets[row] = out.count();
        }
        out.finalize();

        return result;
    }

private:
    /// An argument of the `Geometry` type gets here as its variants, because of the default implementation for `Variant`.
    [[nodiscard]] GeoJSONGeometryKind getKind(const DataTypePtr & type) const
    {
        auto kind = getGeoJSONGeometryKind(type->getName());
        /// As in `callOnGeometryDataType`, an array of points without a type name is a `Ring`.
        if (const auto geometry_type = getGeometryColumnTypeFromDataType(type); !kind && geometry_type)
            kind = getGeoJSONGeometryKind(String(magic_enum::enum_name(*geometry_type)));

        if (!kind)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "The argument of function {} must be a geometry, got {}", getName(), type->getName());
        return *kind;
    }

    const FormatSettings format_settings;
    /// The hash of the session settings `format_settings` was derived from (see `getFormatSettingsHash`).
    const UInt64 format_settings_hash;
};

}

REGISTER_FUNCTION(GeoJSON)
{
    FunctionDocumentation::IntroducedIn introduced_in = {26, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Geo;

    {
        FunctionDocumentation::Description description = R"(
Parses a [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) geometry object into a `Geometry`, like `ST_GeomFromGeoJSON`.
Coordinates after the first two, such as an altitude, are ignored.
A JSON `null`, a NULL argument and a missing field of a `JSON` column give NULL.
A geometry object in a `JSON` column is a sub-object, so it is read as `j.^geometry`; `j.geometry` gives NULL for it.

The rules are those of the [`GeoJSON`](/reference/formats/GeoJSON) input format, and the same settings apply: `format_geojson_validate_geometry`
checks the shape of lines and rings. A `GeometryCollection` cannot be represented by `Geometry`, so it is an error,
or NULL with `input_format_geojson_unsupported_geometry_handling = 'null'`.
        )";
        FunctionDocumentation::Syntax syntax = "readGeoJSON(geometry)";
        FunctionDocumentation::Arguments arguments = {
            {"geometry", "A GeoJSON geometry object, as text or as the value of a field of a JSON document. Schema inference gives a named `Tuple` for such a field.", {"String", "JSON", "Tuple"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {"The geometry.", {"Geometry"}};
        FunctionDocumentation::Examples examples = {
            {
                "A line",
                R"(SELECT readGeoJSON('{"type":"LineString","coordinates":[[0,0],[1,1]]}') AS g, variantType(g))",
                "[(0,0),(1,1)]\tLineString"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionReadGeoJSON>(documentation);
    }
    {
        FunctionDocumentation::Description description = R"(
Converts a geometry to a [GeoJSON](https://datatracker.ietf.org/doc/html/rfc7946) geometry object, like `ST_AsGeoJSON`.
GeoJSON has no `Ring`, so a `Ring` becomes a `Polygon` with one ring.

The rules are those of the [`GeoJSON`](/reference/formats/GeoJSON) output format, and the same setting applies: `format_geojson_validate_geometry`
rejects lines and rings of a wrong shape, such as an empty `LineString`, so that the result can be read back.
A coordinate that is NaN or infinite is an error, because JSON has no such numbers.
        )";
        FunctionDocumentation::Syntax syntax = "geoJSON(geometry)";
        FunctionDocumentation::Arguments arguments = {
            {"geometry", "A geometry.", {"Point", "MultiPoint", "Ring", "LineString", "MultiLineString", "Polygon", "MultiPolygon", "Geometry"}}
        };
        FunctionDocumentation::ReturnedValue returned_value = {
            "The GeoJSON geometry object. For a `Geometry` argument the type is `Nullable(String)`, and a NULL geometry gives NULL.",
            {"String", "Nullable(String)"}};
        FunctionDocumentation::Examples examples = {
            {
                "A line",
                "SELECT geoJSON([(0., 0.), (1.5, 1.)]::LineString)",
                R"({"type":"LineString","coordinates":[[0,0],[1.5,1]]})"
            }
        };
        FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
        factory.registerFunction<FunctionGeoJSON>(documentation);
    }

    factory.registerAlias("ST_GeomFromGeoJSON", FunctionReadGeoJSON::name);
    factory.registerAlias("ST_AsGeoJSON", FunctionGeoJSON::name);
}

}
