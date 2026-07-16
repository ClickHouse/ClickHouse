#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool variant_throw_on_type_mismatch;
}

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int ILLEGAL_COLUMN;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionFlipCoordinates final : public IFunction
{
public:
    static constexpr auto name = "flipCoordinates";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFlipCoordinates>(); }

    FunctionFlipCoordinates()
    {
        /// Mirror the default Variant adaptor: when variant_throw_on_type_mismatch is disabled, a
        /// populated arm whose type flipCoordinates cannot process yields NULL rows instead of throwing.
        if (CurrentThread::isInitialized())
        {
            if (auto query_context = CurrentThread::tryGetQueryContext())
                throw_on_type_mismatch = query_context->getSettingsRef()[Setting::variant_throw_on_type_mismatch];
        }
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 1; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    /// Handle the Geometry Variant ourselves so the custom `Geometry` type name is preserved.
    /// The generic FunctionBaseVariantAdaptor rebuilds a bare Variant, dropping the custom name.
    bool useDefaultImplementationForVariant() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one argument, got {}",
                getName(),
                arguments.size());

        return arguments[0];
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        const ColumnWithTypeAndName & arg = arguments[0];

        ColumnPtr column = arg.column;
        bool is_const = false;
        size_t const_size = 0;

        if (const auto * const_column = checkAndGetColumn<ColumnConst>(column.get()))
        {
            column = const_column->getDataColumnPtr();
            is_const = true;
            const_size = const_column->size();
        }

        ColumnPtr result;

        if (const auto * variant_type = checkAndGetDataType<DataTypeVariant>(arg.type.get()))
        {
            result = executeForVariant(column, variant_type);
        }
        else if (checkAndGetDataType<DataTypeTuple>(arg.type.get()))
        {
            result = executeForPoint(column);
        }
        else if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.type.get()))
        {
            result = executeForArray(column, array_type);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of argument of function {}. Expected Point (Tuple), Ring (Array(Point)), Polygon (Array(Ring)), or MultiPolygon (Array(Polygon))",
                arg.type->getName(),
                getName());
        }

        if (is_const)
            return ColumnConst::create(result, const_size);

        return result;
    }

private:
    /// Flip each sub-column of a Variant (e.g. the Geometry type) in place, keeping the same
    /// discriminators and offsets. flipCoordinates preserves the geometry structure (Point stays a
    /// Point, Polygon stays a Polygon, ...), so no rows move between variants and the discriminator
    /// layout is unchanged. Reassembling with the original local_to_global mapping keeps the result
    /// column compatible with the input Variant type, so getReturnTypeImpl's `arguments[0]` (the
    /// custom-named `Geometry` type) is the correct result type.
    ColumnPtr executeForVariant(const ColumnPtr & column, const DataTypeVariant * variant_type) const
    {
        const auto * column_variant = checkAndGetColumn<ColumnVariant>(column.get());
        if (!column_variant)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & variant_types = variant_type->getVariants();
        const auto local_to_global = column_variant->getLocalToGlobalDiscriminatorsMapping();
        const size_t num_variants = column_variant->getNumVariants();

        Columns new_variants;
        new_variants.reserve(num_variants);

        /// Local arms that are populated but hold a type flipCoordinates cannot process. Only used
        /// when variant_throw_on_type_mismatch = 0, where such arms' rows become NULL instead of throwing.
        std::vector<bool> arm_nulled(num_variants, false);
        bool any_nulled = false;

        for (size_t local_discr = 0; local_discr < num_variants; ++local_discr)
        {
            const ColumnPtr & sub_column = column_variant->getVariantPtrByLocalDiscriminator(local_discr);
            const DataTypePtr & sub_type = variant_types[local_to_global[local_discr]];

            /// ColumnVariant keeps an empty subcolumn for every declared alternative, even ones with
            /// no rows in the current block. Leave such arms untouched: they carry no data to flip and
            /// may be non-geometry types (e.g. String in Variant(Point, String)) that flipCoordinates
            /// cannot process. Only arms with rows in this block are flipped.
            if (sub_column->empty())
            {
                new_variants.push_back(sub_column);
                continue;
            }

            ColumnPtr flipped;
            if (checkAndGetDataType<DataTypeTuple>(sub_type.get()))
            {
                flipped = executeForPoint(sub_column);
            }
            else if (const auto * array_type = checkAndGetDataType<DataTypeArray>(sub_type.get()))
            {
                flipped = executeForArray(sub_column, array_type);
            }
            else if (!throw_on_type_mismatch)
            {
                /// variant_throw_on_type_mismatch = 0: mirror the default Variant adaptor and null out
                /// this populated unsupported arm rather than throwing. Its rows are reassigned to
                /// NULL_DISCRIMINATOR below; keep an empty column so no discriminator references its data.
                arm_nulled[local_discr] = true;
                any_nulled = true;
                new_variants.push_back(sub_column->cloneEmpty());
                continue;
            }
            else
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal variant type {} of argument of function {}",
                    sub_type->getName(),
                    getName());
            }

            new_variants.push_back(std::move(flipped));
        }

        if (!any_nulled)
            return ColumnVariant::create(
                column_variant->getLocalDiscriminatorsPtr(),
                column_variant->getOffsetsPtr(),
                new_variants,
                local_to_global);

        /// Rebuild the local discriminators, turning rows that point at a nulled arm into NULLs. The
        /// original offsets are reused: they are never read for NULL_DISCRIMINATOR rows, and rows of the
        /// surviving (flipped) arms keep their unchanged offsets since those arms keep their size.
        const auto & old_discriminators = column_variant->getLocalDiscriminators();
        auto new_discriminators_col = ColumnVariant::ColumnDiscriminators::create();
        auto & new_discriminators = new_discriminators_col->getData();
        new_discriminators.reserve(old_discriminators.size());
        for (auto discr : old_discriminators)
        {
            if (discr != ColumnVariant::NULL_DISCRIMINATOR && arm_nulled[discr])
                new_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
            else
                new_discriminators.push_back(discr);
        }

        return ColumnVariant::create(
            std::move(new_discriminators_col),
            column_variant->getOffsetsPtr(),
            new_variants,
            local_to_global);
    }

    ColumnPtr executeForPoint(const ColumnPtr & column) const
    {
        const auto * column_tuple = checkAndGetColumn<ColumnTuple>(column.get());
        if (!column_tuple)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & tuple_columns = column_tuple->getColumns();

        if (tuple_columns.size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects all Tuple elements to have exactly 2 values (x, y), but found a Tuple with {} elements",
                getName(),
                tuple_columns.size());

        for (size_t i = 0; i < 2; ++i)
        {
            const auto * float_col = checkAndGetColumn<ColumnFloat64>(tuple_columns[i].get());
            const auto * const_float_col = checkAndGetColumnConstData<ColumnFloat64>(tuple_columns[i].get());

            if (!float_col && !const_float_col)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Function {} expects tuple elements to be Float64, but element {} has type {}",
                    getName(),
                    i + 1,
                    tuple_columns[i]->getName());
        }

        Columns new_columns = {tuple_columns[1], tuple_columns[0]};
        return ColumnTuple::create(new_columns);
    }

    ColumnPtr executeForArray(const ColumnPtr & column, const DataTypeArray * array_type) const
    {
        const auto * column_array = checkAndGetColumn<ColumnArray>(column.get());
        if (!column_array)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", column->getName(), getName());

        const auto & nested_type = array_type->getNestedType();
        const auto & nested_column = column_array->getDataPtr();

        ColumnPtr result_nested;

        if (checkAndGetDataType<DataTypeTuple>(nested_type.get()))
        {
            result_nested = executeForPoint(nested_column);
        }
        else if (const auto * nested_array = checkAndGetDataType<DataTypeArray>(nested_type.get()))
        {
            result_nested = executeForArray(nested_column, nested_array);
        }
        else
        {
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal nested type {} of argument of function {}",
                nested_type->getName(),
                getName());
        }

        auto offsets_column = column_array->getOffsetsPtr();
        auto result = ColumnArray::create(result_nested, offsets_column);
        return result;
    }

    /// Snapshot of variant_throw_on_type_mismatch at construction. When false, a populated Variant arm
    /// of an unsupported type yields NULL rows instead of ILLEGAL_TYPE_OF_ARGUMENT.
    bool throw_on_type_mismatch = true;
};

REGISTER_FUNCTION(FlipCoordinates)
{
    FunctionDocumentation::Description description = R"(
Flips the x and y coordinates of geometric objects. This operation swaps latitude and longitude, which is useful for converting between different coordinate systems or correcting coordinate order.

For a Point, it swaps the x and y coordinates. For complex geometries (LineString, Polygon, MultiPolygon, Ring, MultiLineString), it recursively applies the transformation to each coordinate pair.

The function supports both individual geometry types (Point, Ring, Polygon, MultiPolygon, LineString, MultiLineString) and the Geometry variant type.
)";
    FunctionDocumentation::Syntax syntax = "flipCoordinates(geometry)";
    FunctionDocumentation::Arguments arguments = {
        {"geometry", "The geometry to transform. Supported types: Point (Tuple(Float64, Float64)), Ring (Array(Point)), Polygon (Array(Ring)), MultiPolygon (Array(Polygon)), LineString (Array(Point)), MultiLineString (Array(LineString)), or Geometry (a variant containing any of these types)."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"The geometry with flipped coordinates. The return type matches the input type.", {"Point", "Ring", "Polygon", "MultiPolygon", "LineString", "MultiLineString", "Geometry"}};
    FunctionDocumentation::Examples examples = {
        {"basic_point",
         "SELECT flipCoordinates((1.0, 2.0));",
         "(2.0, 1.0)"},
        {"ring",
         "SELECT flipCoordinates([(1.0, 2.0), (3.0, 4.0)]);",
         "[(2.0, 1.0), (4.0, 3.0)]"},
        {"polygon",
         "SELECT flipCoordinates([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]]);",
         "[[(2.0, 1.0), (4.0, 3.0)], [(6.0, 5.0), (8.0, 7.0)]]"},
        {"geometry_wkt",
         "SELECT flipCoordinates(readWkt('POINT(10 20)'));",
         "(20, 10)"},
        {"geometry_polygon_wkt",
         "SELECT flipCoordinates(readWkt('POLYGON((0 0, 5 0, 5 5, 0 5, 0 0))'));",
         "[[(0, 0), (0, 5), (5, 5), (5, 0), (0, 0)]]"}
    };
    FunctionDocumentation::IntroducedIn introduced_in = {25, 11};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;

    FunctionDocumentation function_documentation = {
        .description = description,
        .syntax = syntax,
        .arguments = arguments,
        .returned_value = returned_value,
        .examples = examples,
        .introduced_in = introduced_in,
        .category = category
    };

    factory.registerFunction<FunctionFlipCoordinates>(function_documentation);
}

}
