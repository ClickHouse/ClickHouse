#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeNullable.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class FunctionFlipCoordinates : public IFunction
{
public:
    static constexpr auto name = "flipCoordinates";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionFlipCoordinates>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }

    bool useDefaultImplementationForNulls() const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

private:
    static const IDataType * stripNullable(const IDataType & t)
    {
        if (const auto * nt = typeid_cast<const DataTypeNullable *>(&t))
            return nt->getNestedType().get();
        return &t;
    }

    static bool isNumericOrDecimal(const IDataType & t)
    {
        const WhichDataType which(t);
        return which.isInt() || which.isUInt() || which.isFloat() || which.isDecimal();
    }

    static bool isPointTupleType(const IDataType & t0)
    {
        const IDataType * t = stripNullable(t0);
        const auto * tup = typeid_cast<const DataTypeTuple *>(t);
        if (!tup) return false;

        const auto & elems = tup->getElements();
        if (elems.size() != 2) return false;

        const IDataType * a = stripNullable(*elems[0]);
        const IDataType * b = stripNullable(*elems[1]);

        if (a->getName() != b->getName()) return false;
        return isNumericOrDecimal(*a);
    }

    // Check if type is a Point tuple or nested arrays with Point tuple at the base.
    // Examples: Tuple(T,T), Array(Tuple(T,T)), Array(Array(Tuple(T,T))), etc.
    static bool hasPointTupleBase(const IDataType & t0)
    {
        const IDataType * current_type = stripNullable(t0);

        while (const auto * arr = typeid_cast<const DataTypeArray *>(current_type))
        {
            const auto & nested = arr->getNestedType();
            current_type = stripNullable(*nested);
        }

        // Check if the base type is a Point tuple.
        return isPointTupleType(*current_type);
    }

public:
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1)
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} takes exactly one argument, got {}",
                getName(),
                arguments.size());

        const auto & t = arguments[0];

        if (hasPointTupleBase(*t))
            return t;

        if (const auto * vt = checkAndGetDataType<DataTypeVariant>(t.get()))
        {
            for (const auto & alt : vt->getVariants())
            {
                if (!hasPointTupleBase(*alt))
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {}: Variant alternative {} is not supported; "
                        "expected Tuple(T,T) with numeric/decimal T, or arrays thereof.",
                        getName(),
                        alt->getName());
            }
            return t;
        }

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}. "
            "Expected Tuple(T,T), Array(...), or Variant(...) thereof.",
            t->getName(),
            getName());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments,
                          const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        const auto & arg = arguments[0];

        // Variant (Geometry) -  process each alternative subcolumn in bulk.
        if (const auto * vt = checkAndGetDataType<DataTypeVariant>(arg.type.get()))
            return executeForVariantBulk(*vt, assert_cast<const ColumnVariant &>(*arg.column));

        if (isPointTupleType(*arg.type))
            return executeForPoint(*arg.column);

        if (const auto * array_type = checkAndGetDataType<DataTypeArray>(arg.type.get()))
            return executeForArray(*arg.column, array_type);

        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Illegal type {} of argument of function {}. Expected Point (Tuple), LineString (Array(Point)), Ring (Array(Point)), Polygon "
            "(Array(Ring)), MultiLineString (Array(LineString)), MultiPolygon (Array(Polygon)), or Geometry (Variant)",
            arg.type->getName(),
            getName());
    }

private:
    ColumnPtr executeForPoint(const IColumn & column) const
    {
        const auto * column_tuple = checkAndGetColumn<ColumnTuple>(&column);
        if (!column_tuple)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column {} of first argument of function {}",
                            column.getName(), getName());

        const auto & tuple_columns = column_tuple->getColumns();
        if (tuple_columns.size() != 2)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} expects Tuple with exactly 2 elements (x,y), found {}",
                getName(),
                tuple_columns.size());

        const Columns swapped = { tuple_columns[1], tuple_columns[0] };
        return ColumnTuple::create(swapped);
    }

    ColumnPtr executeForArray(const IColumn & column, const DataTypeArray * array_type) const
    {
        // Unwrap all array layers, storing offset columns at each level.
        std::vector<ColumnPtr> offset_columns;
        const IColumn * current_col = &column;
        const DataTypeArray * current_type = array_type;

        while (true)
        {
            const auto * col_array = checkAndGetColumn<ColumnArray>(current_col);
            if (!col_array)
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                                "Illegal column {} of first argument of function {}",
                                current_col->getName(), getName());

            offset_columns.push_back(col_array->getOffsetsPtr());
            current_col = &col_array->getData();

            const auto & nested_type_ptr = current_type->getNestedType();
            const IDataType * nested_nn  = stripNullable(*nested_type_ptr);

            if (const auto * next_arr = checkAndGetDataType<DataTypeArray>(nested_nn))
            {
                current_type = next_arr;
                continue;
            }

            if (!isPointTupleType(*nested_nn))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                "Illegal nested type {} of argument of function {}",
                                nested_type_ptr->getName(), getName());
            break;
        }

        // Process the innermost column (Point tuple).
        ColumnPtr result = executeForPoint(*current_col);

        // Re-wrap with all array layers in reverse order.
        for (auto it = offset_columns.rbegin(); it != offset_columns.rend(); ++it)
            result = ColumnArray::create(result, *it);

        return result;
    }

    ColumnPtr flipTupleOrArray(const IDataType & type, const IColumn & col) const
    {
        if (isPointTupleType(type))
            return executeForPoint(col);

        if (const auto * arr_t = checkAndGetDataType<DataTypeArray>(&type))
            return executeForArray(col, arr_t);

        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "flipCoordinates: unsupported for type: {}", type.getName());
    }

    ColumnPtr executeForVariantBulk(const DataTypeVariant & variant_type,
                                    const ColumnVariant & column_variant) const
    {
        const auto & alts = variant_type.getVariants();

        Columns result_columns(alts.size());

        for (size_t j = 0; j < alts.size(); ++j)
        {
            const IDataType & alt_t = *alts[j];
            const IColumn & alt_col = column_variant.getVariantByLocalDiscriminator(j);

            if (hasPointTupleBase(alt_t))
                result_columns[j] = flipTupleOrArray(alt_t, alt_col);
            else
                result_columns[j] = alt_t.createColumn();
        }

        auto discriminators_column = ColumnVariant::ColumnDiscriminators::create();
        discriminators_column->reserve(column_variant.size());
        const auto & src = column_variant.getLocalDiscriminators();
        for (size_t i = 0; i < column_variant.size(); ++i)
            discriminators_column->insertValue(src[i]);

        return ColumnVariant::create(std::move(discriminators_column), result_columns);
    }
};

REGISTER_FUNCTION(FlipCoordinates)
{
    FunctionDocumentation::Description description =
        "Flips coordinates for points and recursively for arrays of points. "
        "Accepts Tuple(T,T) with numeric/decimal T (point), Array(...Array(point)...), "
        "and Geometry as Variant of those (Point, LineString, Ring, Polygon, MultiLineString, MultiPolygon). "
        "The return type equals the input type.";

    FunctionDocumentation::Syntax syntax = "flipCoordinates(geometry)";

    FunctionDocumentation::Arguments arguments = {
        {"geometry", "The geometry to transform. Supported: "
                     "Point (Tuple(T,T)), LineString (Array(Point)), Ring (Array(Point)), "
                     "Polygon (Array(Ring)), MultiLineString (Array(LineString)), "
                     "MultiPolygon (Array(Polygon)), Geometry (Variant of these)." }
    };

    FunctionDocumentation::ReturnedValue returned_value =
        {"The geometry with flipped coordinates. The type is the same as the input.",
         {"Point", "LineString", "Ring", "Polygon", "MultiLineString", "MultiPolygon", "Geometry"}};

    FunctionDocumentation::Examples examples = {
        {"basic_point", "SELECT flipCoordinates((1.0, 2.0));", "(2.0, 1.0)"},
        {"ring",        "SELECT flipCoordinates([(1.0, 2.0), (3.0, 4.0)]);",
                         "[(2.0, 1.0), (4.0, 3.0)]"},
        {"polygon",     "SELECT flipCoordinates([[(1.0, 2.0), (3.0, 4.0)], [(5.0, 6.0), (7.0, 8.0)]]);",
                         "[[(2.0, 1.0), (4.0, 3.0)], [(6.0, 5.0), (8.0, 7.0)]]"}
    };

    FunctionDocumentation::IntroducedIn introduced_in = {25, 10};
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

