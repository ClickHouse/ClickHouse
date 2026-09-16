#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>

#include <Common/typeid_cast.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
}

class FunctionArrayInsert final : public IFunction
{
public:
    static constexpr auto name = "arrayInsert";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayInsert>(); }

    String getName() const override { return name; }

    bool isVariadic() const override { return false; }

    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments[0]->onlyNull())
            return arguments[0];

        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
        if (!array_type)
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "First argument for function {} must be an array but it has type {}.",
                getName(),
                arguments[0]->getName());

        if (!isNativeInteger(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Second argument for function {} must be a non-null native integer but it has type {}.",
                getName(),
                arguments[1]->getName());

        DataTypes types = {array_type->getNestedType(), arguments[2]};
        return std::make_shared<DataTypeArray>(getLeastSupertype(types));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const override
    {
        if (return_type->onlyNull())
            return return_type->createColumnConstWithDefaultValue(input_rows_count);

        auto array_column = arguments[0].column;
        auto position_column = arguments[1].column;
        auto inserted_column = arguments[2].column;

        if (!arguments[0].type->equals(*return_type))
            array_column = castColumn(arguments[0], return_type);

        const auto & return_array_type = typeid_cast<const DataTypeArray &>(*return_type);
        const auto & return_nested_type = return_array_type.getNestedType();
        if (!arguments[2].type->equals(*return_nested_type))
            inserted_column = castColumn(arguments[2], return_nested_type);

        std::unique_ptr<GatherUtils::IArraySource> array_source;
        std::unique_ptr<GatherUtils::IValueSource> value_source;

        size_t size = array_column->size();
        bool is_const = false;

        if (const auto * const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
        {
            is_const = true;
            array_column = const_array_column->getDataColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
            array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "First argument for function {} must be an array.", getName());

        bool is_inserted_const = false;
        if (const auto * const_inserted_column = typeid_cast<const ColumnConst *>(inserted_column.get()))
        {
            is_inserted_const = true;
            inserted_column = const_inserted_column->getDataColumnPtr();
        }

        value_source = GatherUtils::createValueSource(*inserted_column, is_inserted_const, size);

        auto result_column = return_type->createColumn();
        auto & result_array = typeid_cast<ColumnArray &>(*result_column);
        auto sink = GatherUtils::createArraySink(result_array, size);

        const bool position_is_unsigned = WhichDataType(arguments[1].type).isNativeUInt();
        if (isColumnConst(*position_column))
        {
            if (position_is_unsigned)
            {
                const auto position = position_column->getUInt(0);
                if (position == 1)
                    GatherUtils::push(*array_source, *value_source, *sink, true);
                else
                    GatherUtils::insertConstantPosition(*array_source, *value_source, *sink, position);
            }
            else
            {
                const auto position = position_column->getInt(0);
                if (position == 1)
                    GatherUtils::push(*array_source, *value_source, *sink, true);
                else if (position == -1)
                    GatherUtils::push(*array_source, *value_source, *sink, false);
                else
                    GatherUtils::insertConstantPosition(*array_source, *value_source, *sink, position);
            }
        }
        else
            GatherUtils::insertDynamicPosition(*array_source, *value_source, *sink, *position_column, position_is_unsigned);

        return result_column;
    }
};

REGISTER_FUNCTION(ArrayInsert)
{
    FunctionDocumentation::Description description = "Inserts one item into an array at the specified position.";
    FunctionDocumentation::Syntax syntax = "arrayInsert(arr, pos, x)";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "The array into which to insert `x`.", {"Array(T)"}},
        {"pos", R"(
Insertion position. Positive positions are 1-based. Negative positions count from the end: `-1` appends, and `-(length(arr) + 1)` prepends.

The position must be a non-null native integer. Position `0` and positions outside the array's insertion range cause an exception.
    )"},
        {"x", "The value to insert. Its type is combined with the array element type in the same way as for `arrayPushBack`."},
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `arr` with `x` inserted at `pos`.", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Positive position", "SELECT arrayInsert([1, 2, 3], 2, 9);", "[1, 9, 2, 3]"},
        {"Negative position", "SELECT arrayInsert([1, 2, 3], -1, 9);", "[1, 2, 3, 9]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 10};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayInsert>(documentation);
}

}
