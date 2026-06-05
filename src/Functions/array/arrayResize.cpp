#include <Functions/array/arrayResize.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <Interpreters/castColumn.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{
bool isArrayOrNullableArray(const IDataType & type)
{
    if (const auto * nullable = typeid_cast<const DataTypeNullable *>(&type))
        return checkAndGetDataType<DataTypeArray>(nullable->getNestedType().get()) != nullptr;
    return checkAndGetDataType<DataTypeArray>(&type) != nullptr;
}

ColumnPtr convertToStructure(const ColumnPtr & column, const IColumn & structure)
{
    if (column->structureEquals(structure))
        return column;

    auto result = structure.cloneEmpty();
    result->insertRangeFrom(*column, 0, column->size());
    return result;
}
}

DataTypePtr FunctionArrayResize::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{

    FunctionArgumentDescriptors mandatory_args{
        {"array", &isArrayOrNullableArray, nullptr, "Array"},
        {"size", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isNumber), nullptr, "Number"}
    };

    FunctionArgumentDescriptors optional_args{
        {"extender", nullptr, nullptr, "Any type"}
    };

    validateFunctionArguments(*this, arguments, mandatory_args, optional_args);

    if (arguments[0].type->onlyNull())
        return arguments[0].type;

    /// Issue #48398
    if (arguments[1].type->isNullable())
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Second argument for function {} must not be Nullable.", getName());

    if (arguments.size() == 2)
        return arguments[0].type;
    else
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(removeNullable(arguments[0].type).get());
        auto data_types = {array_type->getNestedType(), arguments[2].type};
        DataTypePtr result = std::make_shared<DataTypeArray>(getLeastSupertype(data_types));
        if (arguments[0].type->isNullable())
            return makeNullable(result);
        return result;
    }
}

ColumnPtr FunctionArrayResize::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const
{
    if (return_type->onlyNull())
        return return_type->createColumnConstWithDefaultValue(input_rows_count);

    const ColumnConst * col_const = checkAndGetColumn<ColumnConst>(arguments[0].column.get());
    const IColumn * data_column = col_const ? &col_const->getDataColumn() : arguments[0].column.get();

    const bool argument_type_is_nullable = arguments[0].type->isNullable();
    if (const auto * nullable_array_column = checkAndGetColumn<ColumnNullable>(data_column))
    {
        if (checkAndGetColumn<ColumnArray>(&nullable_array_column->getNestedColumn()))
        {
            ColumnsWithTypeAndName nested_arguments = arguments;
            nested_arguments[0].column = col_const
                ? ColumnConst::create(nullable_array_column->getNestedColumnPtr(), col_const->size())
                : nullable_array_column->getNestedColumnPtr();
            nested_arguments[0].type = removeNullable(arguments[0].type);

            auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

            if (return_type->isNullable())
            {
                auto mutable_nested_result = IColumn::mutate(nested_result->convertToFullColumnIfConst());
                const size_t num_rows = col_const ? col_const->size() : mutable_nested_result->size();
                if (mutable_nested_result->size() == 1 && num_rows != 1)
                    mutable_nested_result = mutable_nested_result->cloneResized(num_rows);

                auto null_map = ColumnUInt8::create();
                auto & null_map_data = null_map->getData();
                null_map_data.assign(
                    nullable_array_column->getNullMapData().begin(), nullable_array_column->getNullMapData().end());
                if (null_map_data.size() == 1)
                    null_map_data.resize_fill(num_rows, nullable_array_column->getNullMapData()[0]);
                else if (null_map_data.size() != num_rows)
                    null_map_data.resize_fill(num_rows, 0);

                return ColumnNullable::create(std::move(mutable_nested_result), std::move(null_map));
            }

            return nested_result;
        }
    }
    else if (argument_type_is_nullable)
    {
        ColumnsWithTypeAndName nested_arguments = arguments;
        nested_arguments[0].type = removeNullable(arguments[0].type);

        auto nested_result = executeImpl(nested_arguments, removeNullable(return_type), input_rows_count);

        if (return_type->isNullable())
        {
            auto null_map = ColumnUInt8::create();
            null_map->getData().resize_fill(nested_result->size(), 0);
            return ColumnNullable::create(nested_result, std::move(null_map));
        }

        return nested_result;
    }

    auto result_column = removeNullable(return_type)->createColumn();

    auto array_column = arguments[0].column;
    auto size_column = arguments[1].column;

    if (!arguments[0].type->equals(*return_type))
        array_column = castColumn(arguments[0], return_type);

    const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*removeNullable(return_type)).getNestedType();
    size_t size = array_column->size();

    std::unique_ptr<GatherUtils::IArraySource> array_source;
    std::unique_ptr<GatherUtils::IValueSource> value_source;

    bool is_const = false;

    if (const auto * const_array_column = typeid_cast<const ColumnConst *>(array_column.get()))
    {
        is_const = true;
        array_column = const_array_column->getDataColumnPtr();
    }

    if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(array_column.get()))
        array_source = GatherUtils::createArraySource(*argument_column_array, is_const, size);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "First arguments for function {} must be array.", getName());

    auto result_column = array_column->cloneEmpty();
    auto & result_array = typeid_cast<ColumnArray &>(*result_column);

    ColumnPtr appended_column;
    if (arguments.size() == 3)
    {
        appended_column = arguments[2].column;
        if (!arguments[2].type->equals(*return_nested_type))
            appended_column = castColumn(arguments[2], return_nested_type);
    }
    else
    {
        auto default_column = result_array.getData().cloneEmpty();
        default_column->insertDefault();
        appended_column = ColumnConst::create(std::move(default_column), size);
    }

    bool is_appended_const = false;
    if (const auto * const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
    {
        is_appended_const = true;
        appended_column = const_appended_column->getDataColumnPtr();
    }

    appended_column = convertToStructure(appended_column, result_array.getData());
    value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

    auto sink = GatherUtils::createArraySink(result_array, size);

    if (isColumnConst(*size_column))
        GatherUtils::resizeConstantSize(*array_source, *value_source, *sink, size_column->getInt(0));
    else
        GatherUtils::resizeDynamicSize(*array_source, *value_source, *sink, *size_column);

    return result_column;
}

REGISTER_FUNCTION(ArrayResize)
{
    FunctionDocumentation::Description description = "Changes the length of the array.";
    FunctionDocumentation::Syntax syntax = "arrayResize(arr, size[, extender])";
    FunctionDocumentation::Arguments arguments = {
        {"arr", "Array to resize.", {"Array(T)"}},
        {"size", R"(
-The new length of the array.
If `size` is less than the original size of the array, the array is truncated from the right.
If `size` is larger than the initial size of the array, the array is extended to the right with `extender` values or default values for the data type of the array items.
)"},
        {"extender", "Value to use for extending the array. Can be `NULL`."}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"An array of length `size`.", {"Array(T)"}};
    FunctionDocumentation::Examples examples = {
        {"Example 1", "SELECT arrayResize([1], 3);", "[1,0,0]"},
        {"Example 2", "SELECT arrayResize([1], 3, NULL);", "[1,NULL,NULL]"},
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Array;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionArrayResize>(documentation);
}

}
