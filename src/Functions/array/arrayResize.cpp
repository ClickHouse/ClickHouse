#include <Functions/array/arrayResize.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/getLeastSupertype.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
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

DataTypePtr FunctionArrayResize::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{

    FunctionArgumentDescriptors mandatory_args{
        {"array", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isArray), nullptr, "Array"},
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
        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[0].type.get());
        auto data_types = {array_type->getNestedType(), arguments[2].type};
        return std::make_shared<DataTypeArray>(getLeastSupertype(data_types));
    }
}

ColumnPtr FunctionArrayResize::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & return_type, size_t input_rows_count) const
{
    if (return_type->onlyNull())
        return return_type->createColumnConstWithDefaultValue(input_rows_count);

    auto result_column = return_type->createColumn();

    auto array_column = arguments[0].column;
    auto size_column = arguments[1].column;

    if (!arguments[0].type->equals(*return_type))
        array_column = castColumn(arguments[0], return_type);

    const DataTypePtr & return_nested_type = typeid_cast<const DataTypeArray &>(*return_type).getNestedType();
    size_t size = array_column->size();

    ColumnPtr appended_column;
    if (arguments.size() == 3)
    {
        appended_column = arguments[2].column;
        if (!arguments[2].type->equals(*return_nested_type))
            appended_column = castColumn(arguments[2], return_nested_type);
    }
    else
        appended_column = return_nested_type->createColumnConstWithDefaultValue(size);

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


    bool is_appended_const = false;
    if (const auto * const_appended_column = typeid_cast<const ColumnConst *>(appended_column.get()))
    {
        is_appended_const = true;
        appended_column = const_appended_column->getDataColumnPtr();
    }

    value_source = GatherUtils::createValueSource(*appended_column, is_appended_const, size);

    auto sink = GatherUtils::createArraySink(typeid_cast<ColumnArray &>(*result_column), size);

    if (isColumnConst(*size_column))
        GatherUtils::resizeConstantSize(*array_source, *value_source, *sink, size_column->getInt(0));
    else
        GatherUtils::resizeDynamicSize(*array_source, *value_source, *sink, *size_column);

    return result_column;
}

REGISTER_FUNCTION(ArrayResize)
{
    factory.registerFunction<FunctionArrayResize>();
}

}
