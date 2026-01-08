#include <Functions/array/arrayConcat.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <base/range.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

DataTypePtr FunctionArrayConcat::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.empty())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires at least one argument.", getName());

    for (auto i : collections::range(0, arguments.size()))
    {
        const auto * array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
        if (!array_type)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                            "Argument {} for function {} must be an array but it has type {}.",
                            i, getName(), arguments[i]->getName());
    }

    return getLeastSupertype(arguments);
}

ColumnPtr FunctionArrayConcat::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const
{
    if (result_type->onlyNull())
        return result_type->createColumnConstWithDefaultValue(input_rows_count);

    size_t num_args = arguments.size();

    Columns preprocessed_columns(num_args);

    for (size_t i = 0; i < num_args; ++i)
    {
        const ColumnWithTypeAndName & arg = arguments[i];
        ColumnPtr preprocessed_column = arg.column;

        if (!arg.type->equals(*result_type))
            preprocessed_column = castColumn(arg, result_type);

        preprocessed_columns[i] = std::move(preprocessed_column);
    }

    std::vector<std::unique_ptr<GatherUtils::IArraySource>> sources;

    for (auto & argument_column : preprocessed_columns)
    {
        bool is_const = false;

        if (const auto * argument_column_const = typeid_cast<const ColumnConst *>(argument_column.get()))
        {
            is_const = true;
            argument_column = argument_column_const->getDataColumnPtr();
        }

        if (const auto * argument_column_array = typeid_cast<const ColumnArray *>(argument_column.get()))
            sources.emplace_back(GatherUtils::createArraySource(*argument_column_array, is_const, input_rows_count));
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments for function {} must be arrays.", getName());
    }

    auto sink = GatherUtils::concat(sources);

    return sink;
}

REGISTER_FUNCTION(ArrayConcat)
{
    factory.registerFunction<FunctionArrayConcat>();
}

}
