#include <Functions/FunctionsExternalModels.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalModels.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <ext/range.h>

namespace DB
{

FunctionPtr FunctionModelEvaluate::create(const Context & context)
{
    return std::make_shared<FunctionModelEvaluate>(context.getExternalModels());
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

DataTypePtr FunctionModelEvaluate::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 2)
        throw Exception("Function " + getName() + " expects at least 2 arguments",
                        ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);

    if (!checkDataType<DataTypeString>(arguments[0].get()))
        throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                        + ", expected a string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    return std::make_shared<DataTypeFloat64>();
}

void FunctionModelEvaluate::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto name_col = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
    if (!name_col)
        throw Exception("First argument of function " + getName() + " must be a constant string",
                        ErrorCodes::ILLEGAL_COLUMN);

    auto model = models.getModel(name_col->getValue<String>());

    ConstColumnPlainPtrs columns;
    columns.reserve(arguments.size());
    for (auto i : ext::range(1, arguments.size()))
        columns.push_back(block.getByPosition(arguments[i]).column.get());

    block.getByPosition(result).column = model->evaluate(columns);
}

void registerFunctionsExternalModels(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModelEvaluate>();
}

}
