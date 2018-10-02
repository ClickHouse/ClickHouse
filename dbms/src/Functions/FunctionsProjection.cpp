#include <memory>
#include <vector>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionsProjection.h>

namespace DB
{
FunctionPtr FunctionOneOrZero::create(const Context &)
{
    return std::make_shared<FunctionOneOrZero>();
}

String FunctionOneOrZero::getName() const
{
    return name;
}

size_t FunctionOneOrZero::getNumberOfArguments() const
{
    return 1;
}

DataTypePtr FunctionOneOrZero::getReturnTypeImpl(const DataTypes & /*arguments*/) const
{
    return std::make_shared<DataTypeUInt8>();
}

void FunctionOneOrZero::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const auto & data_column = block.getByPosition(arguments[0]).column;
    auto col_res = ColumnUInt8::create();
    auto & vec_res = col_res->getData();
    vec_res.resize(data_column->size());
    for (size_t i = 0; i < data_column->size(); ++i)
    {
        if (data_column->getBool(i))
        {
            vec_res[i] = 1;
        }
        else
        {
            vec_res[i] = 0;
        }
    }
    block.getByPosition(result).column = std::move(col_res);
}

FunctionPtr FunctionProject::create(const Context &)
{
    return std::make_shared<FunctionProject>();
}

String FunctionProject::getName() const
{
    return name;
}

size_t FunctionProject::getNumberOfArguments() const
{
    return 2;
}

DataTypePtr FunctionProject::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (!checkAndGetDataType<DataTypeUInt8>(arguments[1].get()))
    {
        throw Exception(
            "Illegal type " + arguments[1]->getName() + " of 2nd argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }
    return arguments[0];
}

void FunctionProject::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const auto & data_column = block.getByPosition(arguments[0]).column;
    const auto & projection_column = block.getByPosition(arguments[1]).column;
    if (const auto projection_column_uint8 = checkAndGetColumn<ColumnUInt8>(projection_column.get()))
    {
        block.getByPosition(result).column = data_column->filter(projection_column_uint8->getData(), -1);
    }
    else if (const auto projection_column_uint8_const = checkAndGetColumnConst<ColumnUInt8>(projection_column.get()))
    {
        if (projection_column_uint8_const->getBool(0))
        {
            block.getByPosition(result).column = data_column->cloneResized(data_column->size());
        }
        else
        {
            block.getByPosition(result).column = data_column->cloneEmpty();
        }
    }
    else
    {
        throw Exception("Unexpected column: " + projection_column->getName(), ErrorCodes::ILLEGAL_COLUMN);
    }
}

FunctionPtr FunctionBuildProjectionComposition::create(const Context &)
{
    return std::make_shared<FunctionBuildProjectionComposition>();
}

String FunctionBuildProjectionComposition::getName() const
{
    return name;
}

size_t FunctionBuildProjectionComposition::getNumberOfArguments() const
{
    return 2;
}

DataTypePtr FunctionBuildProjectionComposition::getReturnTypeImpl(const DataTypes & arguments) const
{
    for (size_t i = 0; i < 2; ++i)
    {
        if (!checkAndGetDataType<DataTypeUInt8>(arguments[i].get()))
        {
            throw Exception(
                "Illegal type " + arguments[i]->getName() + " of " + std::to_string(i + 1) + " argument of function " + getName(),
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }
    return std::make_shared<DataTypeUInt8>();
}

void FunctionBuildProjectionComposition::executeImpl(
    Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    const auto & first_projection_column = block.getByPosition(arguments[0]).column;
    const auto & second_projection_column = block.getByPosition(arguments[1]).column;
    auto col_res = ColumnUInt8::create();
    auto & vec_res = col_res->getData();
    vec_res.resize(first_projection_column->size());
    size_t current_reserve_index = 0;
    for (size_t i = 0; i < first_projection_column->size(); ++i)
    {
        if (!first_projection_column->getBool(i))
        {
            vec_res[i] = 0;
        }
        else
        {
            vec_res[i] = second_projection_column->getBool(current_reserve_index);
            ++current_reserve_index;
        }
    }
    if (current_reserve_index != second_projection_column->size())
    {
        throw Exception("Second argument size is not appropriate: " + std::to_string(second_projection_column->size()) + " instead of  "
                + std::to_string(current_reserve_index),
            ErrorCodes::BAD_ARGUMENTS);
    }
    block.getByPosition(result).column = std::move(col_res);
}

FunctionPtr FunctionRestoreProjection::create(const Context &)
{
    return std::make_shared<FunctionRestoreProjection>();
}

String FunctionRestoreProjection::getName() const
{
    return name;
}

bool FunctionRestoreProjection::isVariadic() const
{
    return true;
}

size_t FunctionRestoreProjection::getNumberOfArguments() const
{
    return 0;
}

DataTypePtr FunctionRestoreProjection::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 2)
    {
        throw Exception("Wrong argument count: " + std::to_string(arguments.size()), ErrorCodes::BAD_ARGUMENTS);
    }
    return arguments[1];
}

void FunctionRestoreProjection::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    if (arguments.size() < 2)
    {
        throw Exception("Wrong argument count: " + std::to_string(arguments.size()), ErrorCodes::BAD_ARGUMENTS);
    }
    const auto & projection_column = block.getByPosition(arguments[0]).column;
    auto col_res = block.getByPosition(arguments[1]).column->cloneEmpty();
    std::vector<size_t> override_indices(arguments.size() - 1, 0);
    for (size_t i = 0; i < projection_column->size(); ++i)
    {
        size_t argument_index = projection_column->getBool(i);
        col_res->insertFrom(*block.getByPosition(arguments[argument_index + 1]).column, override_indices[argument_index]++);
    }
    block.getByPosition(result).column = std::move(col_res);
}

}
