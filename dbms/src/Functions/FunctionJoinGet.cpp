#include <Functions/FunctionJoinGet.h>

#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Join.h>
#include <Storages/StorageJoin.h>
#include <Functions/FunctionFactory.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

FunctionBasePtr FunctionBuilderJoinGet::buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
{
    if (arguments.size() != 3)
        throw Exception{"Function " + getName() + " takes 3 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    String join_name;
    if (auto name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
    {
        join_name = name_col->getValue<String>();
    }
    else
        throw Exception{"Illegal type " + arguments[0].type->getName() + " of first argument of function " + getName()
                            + ", expected a const string.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    auto table = context.getTable("", join_name);

    StorageJoin * storage_join = dynamic_cast<StorageJoin *>(table.get());

    if (!storage_join)
        throw Exception{"Table " + join_name + " should have engine StorageJoin", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    auto join = storage_join->getJoin();
    String attr_name;
    if (auto name_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
    {
        attr_name = name_col->getValue<String>();
    }
    else
        throw Exception{"Illegal type " + arguments[1].type->getName() + " of second argument of function " + getName()
                            + ", expected a const string.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    DataTypes data_types(arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    return std::make_shared<DefaultFunction>(
        std::make_shared<FunctionJoinGet>(join, attr_name), data_types, join->joinGetReturnType(attr_name));
}

void FunctionJoinGet::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t /*input_rows_count*/)
{
    auto & ctn = block.getByPosition(arguments[2]);
    ctn.name = ""; // make sure the key name never collide with the join columns
    Block key_block = {ctn};
    join->joinGet(key_block, attr_name);
    block.getByPosition(result) = key_block.getByPosition(1);
}

void registerFunctionJoinGet(FunctionFactory & factory)
{
    factory.registerFunction<FunctionBuilderJoinGet>();
}

}
