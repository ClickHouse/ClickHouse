#include <Functions/FunctionJoinGet.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Join.h>
#include <Storages/StorageJoin.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

static auto getJoin(const ColumnsWithTypeAndName & arguments, const Context & context)
{
    if (arguments.size() != 3)
        throw Exception{"Function joinGet takes 3 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    String join_name;
    if (auto name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
    {
        join_name = name_col->getValue<String>();
    }
    else
        throw Exception{"Illegal type " + arguments[0].type->getName() + " of first argument of function joinGet, expected a const string.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    size_t dot = join_name.find('.');
    String database_name;
    if (dot == String::npos)
    {
        database_name = context.getCurrentDatabase();
        dot = 0;
    }
    else
    {
        database_name = join_name.substr(0, dot);
        ++dot;
    }
    String table_name = join_name.substr(dot);
    auto table = context.getTable(database_name, table_name);
    auto storage_join = std::dynamic_pointer_cast<StorageJoin>(table);
    if (!storage_join)
        throw Exception{"Table " + join_name + " should have engine StorageJoin", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    String attr_name;
    if (auto name_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
    {
        attr_name = name_col->getValue<String>();
    }
    else
        throw Exception{"Illegal type " + arguments[1].type->getName()
                            + " of second argument of function joinGet, expected a const string.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    return std::make_pair(storage_join, attr_name);
}

FunctionBasePtr FunctionBuilderJoinGet::buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
{
    auto [storage_join, attr_name] = getJoin(arguments, context);
    auto join = storage_join->getJoin();
    DataTypes data_types(arguments.size());

    auto table_lock = storage_join->lockStructureForShare(false, context.getCurrentQueryId());
    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    auto return_type = join->joinGetReturnType(attr_name);
    return std::make_shared<DefaultFunction>(
        std::make_shared<FunctionJoinGet>(table_lock, storage_join, join, attr_name, return_type), data_types, return_type);
}

DataTypePtr FunctionBuilderJoinGet::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    auto [storage_join, attr_name] = getJoin(arguments, context);
    auto join = storage_join->getJoin();
    return join->joinGetReturnType(attr_name);
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
