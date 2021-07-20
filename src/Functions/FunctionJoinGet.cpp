#include <Functions/FunctionJoinGet.h>

#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/HashJoin.h>
#include <Columns/ColumnString.h>
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
    if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
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
    auto table = DatabaseCatalog::instance().getTable({database_name, table_name}, context);
    auto storage_join = std::dynamic_pointer_cast<StorageJoin>(table);
    if (!storage_join)
        throw Exception{"Table " + join_name + " should have engine StorageJoin", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

    String attr_name;
    if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
    {
        attr_name = name_col->getValue<String>();
    }
    else
        throw Exception{"Illegal type " + arguments[1].type->getName()
                            + " of second argument of function joinGet, expected a const string.",
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
    return std::make_pair(storage_join, attr_name);
}

template <bool or_null>
FunctionBaseImplPtr JoinGetOverloadResolver<or_null>::build(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
{
    auto [storage_join, attr_name] = getJoin(arguments, context);
    auto join = storage_join->getJoin();
    DataTypes data_types(arguments.size());

    auto table_lock = storage_join->lockForShare(context.getInitialQueryId(), context.getSettingsRef().lock_acquire_timeout);
    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    auto return_type = join->joinGetReturnType(attr_name, or_null);
    return std::make_unique<FunctionJoinGet<or_null>>(table_lock, storage_join, join, attr_name, data_types, return_type);
}

template <bool or_null>
DataTypePtr JoinGetOverloadResolver<or_null>::getReturnType(const ColumnsWithTypeAndName & arguments) const
{
    auto [storage_join, attr_name] = getJoin(arguments, context);
    auto join = storage_join->getJoin();
    return join->joinGetReturnType(attr_name, or_null);
}


template <bool or_null>
void ExecutableFunctionJoinGet<or_null>::execute(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count)
{
    auto ctn = block.getByPosition(arguments[2]);
    if (isColumnConst(*ctn.column))
        ctn.column = ctn.column->cloneResized(1);
    ctn.name = ""; // make sure the key name never collide with the join columns
    Block key_block = {ctn};
    join->joinGet(key_block, attr_name, or_null);
    auto & result_ctn = key_block.getByPosition(1);
    if (isColumnConst(*ctn.column))
        result_ctn.column = ColumnConst::create(result_ctn.column, input_rows_count);
    block.getByPosition(result) = result_ctn;
}

template <bool or_null>
ExecutableFunctionImplPtr FunctionJoinGet<or_null>::prepare(const Block &, const ColumnNumbers &, size_t) const
{
    return std::make_unique<ExecutableFunctionJoinGet<or_null>>(join, attr_name);
}

void registerFunctionJoinGet(FunctionFactory & factory)
{
    // joinGet
    factory.registerFunction<JoinGetOverloadResolver<false>>();
    // joinGetOrNull
    factory.registerFunction<JoinGetOverloadResolver<true>>();
}

template class ExecutableFunctionJoinGet<true>;
template class ExecutableFunctionJoinGet<false>;
template class FunctionJoinGet<true>;
template class FunctionJoinGet<false>;
template class JoinGetOverloadResolver<true>;
template class JoinGetOverloadResolver<false>;
}
