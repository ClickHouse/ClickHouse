#include <Columns/ColumnString.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/HashJoin/HashJoin.h>
#include <Storages/StorageJoin.h>
#include <Storages/TableLockHolder.h>
#include <Access/Common/AccessType.h>
#include <Access/Common/AccessFlags.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

class HashJoin;
using StorageJoinPtr = std::shared_ptr<StorageJoin>;

namespace
{

class ExecutableFunctionJoinGet final : public IExecutableFunction
{
public:
    ExecutableFunctionJoinGet(const char * name_,
                              ContextPtr context_,
                              TableLockHolder table_lock_,
                              StorageJoinPtr storage_join_,
                              const DB::Block & result_columns_)
        : function_name(name_)
        , context(context_)
        , table_lock(std::move(table_lock_))
        , storage_join(std::move(storage_join_))
        , result_columns(result_columns_)
    {}

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }
    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;

    String getName() const override { return function_name; }

private:
    const char * function_name;
    ContextPtr context;
    TableLockHolder table_lock;
    StorageJoinPtr storage_join;
    DB::Block result_columns;
};

class FunctionJoinGet final : public IFunctionBase
{
public:
    FunctionJoinGet(const char * name_,
                    ContextPtr context_,
                    TableLockHolder table_lock_,
                    StorageJoinPtr storage_join_, String attr_name_,
                    DataTypes argument_types_, DataTypePtr return_type_)
        : function_name(name_)
        , context(context_)
        , table_lock(std::move(table_lock_))
        , storage_join(storage_join_)
        , attr_name(std::move(attr_name_))
        , argument_types(std::move(argument_types_))
        , return_type(std::move(return_type_))
    {
    }

    String getName() const override { return function_name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getResultType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override;

private:
    const char * function_name;
    ContextPtr context;
    TableLockHolder table_lock;
    StorageJoinPtr storage_join;
    const String attr_name;
    DataTypes argument_types;
    DataTypePtr return_type;
};

class JoinGetOverloadResolver final : public IFunctionOverloadResolver, WithContext
{
public:
    JoinGetOverloadResolver(const char * name_, bool or_null_, ContextPtr context_)
        : WithContext(context_), function_name(name_), or_null(or_null_) {}

    static FunctionOverloadResolverPtr create(const char * name, bool or_null, ContextPtr context_)
    {
        return std::make_unique<JoinGetOverloadResolver>(name, or_null, std::move(context_));
    }

    bool isDeterministic() const override { return false; }
    String getName() const override { return function_name; }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const override;
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return {}; } // Not used

    bool useDefaultImplementationForNulls() const override { return false; }
    bool useDefaultImplementationForLowCardinalityColumns() const override { return false; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

private:
    const char * function_name;
    bool or_null;
};


ColumnPtr ExecutableFunctionJoinGet::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const
{
    ColumnsWithTypeAndName keys;
    for (size_t i = 2; i < arguments.size(); ++i)
    {
        auto key = arguments[i];
        keys.emplace_back(std::move(key));
    }
    return storage_join->joinGet(keys, result_columns, context).column;
}

ExecutableFunctionPtr FunctionJoinGet::prepare(const ColumnsWithTypeAndName &) const
{
    Block result_columns {{return_type->createColumn(), return_type, attr_name}};

    Names column_names = storage_join->getKeyNames();
    column_names.push_back(attr_name);
    context->checkAccess(AccessType::SELECT, storage_join->getStorageID(), column_names);

    return std::make_unique<ExecutableFunctionJoinGet>(function_name, context, table_lock, storage_join, result_columns);
}

std::pair<std::shared_ptr<StorageJoin>, String>
getJoin(const ColumnsWithTypeAndName & arguments, ContextPtr context)
{
    String join_name;
    if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
    {
        join_name = name_col->getValue<String>();
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of first argument of function joinGet, expected a const string.",
                        arguments[0].type->getName());

    const auto qualified_name = QualifiedTableName::parseFromString(join_name);
    const auto storage_id = context->resolveStorageID({qualified_name.database, qualified_name.table});

    auto table = DatabaseCatalog::instance().getTable(storage_id, std::const_pointer_cast<Context>(context));
    auto storage_join = std::dynamic_pointer_cast<StorageJoin>(table);
    if (!storage_join)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Table {} should have engine StorageJoin", join_name);

    String attr_name;
    if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
    {
        attr_name = name_col->getValue<String>();
    }
    else
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Illegal type {} of second argument of function joinGet, expected a const string.",
                        arguments[1].type->getName());
    return std::make_pair(storage_join, attr_name);
}

FunctionBasePtr JoinGetOverloadResolver::buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &) const
{
    if (arguments.size() < 3)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Number of arguments for function '{}' doesn't match: passed {}, should be greater or equal to 3",
            getName() , arguments.size());
    auto [storage_join, attr_name] = getJoin(arguments, getContext());
    DataTypes data_types(arguments.size() - 2);
    DataTypes argument_types(arguments.size());
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        if (i >= 2)
            data_types[i - 2] = arguments[i].type;
        argument_types[i] = arguments[i].type;
    }

    bool effective_or_null = or_null || storage_join->useNulls();
    auto return_type = storage_join->joinGetCheckAndGetReturnType(data_types, attr_name, effective_or_null);
    auto table_lock = storage_join->lockForShare(getContext()->getInitialQueryId(), getContext()->getSettingsRef()[Setting::lock_acquire_timeout]);

    const char * effective_name = effective_or_null ? "joinGetOrNull" : "joinGet";
    return std::make_unique<FunctionJoinGet>(effective_name, getContext(), table_lock, storage_join, attr_name, argument_types, return_type);
}

}

REGISTER_FUNCTION(JoinGet)
{
    FunctionDocumentation::Description description_joinGet = R"(
Allows you to extract data from a table the same way as from a dictionary.
Gets data from Join tables using the specified join key.

:::note
Only supports tables created with the `ENGINE = Join(ANY, LEFT, <join_keys>)` [statement](/engines/table-engines/special/join).
:::
)";
    FunctionDocumentation::Syntax syntax_joinGet = "joinGet(join_storage_table_name, value_column, join_keys)";
    FunctionDocumentation::Arguments arguments_joinGet = {
        {"join_storage_table_name", "An identifier which indicates where to perform the search. The identifier is searched in the default database (see parameter `default_database` in the config file). To override the default database, use the `USE database_name` query or specify the database and the table through a dot, like `database_name.table_name`.", {"String"}},
        {"value_column", "The name of the column of the table that contains required data.", {"const String"}},
        {"join_keys", "A list of join keys.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_joinGet = {"Returns list of values corresponded to list of keys.", {"Any"}};
    FunctionDocumentation::Examples examples_joinGet = {
    {
        "Usage example",
        R"(
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id);
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13);

SELECT joinGet(db_test.id_val, 'val', toUInt32(1));
        )",
        R"(
┌─joinGet(db_test.id_val, 'val', toUInt32(1))─┐
│                                          11 │
└─────────────────────────────────────────────┘
        )"
    },
    {
        "Usage with table from current database",
        R"(
USE db_test;
SELECT joinGet(id_val, 'val', toUInt32(2));
        )",
        R"(
┌─joinGet(id_val, 'val', toUInt32(2))─┐
│                                  12 │
└─────────────────────────────────────┘
        )"
    },
    {
        "Using arrays as join keys",
        R"(
CREATE TABLE some_table (id1 UInt32, id2 UInt32, name String) ENGINE = Join(ANY, LEFT, id1, id2);
INSERT INTO some_table VALUES (1, 11, 'a') (2, 12, 'b') (3, 13, 'c');

SELECT joinGet(some_table, 'name', 1, 11);
        )",
        R"(
┌─joinGet(some_table, 'name', 1, 11)─┐
│ a                                  │
└────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_joinGet = {18, 16};
    FunctionDocumentation::Category category_joinGet = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_joinGet = {description_joinGet, syntax_joinGet, arguments_joinGet, {}, returned_value_joinGet, examples_joinGet, introduced_in_joinGet, category_joinGet};

    FunctionDocumentation::Description description_joinGetOrNull = R"(
Allows you to extract data from a table the same way as from a dictionary.
Gets data from Join tables using the specified join key.
Unlike [`joinGet`](#joinGet) it returns `NULL` when the key is missing.

:::note
Only supports tables created with the `ENGINE = Join(ANY, LEFT, <join_keys>)` [statement](/engines/table-engines/special/join).
:::
)";
    FunctionDocumentation::Syntax syntax_joinGetOrNull = "joinGetOrNull(join_storage_table_name, value_column, join_keys)";
    FunctionDocumentation::Arguments arguments_joinGetOrNull = {
        {"join_storage_table_name", "An identifier which indicates where to perform the search. The identifier is searched in the default database (see parameter default_database in the config file). To override the default database, use the `USE database_name` query or specify the database and the table through a dot, like `database_name.table_name`.", {"String"}},
        {"value_column", "The name of the column of the table that contains required data.", {"const String"}},
        {"join_keys", "A list of join keys.", {"Any"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_joinGetOrNull = {"Returns a list of values corresponding to the list of keys, or `NULL` if a key is not found.", {"Any"}};
    FunctionDocumentation::Examples examples_joinGetOrNull = {
    {
        "Usage example",
        R"(
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id);
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13);

SELECT joinGetOrNull(db_test.id_val, 'val', toUInt32(1)), joinGetOrNull(db_test.id_val, 'val', toUInt32(999));
        )",
        R"(
┌─joinGetOrNull(db_test.id_val, 'val', toUInt32(1))─┬─joinGetOrNull(db_test.id_val, 'val', toUInt32(999))─┐
│                                                11 │                                                ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────┴─────────────────────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_joinGetOrNull = {20, 4};
    FunctionDocumentation::Category category_joinGetOrNull = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation_joinGetOrNull = {description_joinGetOrNull, syntax_joinGetOrNull, arguments_joinGetOrNull, {}, returned_value_joinGetOrNull, examples_joinGetOrNull, introduced_in_joinGetOrNull, category_joinGetOrNull};

    // joinGet
    factory.registerFunction("joinGet",
        [](ContextPtr ctx){ return JoinGetOverloadResolver::create("joinGet", false, std::move(ctx)); },
        documentation_joinGet);
    // joinGetOrNull
    factory.registerFunction("joinGetOrNull",
        [](ContextPtr ctx){ return JoinGetOverloadResolver::create("joinGetOrNull", true, std::move(ctx)); },
        documentation_joinGetOrNull);
}

}
