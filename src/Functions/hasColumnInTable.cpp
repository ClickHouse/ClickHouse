#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_TABLE;
}

namespace
{

/** Usage:
 *  hasColumnInTable('database', 'table', 'column')
 */
class FunctionHasColumnInTable final : public IFunction, WithContext
{
public:
    static constexpr auto name = "hasColumnInTable";
    static FunctionPtr create(ContextPtr context_)
    {
        return std::make_shared<FunctionHasColumnInTable>(context_->getGlobalContext());
    }

    explicit FunctionHasColumnInTable(ContextPtr global_context_) : WithContext(global_context_)
    {
    }

    bool isVariadic() const override
    {
        return false;
    }
    size_t getNumberOfArguments() const override
    {
        return 3;
    }

    String getName() const override
    {
        return name;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override;

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override;
};


DataTypePtr FunctionHasColumnInTable::getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const
{
    static const std::string arg_pos_description[] = {"First", "Second", "Third"};
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const ColumnWithTypeAndName & argument = arguments[i];

        if (!checkColumnConst<ColumnString>(argument.column.get()))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "{} argument for function {} must be const String.",
                            arg_pos_description[i], getName());
        }
    }

    return std::make_shared<DataTypeUInt8>();
}


ColumnPtr FunctionHasColumnInTable::executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const
{
    auto get_string_from_column = [&](const ColumnWithTypeAndName & column) -> String
    {
        const ColumnConst & const_column = checkAndGetColumnConst<ColumnString>(*column.column);
        return const_column.getValue<String>();
    };

    String database_name = get_string_from_column(arguments[0]);
    String table_name = get_string_from_column(arguments[1]);
    String column_name = get_string_from_column(arguments[2]);

    if (table_name.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table name is empty");

    // FIXME this (probably) needs a non-constant access to query context,
    // because it might initialized a storage. Ideally, the tables required
    // by the query should be initialized at an earlier stage.
    const StoragePtr & table = DatabaseCatalog::instance().getTable(
        {database_name, table_name},
        const_pointer_cast<Context>(getContext()));
    auto table_metadata = table->getInMemoryMetadataPtr(getContext(), false);
    bool has_column = table_metadata->getColumns().hasPhysical(column_name);
    bool has_alias_column = table_metadata->getColumns().hasAlias(column_name);

    return DataTypeUInt8().createColumnConst(input_rows_count, Field{static_cast<UInt64>(has_column || has_alias_column)});
}

}

REGISTER_FUNCTION(HasColumnInTable)
{
    FunctionDocumentation::Description description = R"(
Checks if a specific column exists in a database table.
For elements in a nested data structure, the function checks for the existence of a column.
For the nested data structure itself, the function returns `0`.
    )";
    FunctionDocumentation::Syntax syntax = "hasColumnInTable(database, table, column)";
    FunctionDocumentation::Arguments arguments = {
        {"database", "Name of the database.", {"const String"}},
        {"table", "Name of the table.", {"const String"}},
        {"column", "Name of the column.", {"const String"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns `1` if the given column exists, `0` otherwise.", {"UInt8"}};
    FunctionDocumentation::Examples examples = {
    {
        "Check an existing column",
        R"(
SELECT hasColumnInTable('system','metrics','metric')
        )",
        R"(
1
        )"
    },
    {
        "Check a non-existing column",
        R"(
SELECT hasColumnInTable('system','metrics','non-existing_column')
        )",
        R"(
0
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {1, 1};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasColumnInTable>(documentation);
}

}
