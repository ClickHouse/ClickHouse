#include <Core/Settings.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/getStructureOfRemoteTable.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNKNOWN_TABLE;
}

namespace
{

/** Usage:
 *  hasColumnInTable(['hostname'[, 'username'[, 'password']],] 'database', 'table', 'column')
 */
class FunctionHasColumnInTable : public IFunction, WithContext
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
        return true;
    }
    size_t getNumberOfArguments() const override
    {
        return 0;
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
    if (arguments.size() < 3 || arguments.size() > 6)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Invalid number of arguments for function {}", getName());

    static const std::string arg_pos_description[] = {"First", "Second", "Third", "Fourth", "Fifth", "Sixth"};
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
    auto get_string_from_columns = [&](const ColumnWithTypeAndName & column) -> String
    {
        const ColumnConst & const_column = checkAndGetColumnConst<ColumnString>(*column.column);
        return const_column.getValue<String>();
    };

    size_t arg = 0;
    String host_name;
    String user_name;
    String password;

    if (arguments.size() > 3)
        host_name = get_string_from_columns(arguments[arg++]);

    if (arguments.size() > 4)
        user_name = get_string_from_columns(arguments[arg++]);

    if (arguments.size() > 5)
        password = get_string_from_columns(arguments[arg++]);

    String database_name = get_string_from_columns(arguments[arg++]);
    String table_name = get_string_from_columns(arguments[arg++]);
    String column_name = get_string_from_columns(arguments[arg++]);

    if (table_name.empty())
        throw Exception(ErrorCodes::UNKNOWN_TABLE, "Table name is empty");

    bool has_column;
    bool has_alias_column;
    if (host_name.empty())
    {
        // FIXME this (probably) needs a non-constant access to query context,
        // because it might initialized a storage. Ideally, the tables required
        // by the query should be initialized at an earlier stage.
        const StoragePtr & table = DatabaseCatalog::instance().getTable(
            {database_name, table_name},
            const_pointer_cast<Context>(getContext()));
        auto table_metadata = table->getInMemoryMetadataPtr();
        has_column = table_metadata->getColumns().hasPhysical(column_name);
        has_alias_column = table_metadata->getColumns().hasAlias(column_name);
    }
    else
    {
        std::vector<std::vector<String>> host_names = {{ host_name }};

        bool treat_local_as_remote = false;
        bool treat_local_port_as_remote = getContext()->getApplicationType() == Context::ApplicationType::LOCAL;
        ClusterConnectionParameters params{
            !user_name.empty() ? user_name : "default",
            password,
            getContext()->getTCPPort(),
            treat_local_as_remote,
            treat_local_port_as_remote,
            /* secure= */ false,
            /* bind_host= */ "",
            /* priority= */ Priority{1},
            /* cluster_name= */ "",
            /* password= */ ""
        };
        auto cluster = std::make_shared<Cluster>(getContext()->getSettingsRef(), host_names, params);

        // FIXME this (probably) needs a non-constant access to query context,
        // because it might initialized a storage. Ideally, the tables required
        // by the query should be initialized at an earlier stage.
        auto remote_columns = getStructureOfRemoteTable(*cluster,
            {database_name, table_name},
            const_pointer_cast<Context>(getContext()));

        has_column = remote_columns.hasPhysical(column_name);
        has_alias_column = remote_columns.hasAlias(column_name);
    }

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
    FunctionDocumentation::Syntax syntax = "hasColumnInTable([hostname[, username[, password]],]database, table, column)";
    FunctionDocumentation::Arguments arguments = {
        {"database", "Name of the database.", {"const String"}},
        {"table", "Name of the table.", {"const String"}},
        {"column", "Name of the column.", {"const String"}},
        {"hostname", "Optional. Remote server name to perform the check on.", {"const String"}},
        {"username", "Optional. Username for remote server.", {"const String"}},
        {"password", "Optional. Password for remote server.", {"const String"}}
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
    FunctionDocumentation documentation = {description, syntax, arguments, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionHasColumnInTable>(documentation);
}

}
