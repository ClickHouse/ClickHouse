#include "config.h"

#if USE_YTSAURUS
#include <Storages/YTsaurus/StorageYTsaurus.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>

#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_FUNCTION;
}

namespace Setting
{
    extern const SettingsBool allow_experimental_ytsaurus_table_function;
}

namespace
{

class TableFunctionYTsaurus : public ITableFunction
{
public:
    static constexpr auto name = "ytsaurus";

    std::string getName() const override { return name; }

private:
    StoragePtr executeImpl(
        const ASTPtr & ast_function,
        ContextPtr context,
        const std::string & table_name,
        ColumnsDescription cached_columns,
        bool is_insert_query) const override;

    const char * getStorageTypeName() const override { return "YTsaurus"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    std::shared_ptr<YTsaurusStorageConfiguration> configuration;
    String structure;
};

StoragePtr TableFunctionYTsaurus::executeImpl(
    const ASTPtr & /*ast_function*/,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /*cached_columns*/,
    bool is_insert_query) const
{
    if (!context->getSettingsRef()[Setting::allow_experimental_ytsaurus_table_function])
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "Table function ytsaurus is experimental."
                "Set `allow_experimental_ytsaurus_table_function` setting to enable it");

    auto columns = getActualTableStructure(context, is_insert_query);
    auto storage = std::make_shared<StorageYTsaurus>(
        StorageID(getDatabaseName(), table_name), std::move(*configuration), columns, ConstraintsDescription(), String{});
    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionYTsaurus::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(structure, context);
}

void TableFunctionYTsaurus::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & func_args = ast_function->as<ASTFunction &>();
    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function 'ytsaurus' must have arguments.");

    ASTs & args = func_args.arguments->children;


    if (args.size() == 4)
    {
        ASTs main_arguments(args.begin(), args.begin() + 3);
        configuration = std::make_shared<YTsaurusStorageConfiguration>(StorageYTsaurus::getConfiguration(main_arguments, nullptr, context));
        structure = checkAndGetLiteralArgument<String>(args[3], "structure");
    }
    else
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function 'ytsaurus' 4 parameters: "
            "ytsaurus('http_proxy_url', cypress_path, oauth_token, structure).");
    }
}

}

void registerTableFunctionYTsaurus(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionYTsaurus>(
    {
            .documentation =
            {
                    .description = "Allows get data from YTsaurus.",
                    .examples = {
                        {"Fetch collection by URI", "SELECT * FROM ytsaurus('localhost:80', '//tmp/test', 'auth_token', 'key UInt64, data String')", ""},
                    },
                    .category = FunctionDocumentation::Category::TableFunction
            },
    });
}

}
#endif
