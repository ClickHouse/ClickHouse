#include "config.h"

#if USE_YTSAURUS
#include <Storages/YTsaurus/StorageYTsaurus.h>

#include <Common/Exception.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSetQuery.h>

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

    const char * getStorageEngineName() const override { return "YTsaurus"; }

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

    YTsaurusSettings yt_settings;

    for (auto it = args.begin(); it != args.end(); ++it)
    {
        const ASTSetQuery * settings_ast = (*it)->as<ASTSetQuery>();
        if (settings_ast)
        {
            yt_settings.loadFromQuery(*settings_ast);
            args.erase(it);
            break;
        }
    }
    if (args.size() == 2)
    {
        // With Named Collection
        ASTs main_arguments(args.begin(), args.begin() + 1);
        configuration = std::make_shared<YTsaurusStorageConfiguration>(StorageYTsaurus::getConfiguration(main_arguments, yt_settings, context));
        structure = checkAndGetLiteralArgument<String>(args[1], "structure");
    }
    else if (args.size() == 4)
    {
        ASTs main_arguments(args.begin(), args.begin() + 3);
        configuration = std::make_shared<YTsaurusStorageConfiguration>(StorageYTsaurus::getConfiguration(main_arguments, yt_settings, context));
        structure = checkAndGetLiteralArgument<String>(args[3], "structure");
    }
    else
    {
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function 'ytsaurus' 4 parameters: "
            "ytsaurus('http_proxy_url', cypress_path, oauth_token, structure) "
            "or with 2 parameters: ytsaurus(named_collections, structure)."
        );
    }
}

}

void registerTableFunctionYTsaurus(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionYTsaurus>(
    {.description = R"DOCS_MD(
import ExperimentalBadge from "/snippets/components/ExperimentalBadge/ExperimentalBadge.jsx";

<ExperimentalBadge/>

The table function allows to read data from the YTsaurus cluster.

## Syntax {#syntax}

```sql
ytsaurus(http_proxy_url, cypress_path, oauth_token, format)
```

<Info>
This is an experimental feature that may change in backwards-incompatible ways in the future releases.
Enable usage of the YTsaurus table function
with [allow_experimental_ytsaurus_table_function](/reference/settings/session-settings#allow_experimental_ytsaurus_table_engine) setting.
Input the command `set allow_experimental_ytsaurus_table_function = 1`.
</Info>

## Arguments {#arguments}

- `http_proxy_url` — URL to the YTsaurus http proxy.
- `cypress_path` — Cypress path to the data source.
- `oauth_token` — OAuth token.
- `format` — The [format](/reference/formats/index) of the data source.

**Returned value**

A table with the specified structure for reading data in the specified ytsaurus cypress path in YTsaurus cluster.

**See Also**

- [ytsaurus engine](/reference/engines/table-engines/integrations/ytsaurus)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
#endif
