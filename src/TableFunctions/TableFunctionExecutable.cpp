
#include <Analyzer/TableFunctionNode.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/parseQuery.h>
#include <Storages/ExecutableSettings.h>
#include <Storages/StorageExecutable.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/registerTableFunctions.h>
#include <boost/program_options/parsers.hpp>
#include <boost/token_functions.hpp>
#include <Common/Exception.h>
#include <Common/VectorWithMemoryTracking.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

/* executable(script_name_optional_arguments, format, structure, input_query) - creates a temporary storage from executable file
 *
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionExecutable : public ITableFunction
{
public:
    static constexpr auto name = "executable";

    std::string getName() const override { return name; }

    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;

    const char * getStorageEngineName() const override { return "Executable"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;

    VectorWithMemoryTracking<size_t> skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr context) const override;

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String script_name;
    VectorWithMemoryTracking<String> arguments;
    String format;
    String structure;
    VectorWithMemoryTracking<ASTPtr> input_queries;
    ASTPtr settings_query = nullptr;
};


VectorWithMemoryTracking<size_t> TableFunctionExecutable::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    const auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    const auto & table_function_node_arguments = table_function_node.getArguments().getNodes();
    size_t table_function_node_arguments_size = table_function_node_arguments.size();

    if (table_function_node_arguments_size <= 3)
        return {};

    VectorWithMemoryTracking<size_t> result_indexes;
    result_indexes.reserve(table_function_node_arguments_size - 3);
    for (size_t i = 3; i < table_function_node_arguments_size; ++i)
        result_indexes.push_back(i);

    return result_indexes;
}

void TableFunctionExecutable::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto * function = ast_function->as<ASTFunction>();

    if (!function->arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Table function '{}' must have arguments",
            getName());

    auto args = function->arguments->children;

    if (args.size() < 3)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Table function '{}' requires minimum 3 arguments: script_name, format, structure, [input_query...]",
            getName());

    auto check_argument = [&](size_t i, const std::string & argument_name)
    {
        if (!args[i]->as<ASTIdentifier>() &&
            !args[i]->as<ASTLiteral>() &&
            !args[i]->as<ASTQueryParameter>() &&
            !args[i]->as<ASTSubquery>() &&
            !args[i]->as<ASTFunction>())
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of argument '{}' for table function '{}': must be an identifier or string literal, but got: {}",
                argument_name, getName(), args[i]->formatForErrorMessage());
    };

    check_argument(0, "script_name");
    check_argument(1, "format");
    check_argument(2, "structure");

    for (size_t i = 0; i <= 2; ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    auto script_name_with_arguments_value = checkAndGetLiteralArgument<String>(args[0], "script_name_with_arguments_value");

    auto script_name_with_arguments = [&]()
    {
        try
        {
            return boost::program_options::split_unix(script_name_with_arguments_value);
        }
        catch (const boost::escaped_list_error & e)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Failed to parse script name and arguments: {}", e.what());
        }
    }();

    if (script_name_with_arguments.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Script name cannot be empty");

    script_name = std::move(script_name_with_arguments[0]);
    script_name_with_arguments.erase(script_name_with_arguments.begin());
    arguments.assign(script_name_with_arguments.begin(), script_name_with_arguments.end());
    format = checkAndGetLiteralArgument<String>(args[1], "format");
    structure = checkAndGetLiteralArgument<String>(args[2], "structure");

    for (size_t i = 3; i < args.size(); ++i)
    {
        if (args[i]->as<ASTSetQuery>())
        {
            settings_query = std::move(args[i]);
        }
        else
        {
            ASTPtr query;
            if (!args[i]->children.empty())
                query = args[i]->children.at(0);

            if (query && query->as<ASTSelectWithUnionQuery>())
            {
                input_queries.emplace_back(std::move(query));
            }
            else
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Table function '{}' argument is invalid {}",
                    getName(),
                    args[i]->formatForErrorMessage());
            }
        }
    }
}

ColumnsDescription TableFunctionExecutable::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionExecutable::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto storage_id = StorageID(getDatabaseName(), table_name);
    auto global_context = context->getGlobalContext();
    ExecutableSettings settings;
    settings.script_name = script_name;
    settings.script_arguments = arguments;
    if (settings_query != nullptr)
        settings.applyChanges(settings_query->as<ASTSetQuery>()->changes);

    auto storage = std::make_shared<StorageExecutable>(
        storage_id,
        format,
        settings,
        input_queries,
        getActualTableStructure(context, is_insert_query),
        ConstraintsDescription{},
        /* comment = */ "");
    storage->startup();
    return storage;
}

}

void registerTableFunctionExecutable(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionExecutable>({.description = R"DOCS_MD(
The `executable` table function creates a table based on the output of a user-defined function (UDF) that you define in a script that outputs rows to **stdout**. The executable script is stored in the `users_scripts` directory and can read data from any source. Make sure your ClickHouse server has all the required packages to run the executable script. For example, if it is a Python script, ensure that the server has the necessary Python packages installed.

You can optionally include one or more input queries that stream their results to **stdin** for the script to read.

<Note>
A key advantage between ordinary UDF functions and the `executable` table function and `Executable` table engine is that ordinary UDF functions cannot change the row count. For example, if the input is 100 rows, then the result must return 100 rows. When using the `executable` table function or `Executable` table engine, your script can make any data transformations you want, including complex aggregations.
</Note>

## Syntax {#syntax}

The `executable` table function requires three parameters and accepts an optional list of input queries:

```sql
executable(script_name, format, structure, [input_query...] [,SETTINGS ...])
```

- `script_name`: the file name of the script. saved in the `user_scripts` folder (the default folder of the `user_scripts_path` setting)
- `format`: the format of the generated table
- `structure`: the table schema of the generated table
- `input_query`: an optional query (or collection or queries) whose results are passed to the script via **stdin**

<Note>
If you are going to invoke the same script repeatedly with the same input queries, consider using the [`Executable` table engine](/reference/engines/table-engines/special/executable).
</Note>

The following Python script is named `generate_random.py` and is saved in the `user_scripts` folder. It reads in a number `i` and prints `i` random strings, with each string preceded by a number that is separated by a tab:

```python
#!/usr/local/bin/python3.9

import sys
import string
import random

def main():

    # Read input value
    for number in sys.stdin:
        i = int(number)

        # Generate some random rows
        for id in range(0, i):
            letters = string.ascii_letters
            random_string =  ''.join(random.choices(letters ,k=10))
            print(str(id) + '\t' + random_string + '\n', end='')

        # Flush results to stdout
        sys.stdout.flush()

if __name__ == "__main__":
    main()
```

Let's invoke the script and have it generate 10 random strings:

```sql
SELECT * FROM executable('generate_random.py', TabSeparated, 'id UInt32, random String', (SELECT 10))
```

The response looks like:

```response
┌─id─┬─random─────┐
│  0 │ xheXXCiSkH │
│  1 │ AqxvHAoTrl │
│  2 │ JYvPCEbIkY │
│  3 │ sWgnqJwGRm │
│  4 │ fTZGrjcLon │
│  5 │ ZQINGktPnd │
│  6 │ YFSvGGoezb │
│  7 │ QyMJJZOOia │
│  8 │ NfiyDDhmcI │
│  9 │ REJRdJpWrg │
└────┴────────────┘
```

## Settings {#settings}

- `send_chunk_header` - controls whether to send row count before sending a chunk of data to process. Default value is `false`.
- `pool_size` — Size of pool. If 0 is specified as `pool_size` then there is no pool size restrictions. Default value is `16`.
- `max_command_execution_time` — Maximum executable script command execution time for processing block of data. Specified in seconds. Default value is 10.
- `command_termination_timeout` — executable script should contain main read-write loop. After table function is destroyed, pipe is closed, and executable file will have `command_termination_timeout` seconds to shutdown, before ClickHouse will send SIGTERM signal to child process. Specified in seconds. Default value is 10.
- `command_read_timeout` - timeout for reading data from command stdout in milliseconds. Default value 10000.
- `command_write_timeout` - timeout for writing data to command stdin in milliseconds. Default value 10000.

## Passing Query Results to a Script {#passing-query-results-to-a-script}

Be sure to check out the example in the `Executable` table engine on [how to pass query results to a script](/reference/engines/table-engines/special/executable#passing-query-results-to-a-script). Here is how you execute the same script in that example using the `executable` table function:

```sql
SELECT * FROM executable(
    'sentiment.py',
    TabSeparated,
    'id UInt64, sentiment Float32',
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
