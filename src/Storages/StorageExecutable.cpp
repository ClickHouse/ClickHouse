#include <Storages/StorageExecutable.h>

#include <filesystem>
#include <unistd.h>

#include <boost/algorithm/string/split.hpp>

#include <Columns/IColumn.h>

#include <Common/VectorWithMemoryTracking.h>
#include <Common/filesystemHelpers.h>

#include <Core/Block.h>
#include <Core/Settings.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTCreateQuery.h>

#include <QueryPipeline/Pipe.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <Storages/ExecutableSettings.h>
#include <Storages/StorageFactory.h>
#include <Storages/checkAndGetLiteralArgument.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsSeconds max_execution_time;
}

namespace ExecutableSetting
{
    extern const ExecutableSettingsBool send_chunk_header;
    extern const ExecutableSettingsUInt64 pool_size;
    extern const ExecutableSettingsUInt64 max_command_execution_time;
    extern const ExecutableSettingsUInt64 command_termination_timeout;
    extern const ExecutableSettingsUInt64 command_read_timeout;
    extern const ExecutableSettingsUInt64 command_write_timeout;
    extern const ExecutableSettingsExternalCommandStderrReaction stderr_reaction;
    extern const ExecutableSettingsBool check_exit_code;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNSUPPORTED_METHOD;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{
    void transformToSingleBlockSources(Pipes & inputs)
    {
        size_t inputs_size = inputs.size();
        for (size_t i = 0; i < inputs_size; ++i)
        {
            auto && input = inputs[i];
            QueryPipeline input_pipeline(std::move(input));
            PullingPipelineExecutor input_pipeline_executor(input_pipeline);

            auto header = input_pipeline_executor.getHeader();
            auto result_block = header.cloneEmpty();

            size_t result_block_columns = result_block.columns();

            Block result;
            while (input_pipeline_executor.pull(result))
            {
                for (size_t result_block_index = 0; result_block_index < result_block_columns; ++result_block_index)
                {
                    auto & block_column = result.safeGetByPosition(result_block_index);
                    auto & result_block_column = result_block.safeGetByPosition(result_block_index);

                    result_block_column.column->assumeMutable()->insertRangeFrom(*block_column.column, 0, block_column.column->size());
                }
            }

            auto source = std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(result_block)));
            inputs[i] = Pipe(std::move(source));
        }
    }
}

StorageExecutable::StorageExecutable(
    const StorageID & table_id_,
    const String & format,
    const ExecutableSettings & settings_,
    const VectorWithMemoryTracking<ASTPtr> & input_queries_,
    const ColumnsDescription & columns,
    const ConstraintsDescription & constraints,
    const String & comment)
    : StorageWithCommonVirtualColumns(table_id_)
    , settings(std::make_unique<ExecutableSettings>(settings_))
    , input_queries(input_queries_)
    , log(settings->is_executable_pool ? getLogger("StorageExecutablePool") : getLogger("StorageExecutable"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns);
    storage_metadata.setConstraints(constraints);
    storage_metadata.setComment(comment);
    storage_metadata.setVirtuals(createVirtuals());
    setInMemoryMetadata(storage_metadata);

    ShellCommandSourceCoordinator::Configuration configuration
    {
        .format = format,
        .command_termination_timeout_seconds = (*settings)[ExecutableSetting::command_termination_timeout],
        .command_read_timeout_milliseconds = (*settings)[ExecutableSetting::command_read_timeout],
        .command_write_timeout_milliseconds = (*settings)[ExecutableSetting::command_write_timeout],
        .stderr_reaction = (*settings)[ExecutableSetting::stderr_reaction],
        .check_exit_code = (*settings)[ExecutableSetting::check_exit_code],

        .pool_size = (*settings)[ExecutableSetting::pool_size],
        .max_command_execution_time_seconds = (*settings)[ExecutableSetting::max_command_execution_time],

        .is_executable_pool = settings->is_executable_pool,
        .send_chunk_header = (*settings)[ExecutableSetting::send_chunk_header],
        .execute_direct = true
    };

    coordinator = std::make_unique<ShellCommandSourceCoordinator>(std::move(configuration));
}

VirtualColumnsDescription StorageExecutable::createVirtuals()
{
    VirtualColumnsDescription desc;
    desc.addEphemeral("_table", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    desc.addEphemeral("_database", std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()), "", VirtualsMaterializationPlace::Plan);
    return desc;
}

StorageExecutable::~StorageExecutable() = default;

String StorageExecutable::getName() const
{
    if (settings->is_executable_pool)
        return "ExecutablePool";
    return "Executable";
}

void StorageExecutable::readImpl(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t /*threads*/)
{
    auto & script_name = settings->script_name;

    auto user_scripts_path = context->getUserScriptsPath();
    auto script_path = user_scripts_path + '/' + script_name;

    if (!fileOrSymlinkPathStartsWith(script_path, user_scripts_path))
        throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} must be inside user scripts folder {}",
            script_name,
            user_scripts_path);

    if (!FS::exists(script_path))
         throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} does not exist inside user scripts folder {}",
            script_name,
            user_scripts_path);

    if (!FS::canExecute(script_path))
         throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
            "Executable file {} is not executable inside user scripts folder {}",
            script_name,
            user_scripts_path);

    Pipes inputs;
    QueryPlanResourceHolder resources;
    inputs.reserve(input_queries.size());

    for (auto & input_query : input_queries)
    {
        QueryPipelineBuilder builder;
        if (context->getSettingsRef()[Setting::allow_experimental_analyzer])
            builder = InterpreterSelectQueryAnalyzer(input_query, context, {}).buildQueryPipeline();
        else
            builder = InterpreterSelectWithUnionQuery(input_query, context, {}).buildQueryPipeline();
        inputs.emplace_back(QueryPipelineBuilder::getPipe(std::move(builder), resources));
    }

    /// For executable pool we read data from input streams and convert it to single blocks streams.
    if (settings->is_executable_pool)
        transformToSingleBlockSources(inputs);

    auto sample_block = storage_snapshot->metadata->getSampleBlock();

    ShellCommandSourceConfiguration configuration;
    configuration.max_block_size = max_block_size;

    if (settings->is_executable_pool)
    {
        configuration.read_fixed_number_of_rows = true;
        configuration.read_number_of_rows_from_process_output = true;
    }

    auto pipe = coordinator->createPipe(script_path, settings->script_arguments, std::move(inputs), std::move(sample_block), context, configuration);
    IStorage::readFromPipe(query_plan, std::move(pipe), column_names, storage_snapshot, query_info, context, shared_from_this());
    query_plan.addResources(std::move(resources));
}

void registerStorageExecutable(StorageFactory & factory)
{
    auto register_storage = [](const StorageFactory::Arguments & args, bool is_executable_pool) -> StoragePtr
    {
        auto local_context = args.getLocalContext();

        if (args.engine_args.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "StorageExecutable requires minimum 2 arguments: script_name, format, [input_query...]");

        for (size_t i = 0; i < 2; ++i)
            args.engine_args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args.engine_args[i], local_context);

        auto script_name_with_arguments_value = checkAndGetLiteralArgument<String>(args.engine_args[0], "script_name_with_arguments_value");

        VectorWithMemoryTracking<String> script_name_with_arguments;
        boost::split(script_name_with_arguments, script_name_with_arguments_value, [](char c) { return c == ' '; });

        auto script_name = script_name_with_arguments[0];
        script_name_with_arguments.erase(script_name_with_arguments.begin());
        auto format = checkAndGetLiteralArgument<String>(args.engine_args[1], "format");

        VectorWithMemoryTracking<ASTPtr> input_queries;
        for (size_t i = 2; i < args.engine_args.size(); ++i)
        {
            if (args.engine_args[i]->children.empty())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "StorageExecutable argument \"{}\" is invalid query",
                    args.engine_args[i]->formatForErrorMessage());

            ASTPtr query = args.engine_args[i]->children.at(0);
            if (!query->as<ASTSelectWithUnionQuery>())
                throw Exception(
                    ErrorCodes::UNSUPPORTED_METHOD, "StorageExecutable argument \"{}\" is invalid input query",
                    query->formatForErrorMessage());

            input_queries.emplace_back(std::move(query));
        }

        const auto & columns = args.columns;
        const auto & constraints = args.constraints;

        ExecutableSettings settings;
        settings.script_name = script_name;
        settings.script_arguments = script_name_with_arguments;
        settings.is_executable_pool = is_executable_pool;

        if (is_executable_pool)
        {
            size_t max_command_execution_time = 10;

            size_t max_execution_time_seconds = static_cast<size_t>(args.getContext()->getSettingsRef()[Setting::max_execution_time].totalSeconds());
            if (max_execution_time_seconds != 0 && max_command_execution_time > max_execution_time_seconds)
                max_command_execution_time = max_execution_time_seconds;

            settings[ExecutableSetting::max_command_execution_time] = max_command_execution_time;
        }

        if (args.storage_def->settings)
            settings.loadFromQuery(*args.storage_def);

        auto global_context = args.getContext()->getGlobalContext();
        return std::make_shared<StorageExecutable>(args.table_id, format, settings, input_queries, columns, constraints, args.comment);
    };

    StorageFactory::StorageFeatures storage_features;
    storage_features.supports_settings = true;
    storage_features.has_builtin_setting_fn = ExecutableSettings::hasBuiltin;

    factory.registerStorage("Executable", [&](const StorageFactory::Arguments & args)
    {
        return register_storage(args, false /*is_executable_pool*/);
    }, storage_features,
    Documentation{
        .description = R"DOCS_MD(
The `Executable` and `ExecutablePool` table engines allow you to define a table whose rows are generated from a script that you define (by writing rows to **stdout**). The executable script is stored in the `users_scripts` directory and can read data from any source.

- `Executable` tables: the script is run on every query
- `ExecutablePool` tables: maintains a pool of persistent processes, and takes processes from the pool for reads

You can optionally include one or more input queries that stream their results to **stdin** for the script to read.

## Creating an `Executable` table {#creating-an-executable-table}

The `Executable` table engine requires two parameters: the name of the script and the format of the incoming data. You can optionally pass in one or more input queries:

```sql
Executable(script_name, format, [input_query...])
```

Here are the relevant settings for an `Executable` table:

- `send_chunk_header`
  - Description: Send the number of rows in each chunk before sending a chunk to process. This setting can help to write your script in a more efficient way to preallocate some resources
  - Default value: false
- `command_termination_timeout`
  - Description: Command termination timeout in seconds
  - Default value: 10
- `command_read_timeout`
  - Description: Timeout for reading data from command stdout in milliseconds
  - Default value: 10000
- `command_write_timeout`
  - Description: Timeout for writing data to command stdin in milliseconds
  - Default value: 10000

Let's look at an example. The following Python script is named `my_script.py` and is saved in the `user_scripts` folder. It reads in a number `i` and prints `i` random strings, with each string preceded by a number that is separated by a tab:

```python
#!/usr/bin/python3

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

The following `my_executable_table` is built from the output of `my_script.py`, which will generate 10 random strings every time you run a `SELECT` from `my_executable_table`:

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

Creating the table returns immediately and does not invoke the script. Querying `my_executable_table` causes the script to be invoked:

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

## Passing query results to a script {#passing-query-results-to-a-script}

Users of the Hacker News website leave comments. Python contains a natural language processing toolkit (`nltk`) with a `SentimentIntensityAnalyzer` for determining if comments are positive, negative, or neutral - including assigning a value between -1 (a very negative comment) and 1 (a very positive comment). Let's create an `Executable` table that computes the sentiment of Hacker News comments using `nltk`.

This example uses the `hackernews` table described [here](/engines/table-engines/mergetree-family/textindexes/#hacker-news-dataset). The `hackernews` table includes an `id` column of type `UInt64` and a `String` column named `comment`. Let's start by defining the `Executable` table:

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

Some comments about the `sentiment` table:

- The file `sentiment.py` is saved in the `user_scripts` folder (the default folder of the `user_scripts_path` setting)
- The `TabSeparated` format means our Python script needs to generate rows of raw data that contain tab-separated values
- The query selects two columns from `hackernews`. The Python script will need to parse out those column values from the incoming rows

Here is the definition of `sentiment.py`:

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

Some comments about our Python script:

- For this to work, you will need to run `nltk.downloader.download('vader_lexicon')`. This could have been placed in the script, but then it would have been downloaded every time a query was executed on the `sentiment` table - which is not efficient
- Each value of `row` is going to be a row in the result set of `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20`
- The incoming row is tab-separated, so we parse out the `id` and `comment` using the Python `split` function
- The result of `polarity_scores` is a JSON object with a handful of values. We decided to just grab the `compound` value of this JSON object
- Recall that the `sentiment` table in ClickHouse uses the `TabSeparated` format and contains two columns, so our `print` function separates those columns with a tab

Every time you write a query that selects rows from the `sentiment` table, the `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` query is executed and the result is passed to `sentiment.py`. Let's test it out:

```sql
SELECT *
FROM sentiment
```

The response looks like:

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```

## Creating an `ExecutablePool` table {#creating-an-executablepool-table}

The syntax for `ExecutablePool` is similar to `Executable`, but there are a couple of relevant settings unique to an `ExecutablePool` table:

- `pool_size`
  - Description: Processes pool size. If size is 0, then there are no size restrictions
  - Default value: 16
- `max_command_execution_time`
  - Description: Max command execution time in seconds
  - Default value: 10

We can easily convert the `sentiment` table above to use `ExecutablePool` instead of `Executable`:

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
    pool_size = 4;
```

ClickHouse will maintain 4 processes on-demand when your client queries the `sentiment_pooled` table.
)DOCS_MD",
        .syntax = "ENGINE = Executable(script_name, format[, input_query...])",
        .related = {"ExecutablePool"}});

    factory.registerStorage("ExecutablePool", [&](const StorageFactory::Arguments & args)
    {
        return register_storage(args, true /*is_executable_pool*/);
    }, storage_features,
    Documentation{
        .description = R"DOCS_MD(
The `Executable` and `ExecutablePool` table engines allow you to define a table whose rows are generated from a script that you define (by writing rows to **stdout**). The executable script is stored in the `users_scripts` directory and can read data from any source.

- `Executable` tables: the script is run on every query
- `ExecutablePool` tables: maintains a pool of persistent processes, and takes processes from the pool for reads

You can optionally include one or more input queries that stream their results to **stdin** for the script to read.

## Creating an `Executable` table {#creating-an-executable-table}

The `Executable` table engine requires two parameters: the name of the script and the format of the incoming data. You can optionally pass in one or more input queries:

```sql
Executable(script_name, format, [input_query...])
```

Here are the relevant settings for an `Executable` table:

- `send_chunk_header`
  - Description: Send the number of rows in each chunk before sending a chunk to process. This setting can help to write your script in a more efficient way to preallocate some resources
  - Default value: false
- `command_termination_timeout`
  - Description: Command termination timeout in seconds
  - Default value: 10
- `command_read_timeout`
  - Description: Timeout for reading data from command stdout in milliseconds
  - Default value: 10000
- `command_write_timeout`
  - Description: Timeout for writing data to command stdin in milliseconds
  - Default value: 10000

Let's look at an example. The following Python script is named `my_script.py` and is saved in the `user_scripts` folder. It reads in a number `i` and prints `i` random strings, with each string preceded by a number that is separated by a tab:

```python
#!/usr/bin/python3

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

The following `my_executable_table` is built from the output of `my_script.py`, which will generate 10 random strings every time you run a `SELECT` from `my_executable_table`:

```sql
CREATE TABLE my_executable_table (
   x UInt32,
   y String
)
ENGINE = Executable('my_script.py', TabSeparated, (SELECT 10))
```

Creating the table returns immediately and does not invoke the script. Querying `my_executable_table` causes the script to be invoked:

```sql
SELECT * FROM my_executable_table
```

```response
┌─x─┬─y──────────┐
│ 0 │ BsnKBsNGNH │
│ 1 │ mgHfBCUrWM │
│ 2 │ iDQAVhlygr │
│ 3 │ uNGwDuXyCk │
│ 4 │ GcFdQWvoLB │
│ 5 │ UkciuuOTVO │
│ 6 │ HoKeCdHkbs │
│ 7 │ xRvySxqAcR │
│ 8 │ LKbXPHpyDI │
│ 9 │ zxogHTzEVV │
└───┴────────────┘
```

## Passing query results to a script {#passing-query-results-to-a-script}

Users of the Hacker News website leave comments. Python contains a natural language processing toolkit (`nltk`) with a `SentimentIntensityAnalyzer` for determining if comments are positive, negative, or neutral - including assigning a value between -1 (a very negative comment) and 1 (a very positive comment). Let's create an `Executable` table that computes the sentiment of Hacker News comments using `nltk`.

This example uses the `hackernews` table described [here](/engines/table-engines/mergetree-family/textindexes/#hacker-news-dataset). The `hackernews` table includes an `id` column of type `UInt64` and a `String` column named `comment`. Let's start by defining the `Executable` table:

```sql
CREATE TABLE sentiment (
   id UInt64,
   sentiment Float32
)
ENGINE = Executable(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20)
);
```

Some comments about the `sentiment` table:

- The file `sentiment.py` is saved in the `user_scripts` folder (the default folder of the `user_scripts_path` setting)
- The `TabSeparated` format means our Python script needs to generate rows of raw data that contain tab-separated values
- The query selects two columns from `hackernews`. The Python script will need to parse out those column values from the incoming rows

Here is the definition of `sentiment.py`:

```python
#!/usr/local/bin/python3.9

import sys
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

def main():
    sentiment_analyzer = SentimentIntensityAnalyzer()

    while True:
        try:
            row = sys.stdin.readline()
            if row == '':
                break

            split_line = row.split("\t")

            id = str(split_line[0])
            comment = split_line[1]

            score = sentiment_analyzer.polarity_scores(comment)['compound']
            print(id + '\t' + str(score) + '\n', end='')
            sys.stdout.flush()
        except BaseException as x:
            break

if __name__ == "__main__":
    main()
```

Some comments about our Python script:

- For this to work, you will need to run `nltk.downloader.download('vader_lexicon')`. This could have been placed in the script, but then it would have been downloaded every time a query was executed on the `sentiment` table - which is not efficient
- Each value of `row` is going to be a row in the result set of `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20`
- The incoming row is tab-separated, so we parse out the `id` and `comment` using the Python `split` function
- The result of `polarity_scores` is a JSON object with a handful of values. We decided to just grab the `compound` value of this JSON object
- Recall that the `sentiment` table in ClickHouse uses the `TabSeparated` format and contains two columns, so our `print` function separates those columns with a tab

Every time you write a query that selects rows from the `sentiment` table, the `SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20` query is executed and the result is passed to `sentiment.py`. Let's test it out:

```sql
SELECT *
FROM sentiment
```

The response looks like:

```response
┌───────id─┬─sentiment─┐
│  7398199 │    0.4404 │
│ 21640317 │    0.1779 │
│ 21462000 │         0 │
│ 25168863 │         0 │
│ 25168978 │   -0.1531 │
│ 25169359 │         0 │
│ 25169394 │   -0.9231 │
│ 25169766 │    0.4137 │
│ 25172570 │    0.7469 │
│ 25173687 │    0.6249 │
│ 28291534 │         0 │
│ 28291669 │   -0.4767 │
│ 28291731 │         0 │
│ 28291949 │   -0.4767 │
│ 28292004 │    0.3612 │
│ 28292050 │    -0.296 │
│ 28292322 │         0 │
│ 28295172 │    0.7717 │
│ 28295288 │    0.4404 │
│ 21465723 │   -0.6956 │
└──────────┴───────────┘
```

## Creating an `ExecutablePool` table {#creating-an-executablepool-table}

The syntax for `ExecutablePool` is similar to `Executable`, but there are a couple of relevant settings unique to an `ExecutablePool` table:

- `pool_size`
  - Description: Processes pool size. If size is 0, then there are no size restrictions
  - Default value: 16
- `max_command_execution_time`
  - Description: Max command execution time in seconds
  - Default value: 10

We can easily convert the `sentiment` table above to use `ExecutablePool` instead of `Executable`:

```sql
CREATE TABLE sentiment_pooled (
   id UInt64,
   sentiment Float32
)
ENGINE = ExecutablePool(
    'sentiment.py',
    TabSeparated,
    (SELECT id, comment FROM hackernews WHERE id > 0 AND comment != '' LIMIT 20000)
)
SETTINGS
    pool_size = 4;
```

ClickHouse will maintain 4 processes on-demand when your client queries the `sentiment_pooled` table.
)DOCS_MD",
        .syntax = "ENGINE = ExecutablePool(script_name, format[, input_query...])",
        .related = {"Executable"}});
}

}
