#include <Common/Exception.h>

#include <Storages/GenerateRandomSettings.h>
#include <Storages/StorageGenerateRandom.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSetQuery.h>

#include <TableFunctions/ITableFunction.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Functions/FunctionGenerateRandomStructure.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Common/randomSeed.h>

#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace GenerateRandomSetting
{
    extern const GenerateRandomSettingsFloat null_ratio;
    extern const GenerateRandomSettingsUInt64 max_json_depth;
    extern const GenerateRandomSettingsUInt64 max_json_keys_per_object;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int LOGICAL_ERROR;
}

namespace
{

/* generateRandom([structure, max_array_length, max_string_length, random_seed])
 * - creates a temporary storage that generates columns with random data
 */
class TableFunctionGenerateRandom : public ITableFunction
{
public:
    static constexpr auto name = "generateRandom";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return structure != "auto"; }

    bool needStructureHint() const override { return structure == "auto"; }
    void setStructureHint(const ColumnsDescription & structure_hint_) override { structure_hint = structure_hint_; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override { return "GenerateRandom"; }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String structure = "auto";
    GenerateRandomOptions options;
    std::optional<UInt64> random_seed;
    ColumnsDescription structure_hint;
};

void TableFunctionGenerateRandom::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;

    /// `SETTINGS k = v` is accepted as the last argument of a table function call, also as the only
    /// one. It is taken out of the argument list before anything else looks at the positions, so
    /// that `generateRandom(SETTINGS null_ratio = 0.2)` still means "no structure given".
    for (auto it = args.begin(); it != args.end(); ++it)
    {
        if (const auto * set_query = (*it)->as<ASTSetQuery>())
        {
            GenerateRandomSettings settings;
            settings.applyChanges(set_query->changes);
            settings.sanityCheck();

            options.null_ratio = settings[GenerateRandomSetting::null_ratio];
            options.max_json_depth = settings[GenerateRandomSetting::max_json_depth];
            options.max_json_keys_per_object = settings[GenerateRandomSetting::max_json_keys_per_object];

            args.erase(it);
            break;
        }
    }

    if (args.empty())
        return;

    /// First, check if first argument is structure or seed.
    const auto * first_arg_literal = args[0]->as<const ASTLiteral>();
    bool first_argument_is_structure = !first_arg_literal || first_arg_literal->value.getType() == Field::Types::String;
    size_t max_args = first_argument_is_structure ? 4 : 3;

    if (args.size() > max_args)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires at most four (or three if structure is missing) arguments: "
                        "[structure, random_seed, max_string_length, max_array_length].", getName());

    if (first_argument_is_structure)
    {
        /// Allow constant expression for structure argument, it can be generated using generateRandomStructure function.
        args[0] = evaluateConstantExpressionAsLiteral(args[0], context);
    }

    // All the arguments must be literals.
    for (const auto & arg : args)
    {
        const IAST * arg_raw = arg.get();
        if (const auto * func = arg_raw->as<const ASTFunction>();
            func && func->name == "_CAST" && func->arguments && !func->arguments->children.empty())
            arg_raw = func->arguments->children.at(0).get();

        if (!arg_raw->as<const ASTLiteral>())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "All arguments of table function '{}' except structure argument must be literals. "
                "Got '{}' instead", getName(), arg->formatForErrorMessage());
        }
    }

    size_t arg_index = 0;

    if (first_argument_is_structure)
    {
        /// Parsing first argument as table structure and creating a sample block
        structure = checkAndGetLiteralArgument<String>(args[arg_index], "structure");
        ++arg_index;
    }

    if (args.size() >= arg_index + 1)
    {
        const IAST * arg_raw = args[arg_index].get();
        if (const auto * func = arg_raw->as<const ASTFunction>(); func && func->name == "_CAST")
            arg_raw = func->arguments->children.at(0).get();

        const auto & literal = arg_raw->as<const ASTLiteral &>();
        ++arg_index;
        if (!literal.value.isNull())
            random_seed = checkAndGetLiteralArgument<UInt64>(literal, "random_seed");
    }

    if (args.size() >= arg_index + 1)
    {
        options.max_string_length = checkAndGetLiteralArgument<UInt64>(args[arg_index], "max_string_length");
        ++arg_index;
    }

    if (args.size() == arg_index + 1)
    {
        options.max_array_length = checkAndGetLiteralArgument<UInt64>(args[arg_index], "max_array_length");
        ++arg_index;
    }
}

ColumnsDescription TableFunctionGenerateRandom::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        if (structure_hint.empty())
        {
            auto random_structure = FunctionGenerateRandomStructure::generateRandomStructure(random_seed.value_or(randomSeed()), context);
            return parseColumnsListFromString(random_structure, context);
        }

        return structure_hint;
    }

    return parseColumnsListFromString(structure, context);
}

StoragePtr TableFunctionGenerateRandom::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    ColumnsDescription columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageGenerateRandom>(
        StorageID(getDatabaseName(), table_name), columns, String{}, options, random_seed);
    res->startup();
    return res;
}

}

void registerTableFunctionGenerate(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionGenerateRandom>({.description = R"DOCS_MD(
Generates random data with a given schema.
Allows populating test tables with that data.
All data types that can be stored in a table are supported, including `JSON`, `Dynamic`, `Variant`, `BFloat16`, `Time` and `Time64`.
The types `AggregateFunction`, `Interval`, `Nothing` and `QBit` are not supported.

## Syntax {#syntax}

```sql
generateRandom(['name TypeName[, name TypeName]...', [, 'random_seed'[, 'max_string_length'[, 'max_array_length']]]][, SETTINGS setting = value[, ...]])
```

## Arguments {#arguments}

| Argument            | Description                                                                                     |
|---------------------|-------------------------------------------------------------------------------------------------|
| `name`              | Name of corresponding column.                                                                   |
| `TypeName`          | Type of corresponding column.                                                                   |
| `random_seed`       | Specify random seed manually to produce stable results. If `NULL` — seed is randomly generated. |
| `max_string_length` | Maximum string length for all generated strings. Defaults to `10`.                              |
| `max_array_length`  | Maximum elements for all generated arrays or maps. Defaults to `10`.                            |

## Settings {#settings}

`SETTINGS` is the last argument of the call and controls how `Nullable`, `Variant`, `Dynamic` and `JSON` values are generated.

| Setting                    | Type     | Default  | Description                                                                                                                                                                  |
|----------------------------|----------|----------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `null_ratio`               | `Float`  | `0.0625` | Probability that a `Nullable`, `Variant` or `Dynamic` value is `NULL`, and the base probability that a `JSON` key is absent from a row - a minority of sparse keys are absent several times more often. Must be in `[0, 1]`.                                        |
| `max_json_depth`           | `UInt64` | `3`      | Maximum nesting depth of generated `JSON` objects: `1` means flat objects, objects inside arrays count as a level. Must be in `[1, 32]`.                                      |
| `max_json_keys_per_object` | `UInt64` | `8`      | Maximum number of generated keys on one level of a `JSON` object; the root object gets at least half of it. `0` means that only typed paths are generated. At most `1000`.    |

```sql
SELECT * FROM generateRandom('x JSON', 3, 4, 2, SETTINGS max_json_depth = 1, max_json_keys_per_object = 3) LIMIT 3 FORMAT JSONEachRow;
```

```text
{"x":{"cursor":"","id":[],"parent":-3723905664592977020}}
{"x":{"cursor":"","id":[7037090064212902336]}}
{"x":{"id":[287913403506346525],"parent":-4417942528676528098}}
```

`SETTINGS` can also be the only argument, which keeps the structure of the insertion table:

```sql
INSERT INTO test_table SELECT * FROM generateRandom(SETTINGS null_ratio = 0.5) LIMIT 10;
```

## Returned value {#returned-value}

A table object with requested schema.

## Usage Example {#usage-example}

```sql
SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2) LIMIT 3;
```

```text
┌─a────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ [77]     │ -124167.6723 │ ('2061-04-17 21:59:44.573','3f72f405-ec3e-13c8-44ca-66ef335f7835') │
│ [32,110] │ -141397.7312 │ ('1979-02-09 03:43:48.526','982486d1-5a5d-a308-e525-7bd8b80ffa73') │
│ [68]     │  -67417.0770 │ ('2080-03-12 14:17:31.269','110425e5-413f-10a6-05ba-fa6b3e929f15') │
└──────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

```sql
CREATE TABLE random (a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)) ENGINE=Memory;
INSERT INTO random SELECT * FROM generateRandom() LIMIT 2;
SELECT * FROM random;
```

```text
┌─a────────────────────────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ []                           │   68091.8197 │ ('2037-10-02 12:44:23.368','039ecab7-81c2-45ee-208c-844e5c6c5652') │
│ [8,-83,0,-22,65,9,-30,28,64] │ -186233.4909 │ ('2062-01-11 00:06:04.124','69563ea1-5ad1-f870-16d8-67061da0df25') │
└──────────────────────────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

In combination with [generateRandomStructure](/reference/functions/regular-functions/other-functions#generateRandomStructure):

```sql
SELECT * FROM generateRandom(generateRandomStructure(4, 101), 101) LIMIT 3;
```

```text
┌──────────────────c1─┬──────────────────c2─┬─c3─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─c4──────────────────────────────────────┐
│ 1996-04-15 06:40:05 │ 33954608387.2844801 │ ['232.78.216.176','9.244.59.211','211.21.80.152','44.49.94.109','165.77.195.182','68.167.134.239','212.13.24.185','1.197.255.35','192.55.131.232'] │ 45d9:2b52:ab6:1c59:185b:515:c5b6:b781   │
│ 2063-01-13 01:22:27 │ 36155064970.9514454 │ ['176.140.188.101']                                                                                                                                │ c65a:2626:41df:8dee:ec99:f68d:c6dd:6b30 │
│ 2090-02-28 14:50:56 │  3864327452.3901373 │ ['155.114.30.32']                                                                                                                                  │ 57e9:5229:93ab:fbf3:aae7:e0e4:d1eb:86b  │
└─────────────────────┴─────────────────────┴────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────┘
```

With missing `structure` argument (in this case the structure is random):

```sql
SELECT * FROM generateRandom() LIMIT 3;
```

```text
┌───c1─┬─────────c2─┬─────────────────────c3─┬──────────────────────c4─┬─c5───────┐
│ -128 │  317300854 │ 2030-08-16 08:22:20.65 │ 1994-08-16 12:08:56.745 │ R0qgiC46 │
│   40 │ -744906827 │ 2059-04-16 06:31:36.98 │ 1975-07-16 16:28:43.893 │ PuH4M*MZ │
│  -55 │  698652232 │ 2052-08-04 20:13:39.68 │ 1998-09-20 03:48:29.279 │          │
└──────┴────────────┴────────────────────────┴─────────────────────────┴──────────┘
```

With random seed both for random structure and random data:

```sql
SELECT * FROM generateRandom(11) LIMIT 3;
```

```text
┌───────────────────────────────────────c1─┬─────────────────────────────────────────────────────────────────────────────c2─┬─────────────────────────────────────────────────────────────────────────────c3─┬─────────c4─┬─────────────────────────────────────────────────────────────────────────────c5─┬──────────────────────c6─┬─c7──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┬─c8──────────────────────────────────────┬─────────c9─┐
│  -77422512305044606600216318673365695785 │   636812099959807642229.503817849012019401335326013846687285151335352272727523 │ -34944452809785978175157829109276115789694605299387223845886143311647505037529 │  544473976 │ 111220388331710079615337037674887514156741572807049614590010583571763691328563 │       22016.22623506465 │ {'2052-01-31 20:25:33':4306400876908509081044405485378623663,'1993-04-16 15:58:49':164367354809499452887861212674772770279,'2101-08-19 03:07:18':-60676948945963385477105077735447194811,'2039-12-22 22:31:39':-59227773536703059515222628111999932330} │ a7b2:8f58:4d07:6707:4189:80cf:92f5:902d │ 1950-07-14 │
│ -159940486888657488786004075627859832441 │  629206527868163085099.8195700356331771569105231840157308480121506729741348442 │ -53203761250367440823323469081755775164053964440214841464405368882783634063735 │ 2187136525 │  94881662451116595672491944222189810087991610568040618106057495823910493624275 │ 1.3095786748458954e-104 │ {}                                                                                                                                                                                                                                                      │ a051:e3da:2e0a:c69:7835:aed6:e8b:3817   │ 1943-03-25 │
│   -5239084224358020595591895205940528518 │ -529937657954363597180.1709207212648004850138812370209091520162977548101577846 │  47490343304582536176125359129223180987770215457970451211489086575421345731671 │ 1637451978 │ 101899445785010192893461828129714741298630410942962837910400961787305271699002 │  2.4344456058391296e223 │ {'2013-12-22 17:42:43':80271108282641375975566414544777036006,'2041-03-08 10:28:17':169706054082247533128707458270535852845,'1986-08-31 23:07:38':-54371542820364299444195390357730624136,'2094-04-23 21:26:50':7944954483303909347454597499139023465}  │ 1293:a726:e899:9bfc:8c6f:2aa1:22c9:b635 │ 1924-11-20 │
└──────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────┴────────────────────────────────────────────────────────────────────────────────┴────────────┴────────────────────────────────────────────────────────────────────────────────┴─────────────────────────┴─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┴─────────────────────────────────────────┴────────────┘
```

<Note>
`generateRandom(generateRandomStructure(), [random seed], max_string_length, max_array_length)` with a large enough `max_array_length` can generate a really huge output due to possible big nesting depth (up to 16) of complex types (`Array`, `Tuple`, `Map`, `Nested`).
</Note>

## Generating JSON {#generating-json}

A generated `JSON` column reads like a stream of documents of one schema: the set of keys and the type
of every key are derived from the random seed and stay the same for the whole column, while the values,
the absent keys and the array lengths change from row to row. The depth of the objects is bounded by
`max_json_depth`, the number of keys on one level by `max_json_keys_per_object`, and a key is absent
from a row with probability `null_ratio`. The leaves take the types the JSON parser infers for real
documents: `Int64`, `UInt64`, `Float64`, `Bool`, `String`, `Date`, `DateTime`, arrays of those, arrays
of objects and mixed arrays; a few keys carry a different type in a small fraction of the rows, as they
do in data collected from an application.

The type declaration shapes the generated schema: typed paths are always present with their declared
type, paths excluded by `SKIP` and `SKIP REGEXP` are never generated, and both `max_dynamic_paths` and
`max_dynamic_types` are respected, so the paths beyond `max_dynamic_paths` end up in the shared data of
the column.

```sql
SELECT * FROM generateRandom('x JSON', 1) LIMIT 3 FORMAT JSONEachRow;
```

```text
{"x":{"product_id":"2050-12-17 01:46:35","stage":"g|(&Ql","started":{"group":-80611897324989285}}}
{"x":{"product_id":"2013-10-17 22:35:26","stage":"^ipx|,=a5N","started":{"group":-1326235429680389454}}}
{"x":{"product_id":"1974-11-17 22:22:46","stage":"(U]p'l`","started":{"group":-344141642787805595}}}
```

The structure can also be left to the insertion table, which is convenient for filling a table with
`JSON`, `Dynamic` or `Variant` columns:

```sql
CREATE TABLE t (x JSON) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t SELECT * FROM generateRandom() LIMIT 10;
```

## Related content {#related-content}
- Blog: [Generating random data in ClickHouse](https://clickhouse.com/blog/generating-random-test-distribution-data-for-clickhouse)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});
}

}
