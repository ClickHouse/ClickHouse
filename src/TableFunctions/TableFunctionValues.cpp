#include <Columns/IColumn.h>

#include <Common/typeid_cast.h>
#include <Common/Exception.h>

#include <Core/Block.h>
#include <Storages/StorageValues.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/getLeastSupertype.h>

#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>

#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Interpreters/convertFieldToType.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <TableFunctions/registerTableFunctions.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int CANNOT_EXTRACT_TABLE_STRUCTURE;
}

namespace
{

void parseAndInsertValues(MutableColumns & res_columns, const ASTs & args, const Block & sample_block, size_t start, ContextPtr context)
{
    if (res_columns.size() == 1) /// Parsing arguments as Fields
    {
        for (size_t i = start; i < args.size(); ++i)
        {
            const auto & [value_field, value_type_ptr] = evaluateConstantExpression(args[i], context);

            Field value = convertFieldToTypeOrThrow(value_field, *sample_block.getByPosition(0).type, value_type_ptr.get(), {}, /*convert_inexact_floats=*/true);
            res_columns[0]->insert(value);
        }
    }
    else /// Parsing arguments as Tuples
    {
        for (size_t i = start; i < args.size(); ++i)
        {
            const auto & [value_field, value_type_ptr] = evaluateConstantExpression(args[i], context);

            const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(value_type_ptr.get());
            if (!type_tuple)
                throw Exception(ErrorCodes::BAD_ARGUMENTS,
                    "Table function VALUES requires all but the first argument (rows specification) to be either tuples or single values");

            const Tuple & value_tuple = value_field.safeGet<Tuple>();

            if (value_tuple.size() != sample_block.columns())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Values size should match with the number of columns");

            const DataTypes & value_types_tuple = type_tuple->getElements();
            for (size_t j = 0; j < value_tuple.size(); ++j)
            {
                Field value = convertFieldToTypeOrThrow(value_tuple[j], *sample_block.getByPosition(j).type, value_types_tuple[j].get(), {}, /*convert_inexact_floats=*/true);
                res_columns[j]->insert(value);
            }
        }
    }
}

DataTypes getTypesFromArgument(const ASTPtr & arg, ContextPtr context)
{
    const auto & [value_field, value_type_ptr] = evaluateConstantExpression(arg, context);
    if (const DataTypeTuple * type_tuple = typeid_cast<const DataTypeTuple *>(value_type_ptr.get()))
        return type_tuple->getElements();

    return {value_type_ptr};
}

/* values(structure, values...) - creates a temporary storage filling columns with values
 * values is case-insensitive table function.
 *
 * When interpret_first_argument_as_structure is true (default), the first string argument
 * may be interpreted as a column schema definition (e.g. 'x UInt8, y String').
 * When false (used by SQL standard VALUES clause rewrite), the first argument is always
 * treated as row data, never as a schema.
 */
template <bool interpret_first_argument_as_structure>
class TableFunctionValues : public ITableFunction
{
public:
    static constexpr auto name = interpret_first_argument_as_structure ? "values" : "SQLStandardValues";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }
private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const override;
    const char * getStorageEngineName() const override
    {
        /// It'd be StorageValues but it's not registered as a table engine
        return "";
    }

    ColumnsDescription getActualTableStructure(ContextPtr context, bool is_insert_query) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription structure;
    bool has_structure_in_arguments = false;
};

template <bool interpret_first_argument_as_structure>
void TableFunctionValues<interpret_first_argument_as_structure>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments", getName());

    ASTs & args = args_func.at(0)->children;

    if (args.empty())
        throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION, "Table function '{}' requires at least 1 argument", getName());

    if constexpr (interpret_first_argument_as_structure)
    {
        const auto & literal = args[0]->as<const ASTLiteral>();
        String value;
        String error;
        if (args.size() > 1 && literal && literal->value.tryGet(value) && tryParseColumnsListFromString(value, structure, context, error))
        {
            has_structure_in_arguments = true;
            return;
        }
    }

    has_structure_in_arguments = false;
    DataTypes data_types = getTypesFromArgument(args[0], context);
    for (size_t i = 1; i < args.size(); ++i)
    {
        auto arg_types = getTypesFromArgument(args[i], context);
        if (data_types.size() != arg_types.size())
            throw Exception(
                ErrorCodes::CANNOT_EXTRACT_TABLE_STRUCTURE,
                "Cannot determine a common structure for {} function arguments: the amount of columns is different for different arguments",
                getName());
        for (size_t j = 0; j != arg_types.size(); ++j)
            data_types[j] = getLeastSupertype(DataTypes{data_types[j], arg_types[j]});
    }

    NamesAndTypesList names_and_types;
    for (size_t i = 0; i != data_types.size(); ++i)
        names_and_types.emplace_back("c" + std::to_string(i + 1), data_types[i]);
    structure = ColumnsDescription(names_and_types);
}

template <bool interpret_first_argument_as_structure>
ColumnsDescription TableFunctionValues<interpret_first_argument_as_structure>::getActualTableStructure(ContextPtr /*context*/, bool /*is_insert_query*/) const
{
    return structure;
}

template <bool interpret_first_argument_as_structure>
StoragePtr TableFunctionValues<interpret_first_argument_as_structure>::executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);

    Block sample_block;
    for (const auto & name_type : columns.getOrdinary())
        sample_block.insert({ name_type.type->createColumn(), name_type.type, name_type.name });

    MutableColumns res_columns = sample_block.cloneEmptyColumns();

    ASTs & args = ast_function->children.at(0)->children;

    /// Parsing other arguments as values and inserting them into columns
    parseAndInsertValues(res_columns, args, sample_block, has_structure_in_arguments ? 1 : 0, context);

    Block res_block = sample_block.cloneWithColumns(std::move(res_columns));

    auto res = std::make_shared<StorageValues>(StorageID(getDatabaseName(), table_name), columns, res_block);
    res->startup();
    return res;
}

}

void registerTableFunctionValues(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionValues<true>>({.description = R"DOCS_MD(
The `Values` table function allows you to create temporary storage which fills 
columns with values. It is useful for quick testing or generating sample data.

<Note>
Values is a case-insensitive function. I.e. `VALUES` or `values` are both valid.
</Note>

## Syntax {#syntax}

The basic syntax of the `VALUES` table function is:

```sql
VALUES([structure,] values...)
```

It is commonly used as:

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

## Arguments {#arguments}

- `column1_name Type1, ...` (optional). [String](/reference/data-types/string) 
  specifying the column names and types. If this argument is omitted columns will
  be named as `c1`, `c2`, etc.
- `(value1_row1, value2_row1)`. [Tuples](/reference/data-types/tuple) 
   containing values of any type.

<Note>
Comma separated tuples can be replaced by single values as well. In this case
each value is taken to be a new row. See the [examples](#examples) section for
details.
</Note>

## Returned value {#returned-value}

- Returns a temporary table containing the provided values.

## Examples {#examples}

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

`VALUES` can also be used with single values rather than tuples. For example:

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

Or without providing a row specification (`'column1_name Type1, column2_name Type2, ...'`
in the [syntax](#syntax)), in which case the columns are automatically named. 

For example:

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```   

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

## SQL Standard VALUES Clause {#sql-standard-values-clause}

From version 26.3, ClickHouse also supports the SQL standard `VALUES` clause as a table expression
in `FROM`, as used in PostgreSQL, MySQL, DuckDB, and SQL Server. This syntax is
rewritten internally to use the `values` table function described above.

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

It can be used in CTEs:

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

And in JOINs:

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

<Note>
Column aliases after `AS t(col1, col2, ...)` follow the standard SQL syntax for
naming columns of derived tables. If omitted, columns are named `c1`, `c2`, etc.
</Note>

## See also {#see-also}

- [Values format](/reference/formats/Values)
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true}, TableFunctionFactory::Case::Insensitive);
    factory.registerFunction<TableFunctionValues<false>>({.description = R"(
Internal table function used to implement SQL standard VALUES clause syntax.
Created automatically by the parser when it encounters (VALUES (row1), (row2), ...) in a FROM clause.
)", .category = FunctionDocumentation::Category::Internal}, {.allow_readonly = true});
}

}
