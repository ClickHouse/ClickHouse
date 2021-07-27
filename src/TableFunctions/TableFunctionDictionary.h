#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{
class Context;

/* file(path, format, structure) - creates a temporary storage from file
 *
 * The file must be in the clickhouse data directory.
 * The relative path begins with the clickhouse data directory.
 */
class TableFunctionDictionary final : public ITableFunction
{
public:
    static constexpr auto name = "dictionary";
    std::string getName() const override
    {
        return name;
    }

    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;

    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription) const override;

    const char * getStorageTypeName() const override { return "Dictionary"; }

private:
    String dictionary_name;
    ColumnsDescription dictionary_columns;
};

namespace DictionaryDoc
{
const char * doc = R"(
Displays the [dictionary](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) data as a ClickHouse table. Works the same way as [Dictionary](../../engines/table-engines/special/dictionary.md) engine.

**Syntax**

``` sql
dictionary('dict')
```

**Arguments** 

-   `dict` — A dictionary name. [String](../../sql-reference/data-types/string.md).

**Returned value**

A ClickHouse table.

**Example**

Input table `dictionary_source_table`:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

Create a dictionary:

``` sql
CREATE DICTIONARY new_dictionary(id UInt64, value UInt64 DEFAULT 0) PRIMARY KEY id
SOURCE(CLICKHOUSE(HOST 'localhost' PORT tcpPort() USER 'default' TABLE 'dictionary_source_table')) LAYOUT(DIRECT());
```

Query:

``` sql
SELECT * FROM dictionary('new_dictionary');
```

Result:

``` text
┌─id─┬─value─┐
│  0 │     0 │
│  1 │     1 │
└────┴───────┘
```

**See Also**

-   [Dictionary engine](../../engines/table-engines/special/dictionary.md#dictionary)

)";

}

}
