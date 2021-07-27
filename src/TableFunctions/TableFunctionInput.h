#pragma once

#include <TableFunctions/ITableFunction.h>

namespace DB
{

class Context;

/* input(structure) - allows to make INSERT SELECT from incoming stream of data
 */
class TableFunctionInput : public ITableFunction
{
public:
    static constexpr auto name = "input";
    std::string getName() const override { return name; }
    bool hasStaticStructure() const override { return true; }

private:
    StoragePtr executeImpl(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns) const override;
    const char * getStorageTypeName() const override { return "Input"; }

    ColumnsDescription getActualTableStructure(ContextPtr context) const override;
    void parseArguments(const ASTPtr & ast_function, ContextPtr context) override;

    String structure;
};

namespace InputDoc
{
const char * doc = R"(
`input(structure)` - table function that allows effectively convert and insert data sent to the
server with given structure to the table with another structure.

`structure` - structure of data sent to the server in following format `'column1_name column1_type, column2_name column2_type, ...'`.
For example, `'id UInt32, name String'`.

This function can be used only in `INSERT SELECT` query and only once but otherwise behaves like ordinary table function
(for example, it can be used in subquery, etc.).

Data can be sent in any way like for ordinary `INSERT` query and passed in any available [format](../../interfaces/formats.md#formats)
that must be specified in the end of query (unlike ordinary `INSERT SELECT`).

The main feature of this function is that when server receives data from client it simultaneously converts it
according to the list of expressions in the `SELECT` clause and inserts into the target table. Temporary table
with all transferred data is not created.

**Examples**

-   Let the `test` table has the following structure `(a String, b String)`
    and data in `data.csv` has a different structure `(col1 String, col2 Date, col3 Int32)`. Query for insert
    data from the `data.csv` into the `test` table with simultaneous conversion looks like this:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   If `data.csv` contains data of the same structure `test_structure` as the table `test` then these two queries are equal:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```
)";

}

}
