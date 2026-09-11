#include <filesystem>
#include <Core/NamesAndAliases.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageFilesystem.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionFilesystem.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Interpreters/Context.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int UNEXPECTED_AST_STRUCTURE;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void registerTableFunctionFilesystem(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFilesystem>(
        {.description = R"DOCS_MD(
import CloudNotSupportedBadge from "/snippets/components/CloudNotSupportedBadge/CloudNotSupportedBadge.jsx";

<CloudNotSupportedBadge/>

Recursively iterates a directory and returns a table with file metadata (paths, sizes, types, permissions, modification times) and, optionally, file contents.

In `clickhouse-server` mode, the path must be within the [user_files_path](/reference/settings/server-settings/settings#user_files_path) directory. Symlinks inside `user_files_path` that point outside of it are followed, but only entries whose path (through the symlink) starts with `user_files_path` are returned.

In `clickhouse-local` mode, there are no path restrictions.

## Syntax {#syntax}

```sql
filesystem([path])
```

## Arguments {#arguments}

| Parameter | Description |
|-----------|-------------|
| `path`    | The directory to list. Can be an absolute path (must be inside `user_files_path` in server mode) or a path relative to `user_files_path`. If empty or omitted, defaults to `user_files_path`. |

## Returned columns {#returned_columns}

| Column              | Type                       | Description |
|---------------------|----------------------------|-------------|
| `path`              | `String`                   | Directory containing the entry (does not include the file/directory name itself). |
| `name`              | `String`                   | File or directory name (the last component of the path). |
| `file`              | `String` (ALIAS of `name`) | Alias for the `name` column. |
| `type`              | `Enum8`                    | File type: `'none'`, `'not_found'`, `'regular'`, `'directory'`, `'symlink'`, `'block'`, `'character'`, `'fifo'`, `'socket'`, `'unknown'`. |
| `size`              | `Nullable(UInt64)`         | File size in bytes (for regular files). `NULL` for non-regular files (directories, symlinks, etc.) and on error. |
| `depth`             | `UInt16`                   | Recursion depth. `0` for the queried directory itself and its immediate children, `1` for entries one level deeper, and so on. |
| `modification_time` | `Nullable(DateTime64(6))`  | Last modification time with microsecond precision. `NULL` on error. |
| `is_symlink`        | `Bool`                     | Whether the entry is a symbolic link. |
| `content`           | `Nullable(String)`         | File contents (for regular files). `NULL` for non-regular files (directories, symlinks, etc.). Read errors raise an exception. Reading this column triggers actual file I/O, so omit it if not needed. |
| `owner_read`        | `Bool`                     | Owner has read permission. |
| `owner_write`       | `Bool`                     | Owner has write permission. |
| `owner_exec`        | `Bool`                     | Owner has execute permission. |
| `group_read`        | `Bool`                     | Group has read permission. |
| `group_write`       | `Bool`                     | Group has write permission. |
| `group_exec`        | `Bool`                     | Group has execute permission. |
| `others_read`       | `Bool`                     | Others have read permission. |
| `others_write`      | `Bool`                     | Others have write permission. |
| `others_exec`       | `Bool`                     | Others have execute permission. |
| `set_gid`           | `Bool`                     | Set-GID bit. |
| `set_uid`           | `Bool`                     | Set-UID bit. |
| `sticky_bit`        | `Bool`                     | Sticky bit. |

Only columns actually used in the query are computed, so selecting a subset of columns (especially omitting `content`) is efficient.

## Examples {#examples}

### List files in user_files {#list-files}

```sql
SELECT name, type, size, depth
FROM filesystem()
ORDER BY name;
```

### Find large files {#find-large-files}

```sql
SELECT path, name, size
FROM filesystem()
WHERE type = 'regular' AND size > 1000000
ORDER BY size DESC;
```

### Read file contents {#read-contents}

```sql
SELECT name, content
FROM filesystem('my_directory')
WHERE name LIKE '%.csv';
```

### List only immediate children {#list-immediate}

```sql
SELECT name, type
FROM filesystem('my_directory')
WHERE depth = 0;
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {}, TableFunctionFactory::Case::Insensitive);
}

void TableFunctionFilesystem::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE, "Wrong AST structure in table function '{}'.", getName());

    ASTs & args = args_func.at(0)->children;

    /// With no arguments it assumes empty path.
    if (args.empty())
        return;

    if (args.size() > 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Table function '{}' requires path as its only argument.", getName());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    path = args.front()->as<ASTLiteral &>().value.safeGet<String>();
}

ColumnsDescription TableFunctionFilesystem::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    auto bool_type = DataTypeFactory::instance().get("Bool");

    DataTypeEnum8::Values file_type_values
    {
        {"none",        0},
        {"not_found",   1},
        {"regular",     2},
        {"directory",   3},
        {"symlink",     4},
        {"block",       5},
        {"character",   6},
        {"fifo",        7},
        {"socket",      8},
        {"unknown",     9},
    };
    auto file_type_enum = std::make_shared<DataTypeEnum8>(std::move(file_type_values));

    ColumnsDescription structure
    {
        {
            {"path", std::make_shared<DataTypeString>()},
            {"name", std::make_shared<DataTypeString>()},
            {"type", std::move(file_type_enum)},
            {"size", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>())},
            {"depth", std::make_shared<DataTypeUInt16>()},
            {"modification_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeDateTime64>(6))},
            {"is_symlink", bool_type},
            {"content", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
            {"owner_read", bool_type},
            {"owner_write", bool_type},
            {"owner_exec", bool_type},
            {"group_read", bool_type},
            {"group_write", bool_type},
            {"group_exec", bool_type},
            {"others_read", bool_type},
            {"others_write", bool_type},
            {"others_exec", bool_type},
            {"set_gid", bool_type},
            {"set_uid", bool_type},
            {"sticky_bit", bool_type}
        }
    };

    structure.setAliases(NamesAndAliases
    {
        {"file", std::make_shared<DataTypeString>(), "name"},
    });

    return structure;
}

StoragePtr TableFunctionFilesystem::executeImpl(const ASTPtr &, ContextPtr context, const std::string & table_name, ColumnsDescription, bool is_insert_query) const
{
    bool local_mode = context->getApplicationType() == Context::ApplicationType::LOCAL;

    /// Keep `user_files_path` in the same lexical namespace as user input: `fileOrSymlinkPathStartsWith`
    /// compares lexically-normalized absolute paths, so canonicalizing the prefix would reject otherwise
    /// valid absolute paths whenever `user_files_path` itself is a symlink. This also removes the
    /// requirement that `user_files_path` exist on disk (relevant for `clickhouse-local`).
    fs::path user_files_path(context->getUserFilesPath());
    String user_files_absolute_path_string = fs::absolute(user_files_path).lexically_normal().string();

    StoragePtr res = std::make_shared<StorageFilesystem>(
        StorageID(getDatabaseName(), table_name), getActualTableStructure(context, is_insert_query), ConstraintsDescription(), String{},
        local_mode, path, user_files_absolute_path_string);
    res->startup();
    return res;
}

}
