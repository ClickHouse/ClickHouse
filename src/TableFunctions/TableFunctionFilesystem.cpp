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
        {
            .description = R"(
Provides access to file system to list files and return their metadata and contents. Recursively iterates directories.

## Access control

In server mode, the function is restricted to the `user_files` directory (controlled by the `user_files_path` server config).
Paths are resolved relative to `user_files`. Symlinks whose resolved path leaves the `user_files` subtree are skipped during traversal.
In `clickhouse-local` mode, any path on the local filesystem is accessible.

The `FILE` source access type is required.

## Arguments

- With no arguments, the function lists all files under `user_files`.
- With a single argument, it accepts an absolute or relative path. Relative paths are resolved against `user_files`.

## Columns

- `path` (`String`) — parent directory path.
- `name` (`String`) — file name (aliased as `file`).
- `type` (`Enum8`) — file type: `'none'`, `'not_found'`, `'regular'`, `'directory'`, `'symlink'`, `'block'`, `'character'`, `'fifo'`, `'socket'`, `'unknown'`.
- `size` (`Nullable(UInt64)`) — file size in bytes (NULL for non-regular files).
- `depth` (`UInt16`) — depth relative to the root path (0 for the root and its direct children).
- `modification_time` (`Nullable(DateTime64(6))`) — last modification time with microsecond precision.
- `is_symlink` (`Bool`) — whether the entry is a symbolic link.
- `content` (`Nullable(String)`) — file content for regular files, NULL otherwise.
- `owner_read`, `owner_write`, `owner_exec`, `group_read`, `group_write`, `group_exec`, `others_read`, `others_write`, `others_exec`, `set_gid`, `set_uid`, `sticky_bit` (`Bool`) — POSIX permission bits.

## Optimizations

- **Lazy content reading**: the `content` column is only read when explicitly selected. Queries that do not select `content` avoid file I/O entirely.
- **Predicate pushdown on metadata**: filters on cheap columns (`path`, `name`, `depth`, `type`, `is_symlink`) are evaluated before reading file content, size, or modification time.
- **Parallel traversal**: multiple threads traverse the directory tree concurrently via a shared bounded queue, enabling fast scanning of large directory trees.
- **Streaming LIMIT**: results are streamed as the traversal proceeds, so a `LIMIT` returns once enough rows have been emitted. Note: each individual directory is enumerated in full when first visited (children are pushed into the traversal queue eagerly), so `LIMIT` cannot interrupt scanning of a single very large directory.
)",
            .examples
            {
                {"List all files under user_files", R"(
```sql
SELECT name, size FROM filesystem()
```
)", ""},
                {"List files with a relative path", R"(
```sql
SELECT name, size FROM filesystem('my_data')
```
)", ""},
                {"List files with an absolute path", R"(
```sql
SELECT * FROM filesystem('/var/lib/clickhouse/user_files')
```
)", ""},
            },
            .category = FunctionDocumentation::Category::TableFunction
        }, {}, TableFunctionFactory::Case::Insensitive);
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
