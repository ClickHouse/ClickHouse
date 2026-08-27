#include <TableFunctions/TableFunctionMergeTreeParts.h>

#include <Analyzer/TableFunctionNode.h>
#include <Common/assert_cast.h>
#include <Common/FieldVisitorToString.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/parseColumnsListForTableFunction.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Disks/DiskFromAST.h>
#include <Disks/IDisk.h>

#include <fmt/ranges.h>

#include <algorithm>
#include <array>
#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}


/// The kind of source a disk with the given data source reads from; the same kinds that the
/// `file`, `s3`, ... table functions check access for.
static std::optional<AccessTypeObjects::Source> getSourceForDataSource(const DataSourceDescription & data_source)
{
    if (data_source.type == DataSourceType::Local)
        return AccessTypeObjects::Source::FILE;

    switch (data_source.object_storage_type)
    {
        case ObjectStorageType::S3:
            return AccessTypeObjects::Source::S3;
        case ObjectStorageType::Azure:
            return AccessTypeObjects::Source::AZURE;
        case ObjectStorageType::HDFS:
            return AccessTypeObjects::Source::HDFS;
        case ObjectStorageType::Web:
            return AccessTypeObjects::Source::URL;
        case ObjectStorageType::Local:
            return AccessTypeObjects::Source::FILE;
        case ObjectStorageType::None:
        case ObjectStorageType::Max:
            return std::nullopt;
    }
}

std::optional<AccessTypeObjects::Source> TableFunctionMergeTreeParts::getSourceAccessObject() const
{
    if (!source_access)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Arguments of table function `{}` are not parsed yet", getName());

    return source_access;
}

VectorWithMemoryTracking<size_t> TableFunctionMergeTreeParts::skipAnalysisForArguments(const QueryTreeNodePtr & query_node_table_function, ContextPtr) const
{
    const auto & table_function_node = query_node_table_function->as<TableFunctionNode &>();
    size_t arguments_size = table_function_node.getArguments().getNodes().size();

    VectorWithMemoryTracking<size_t> result_indexes;
    result_indexes.reserve(arguments_size);
    for (size_t i = 0; i < arguments_size; ++i)
        result_indexes.push_back(i);

    return result_indexes;
}

StoragePtr TableFunctionMergeTreeParts::executeImpl(
    const ASTPtr & /*ast_function*/, ContextPtr context, const String & table_name, ColumnsDescription /*cached_columns*/, bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);

    /// The access and readonly checks have passed by now, so the disk may be created. It is not
    /// registered in the context: it lives only as long as the storage, so a query cannot grow the
    /// global disk map or leave directories behind by merely being parsed.
    auto info = read_from_parts_info;
    info.disk = DiskFromAST::createTransientDisk(disk_function_ast, context);

    /// The access check trusted the literal `type` of the disk description; make sure the disk that
    /// came out of it reads from the same kind of source.
    if (getSourceForDataSource(info.disk->getDataSourceDescription()) != source_access)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "The disk of table function `{}` reads from a different kind of source ({}) than access was checked for",
                        getName(), info.disk->getDataSourceDescription().toString());

    auto storage = std::make_shared<StorageMergeTreeParts>(
        info,
        StorageID(getDatabaseName(), table_name),
        columns,
        ConstraintsDescription{},
        context);

    storage->startup();
    return storage;
}

ColumnsDescription TableFunctionMergeTreeParts::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    return parseColumnsListFromString(structure, context);
}

void TableFunctionMergeTreeParts::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    static constexpr auto arguments_num = 4;
    static const UnorderedSetWithMemoryTracking<std::string_view> arg_names = {
        "structure",
        "parts",
        "disk",
        "table_settings"
    };

    static const auto help_message = fmt::format(
        "Table function `{}` requires {} arguments: \n"
        "1) Column names and their types which will be read: `structure(x Int8, y String, ...)`\n"
        "2) Data parts information represented as a list: "
        "\n parts(\n\tWide(path='<part_relative_path>', marks_count=n, ranges=[(x1, y1), (x2, y2), ...], has_lightweight_delete=0)\n\t...\n)\n"
        "3) Disk configuration: `disk(type=s3, endpoint='<endpoint>', ...)`\n"
        "4) Settings: `table_settings(index_granularity_bytes=x, index_granularity=y, share_nested_offsets=z)`, "
        "of which only `index_granularity_bytes` is required",
        getName(), arguments_num);

    const auto & func_args = ast_function->as<ASTFunction &>();

    if (!func_args.arguments)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Table function `{}` must have arguments", getName());

    ASTs & args = func_args.arguments->children;

    if (args.size() != arguments_num)
        throw Exception(
            ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
            "Incorrect number of arguments: {}.\n{}",
            args.size(), help_message);

    auto throw_bad_argument = [](size_t arg_num, const std::string & hint = "")
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Incorrect argument: #{}{}.\n{}",
            arg_num, hint.empty() ? hint : ": " + hint, help_message);
    };

    auto get_function_args = [&](
        size_t argument_number,
        const ASTPtr & ast,
        const UnorderedSetWithMemoryTracking<std::string_view> & possible_function_names = {},
        std::optional<size_t> function_args_min_number = std::nullopt,
        std::optional<size_t> function_args_max_number = std::nullopt) -> ASTs
    {
        const auto * function = ast->as<ASTFunction>();

        if (!function)
            throw_bad_argument(argument_number, fmt::format("expected a function with name: {}", fmt::join(possible_function_names, ", ")));

        if (!possible_function_names.empty() && !possible_function_names.contains(function->name))
            throw_bad_argument(argument_number, fmt::format(
                                    "expected a function with name: {}, got: {} ({})",
                                    fmt::join(possible_function_names, ", "), function->name, function->dumpTree()));

        const auto * function_args_expr = assert_cast<const ASTExpressionList *>(function->arguments.get());
        if (!function_args_expr)
            throw_bad_argument(argument_number, fmt::format("expected expression list of `{}` function arguments", function->name));

        const auto & function_args = function_args_expr->children;
        if ((function_args_min_number && function_args.size() < *function_args_min_number)
            || (function_args_max_number && function_args.size() > *function_args_max_number))
        {
            auto min = function_args_min_number ? toString(*function_args_min_number) : "-";
            auto max = function_args_max_number ? toString(*function_args_max_number) : "-";
            throw_bad_argument(argument_number, fmt::format(
                                    "expected arguments size to be in range [{}, {}] in function `{}`, got {}",
                                    min, max, function->name, function_args.size()));
        }

        return function_args;
    };

    auto get_key_value_result = [&](
        size_t argument_number,
        const ASTPtr & ast,
        const UnorderedSetWithMemoryTracking<std::string_view> & possible_key_names = {}) -> std::pair<std::string, ASTPtr>
    {
        auto equals_function_args = get_function_args(argument_number, ast, {"equals"}, 2, 2);

        if (!equals_function_args[0]->as<ASTIdentifier>() && !equals_function_args[0]->as<ASTLiteral>())
            throw_bad_argument(argument_number, "expected key to be identifier or literal");

        auto literal = evaluateConstantExpressionOrIdentifierAsLiteral(equals_function_args[0], context);
        auto key = checkAndGetLiteralArgument<String>(literal, "key");

        if (!possible_key_names.empty() && !possible_key_names.contains(key))
            throw_bad_argument(argument_number, fmt::format(
                                    "expected argument to be one of: {}, got: {}",
                                    fmt::join(possible_key_names, ", "), key));

        return {key, equals_function_args[1]};
    };

    ASTPtr disk_function;
    size_t disk_arg_num = 0;
    UnorderedSetWithMemoryTracking<std::string_view> seen_arg_names;

    size_t arg_num = 1;
    for (const auto & arg : args)
    {
        const auto * function = arg->as<ASTFunction>();
        if (!function)
            throw_bad_argument(arg_num, fmt::format(
                                    "expected a function with name: {}, got not function: \"{}\"",
                                    fmt::join(arg_names, ", "), arg->formatForErrorMessage()));
        if (!arg_names.contains(function->name))
            throw_bad_argument(arg_num, fmt::format(
                                    "expected a function with name: {}, got: {}",
                                    fmt::join(arg_names, ", "), function->name));
        if (!seen_arg_names.emplace(*arg_names.find(function->name)).second)
            throw_bad_argument(arg_num, fmt::format("argument `{}` is passed more than once", function->name));

        if (function->name == "structure") /// Parse structure as `structure(x Int8, y String, ...)`.
        {
            const auto & structure_function_args = get_function_args(arg_num, arg, {"structure"}, 1, 1);
            structure = checkAndGetLiteralArgument<String>(structure_function_args[0], "structure");
        }
        else if (function->name == "parts")
        /// Parse data parts information:
        /// parts
        /// (
        ///     Wide(path='<part_relative_path>', marks_count=n, ranges=[(x1, y1), (x2, y2), ...], has_lightweight_delete=0)
        ///     ...
        /// )
        {
            auto parts_function_args = get_function_args(arg_num, arg, {"parts"});
            for (const auto & part_expr : parts_function_args)
            {
                StorageMergeTreeParts::ReadFromPartsInfo::ReadFromPart part;

                auto part_function_args = get_function_args(arg_num, part_expr, {"Wide", "Compact"}, 4, 4);
                auto function_name = part_expr->as<ASTFunction>()->name;
                part.type.fromString(function_name);

                /// path = '<path>'
                {
                    auto [_, path_ast] = get_key_value_result(arg_num, part_function_args[0], {"path"});
                    auto literal = evaluateConstantExpressionOrIdentifierAsLiteral(path_ast, context);
                    part.relative_path = checkAndGetLiteralArgument<String>(literal, "path");

                    /// The path is joined onto the root of the disk when the part is read, and
                    /// `std::filesystem` drops the left-hand side of the join for an absolute right-hand
                    /// side and keeps `..` components. Without this check a part path could step outside
                    /// the disk, which is the only thing that confines the query to what the disk exposes.
                    const fs::path part_path(part.relative_path);
                    if (part_path.is_absolute())
                        throw_bad_argument(arg_num, fmt::format(
                            "`path` must be relative to the root of the disk, got an absolute path: {}", part.relative_path));
                    for (const auto & component : part_path)
                        if (component == "..")
                            throw_bad_argument(arg_num, fmt::format(
                                "`path` must not leave the root of the disk, got: {}", part.relative_path));
                }

                /// marks_count = n
                {
                    auto [_, marks_count_ast] = get_key_value_result(arg_num, part_function_args[1], {"marks_count"});
                    auto literal = evaluateConstantExpressionOrIdentifierAsLiteral(marks_count_ast, context);
                    part.marks_count = checkAndGetLiteralArgument<UInt64>(literal, "marks_count");
                }

                /// ranges = [ ... ]
                {
                    auto [_, ranges_ast] = get_key_value_result(arg_num, part_function_args[2], {"ranges"});
                    auto literal = evaluateConstantExpressionAsLiteral(ranges_ast, context);
                    const auto & ranges_field = literal->as<ASTLiteral &>().value;
                    if (ranges_field.getType() != Field::Types::Array)
                        throw_bad_argument(arg_num, "expected an array in `ranges = [(x1, x2), ...]`");

                    for (const auto & range : ranges_field.safeGet<Array>())
                    {
                        if (range.getType() != Field::Types::Tuple)
                            throw_bad_argument(arg_num, "expected tuples in `ranges = [(x1, x2), ...]`");

                        const auto & range_tuple = range.safeGet<Tuple>();
                        if (range_tuple.size() != 2)
                            throw_bad_argument(arg_num, "expected tuples of two elements in `ranges = [(x1, x2), ...]`");

                        /// A negative bound would slip through `safeGet<UInt64>`, which lets `Int64`
                        /// through by reinterpretation.
                        if (range_tuple[0].getType() != Field::Types::UInt64 || range_tuple[1].getType() != Field::Types::UInt64)
                            throw_bad_argument(arg_num, "expected non-negative integers in `ranges = [(x1, x2), ...]`");

                        const auto range_begin = range_tuple[0].safeGet<UInt64>();
                        const auto range_end = range_tuple[1].safeGet<UInt64>();

                        /// The ranges drive the read scheduler directly, and `MarkRange` checks
                        /// `begin <= end` only with an assertion that release builds compile out, so
                        /// anything invalid here would underflow the mark arithmetic instead of failing.
                        if (range_begin >= range_end)
                            throw_bad_argument(arg_num, fmt::format(
                                "expected `begin < end` in every range of `ranges`, got ({}, {})", range_begin, range_end));
                        if (range_end > part.marks_count)
                            throw_bad_argument(arg_num, fmt::format(
                                "range ({}, {}) ends beyond `marks_count` = {}", range_begin, range_end, part.marks_count));
                        if (!part.ranges.empty() && range_begin < part.ranges.back().end)
                            throw_bad_argument(arg_num, fmt::format(
                                "expected the ranges of `ranges` to be sorted and non-overlapping, got ({}, {}) after ({}, {})",
                                range_begin, range_end, part.ranges.back().begin, part.ranges.back().end));

                        part.ranges.emplace_back(range_begin, range_end);
                    }
                }

                /// has_lightweight_delete=1(0)
                {
                    auto [_, has_lightweight_delete] = get_key_value_result(arg_num, part_function_args[3], {"has_lightweight_delete"});
                    auto literal = evaluateConstantExpressionOrIdentifierAsLiteral(has_lightweight_delete, context);
                    part.has_lightweight_delete = checkAndGetLiteralArgument<UInt64>(literal, "has_lightweight_delete");
                }

                read_from_parts_info.parts.push_back(std::move(part));
            }
        }
        else if (function->name == "disk")
        /// Parse disk configuration: `disk(type=s3, path='<path>', ...)`.
        {
            disk_function = arg;
            disk_arg_num = arg_num;
        }
        else if (function->name == "table_settings")
        /// Parse required settings:
        /// - index_granularity_bytes
        {
            const auto & settings_function_args = get_function_args(arg_num, arg, {"table_settings"});
            static const UnorderedSetWithMemoryTracking<std::string_view> settings = {
                "index_granularity_bytes", "index_granularity", "share_nested_offsets"};
            /// Everything else is left at its default, but these have no default that is right for
            /// every part: they decide how the marks and the streams of the part are laid out.
            static const UnorderedSetWithMemoryTracking<std::string_view> required_settings = {"index_granularity_bytes"};
            UnorderedMapWithMemoryTracking<std::string, std::string> parsed_settings;

            for (const auto & setting_arg : settings_function_args)
            {
                auto [key, setting_ast] = get_key_value_result(arg_num, setting_arg, settings);
                auto value_literal = evaluateConstantExpressionOrIdentifierAsLiteral(setting_ast, context);
                parsed_settings.emplace(key, convertFieldToString(value_literal->as<ASTLiteral>()->value));
            }

            for (const auto & setting : required_settings)
            {
                if (!parsed_settings.contains(std::string(setting)))
                    throw_bad_argument(arg_num, fmt::format("setting `{}` is required, but was not found in arguments", setting));
            }

            read_from_parts_info.index_granularity_bytes = parse<size_t>(parsed_settings["index_granularity_bytes"]);

            if (auto it = parsed_settings.find("index_granularity"); it != parsed_settings.end())
            {
                read_from_parts_info.index_granularity = parse<size_t>(it->second);
                if (!read_from_parts_info.index_granularity)
                    throw_bad_argument(arg_num, "setting `index_granularity` must not be zero");
            }

            if (auto it = parsed_settings.find("share_nested_offsets"); it != parsed_settings.end())
                read_from_parts_info.share_nested_offsets = parse<UInt64>(it->second) != 0;
        }

        ++arg_num;
    }

    if (!disk_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument `disk` is required, but was not found.\n{}", help_message);

    /// Prepare the description of the disk. The disk itself is created in `executeImpl`: creating a
    /// disk starts it (a local disk creates its directory right away), which must not happen before
    /// the access and readonly checks have passed.
    {
        ASTs disk_args = get_function_args(disk_arg_num, disk_function, {"disk"});

        /// The table function only reads, so the disk is created read-only. The access check would write a
        /// test file, which a read-only disk rejects, so it has to be skipped. Both are forced, so drop the
        /// values the query gave for them: the first occurrence of a key in the configuration wins.
        static constexpr std::array<std::string_view, 2> forced_disk_settings = {"read_only", "skip_access_check"};

        auto is_forced_setting = [&](const ASTPtr & disk_arg)
        {
            const auto * equals = disk_arg->as<ASTFunction>();
            if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
                return false;
            const auto * key = equals->arguments->children[0]->as<ASTIdentifier>();
            return key && std::ranges::find(forced_disk_settings, key->name()) != forced_disk_settings.end();
        };

        auto make_setting = [](std::string_view key, UInt64 value)
        {
            return makeASTFunction("equals", make_intrusive<ASTIdentifier>(String(key)), make_intrusive<ASTLiteral>(value));
        };

        auto disk_function_to_create = make_intrusive<ASTFunction>();
        disk_function_to_create->name = "disk";
        disk_function_to_create->arguments = make_intrusive<ASTExpressionList>();
        disk_function_to_create->children.push_back(disk_function_to_create->arguments);
        for (const auto & disk_arg : disk_args)
            if (!is_forced_setting(disk_arg))
                disk_function_to_create->arguments->children.push_back(disk_arg->clone());
        for (const auto & forced_setting : forced_disk_settings)
            disk_function_to_create->arguments->children.push_back(make_setting(forced_setting, 1));

        disk_function_ast = disk_function_to_create;

        /// Access is checked before the disk exists, so the source to check it for has to be readable
        /// from the description itself: the `type` (and `object_storage_type`) must be given literally.
        /// A substitution (`from_env`, `from_zk`, `include`) or a wrapper disk cannot tell what kind of
        /// source the query is going to read, so they are rejected here, before any of it is resolved.
        std::string type;
        std::string object_storage_type;
        for (const auto & disk_arg : disk_args)
        {
            const auto * equals = disk_arg->as<ASTFunction>();
            if (!equals || equals->name != "equals" || !equals->arguments || equals->arguments->children.size() != 2)
                continue;
            const auto * key = equals->arguments->children[0]->as<ASTIdentifier>();
            if (!key || (key->name() != "type" && key->name() != "object_storage_type"))
                continue;

            const auto & value_ast = equals->arguments->children[1];
            if (!value_ast->as<ASTLiteral>() && !value_ast->as<ASTIdentifier>())
                throw_bad_argument(disk_arg_num, fmt::format("expected the `{}` of the disk to be a literal", key->name()));

            auto value_literal = evaluateConstantExpressionOrIdentifierAsLiteral(value_ast, context);
            auto value = checkAndGetLiteralArgument<String>(value_literal, key->name());
            if (value.starts_with("from_env") || value.starts_with("from_zk"))
                throw_bad_argument(disk_arg_num, fmt::format(
                    "the `{}` of the disk must be a literal value, not a substitution", key->name()));

            if (key->name() == "type")
                type = value;
            else
                object_storage_type = value;
        }

        if (type.empty())
            throw_bad_argument(disk_arg_num, "the disk description requires an explicit `type`");

        auto source_for_type = [&](const std::string & disk_type) -> std::optional<AccessTypeObjects::Source>
        {
            if (disk_type == "local")
                return AccessTypeObjects::Source::FILE;
            if (disk_type == "s3" || disk_type.starts_with("s3_"))
                return AccessTypeObjects::Source::S3;
            if (disk_type == "azure_blob_storage" || disk_type == "azure")
                return AccessTypeObjects::Source::AZURE;
            if (disk_type == "hdfs")
                return AccessTypeObjects::Source::HDFS;
            if (disk_type == "web")
                return AccessTypeObjects::Source::URL;
            return std::nullopt;
        };

        /// `object_storage` is only a wrapper whose backend is chosen by `object_storage_type`.
        source_access = type == "object_storage" ? source_for_type(object_storage_type) : source_for_type(type);
        if (!source_access)
            throw_bad_argument(disk_arg_num, fmt::format(
                "table function `{}` cannot read from a disk of type `{}`",
                getName(), type == "object_storage" ? type + ", object_storage_type = " + object_storage_type : type));
    }
}

void registerTableFunctionMergeTreeParts(TableFunctionFactory & factory);
void registerTableFunctionMergeTreeParts(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionMergeTreeParts>(
        {.description = R"DOCS_MD(
Reads a set of MergeTree data parts that are described explicitly in the query, from a disk that is
also described in the query. It needs neither a table nor any table metadata on the server that runs
the query, so it can read the parts of a table that lives on another server, as long as the disk with
the data is reachable.

It is a low-level introspection function: everything that a `MergeTree` table would normally take from
its own metadata - the list of parts, the mark ranges to read, the column structure and the index
granularity - has to be passed as arguments. Passing values that do not match the data on the disk
results in an error or in garbage being returned.

## Syntax {#syntax}

```sql
mergeTreeParts(structure(...), parts(...), disk(...), table_settings(...))
```

## Arguments {#arguments}

| Argument         | Description                                                                        |
|------------------|------------------------------------------------------------------------------------|
| `structure`      | Column names and types to read, as a single string: `structure('x Int8, y String')`. |
| `parts`          | The list of data parts to read, see below.                                           |
| `disk`           | The disk holding the parts, configured the same way as in `SETTINGS disk = disk(...)` of a table definition. The disk is created read-only. |
| `table_settings` | The settings of the table the parts belong to that the reader cannot infer, see below. |

Every part in `parts` is described by a function named after the part type, `Wide` or `Compact`, whose
four arguments come in this order:

```sql
parts(
    Wide(path = 'store/707/70794cd7-9505-4011-9400-fde425bb25d1/20000101_1_1_0/',
         marks_count = 4,
         ranges = [(0, 3)],
         has_lightweight_delete = 0),
    ...
)
```

| Part argument            | Description                                                        |
|--------------------------|--------------------------------------------------------------------|
| `path`                   | Path of the part directory, relative to the root of the disk.        |
| `marks_count`            | Number of marks in the part (`marks` in `system.parts`).             |
| `ranges`                 | Mark ranges to read, as an array of half-open `(begin, end)` tuples. The ranges must be non-empty, sorted, non-overlapping, and end at or before `marks_count`. |
| `has_lightweight_delete` | Whether the part has a materialized lightweight delete mask.         |

`table_settings` carries the settings of the table the parts belong to that cannot be inferred from the
parts themselves:

| Setting                  | Description                                                        |
|--------------------------|--------------------------------------------------------------------|
| `index_granularity_bytes` | Required. The `index_granularity_bytes` of the table. `0` means the parts have non-adaptive marks, and then `index_granularity` is what the granule size is taken from. |
| `index_granularity`       | The `index_granularity` of the table, `8192` by default. It is only used for parts with non-adaptive marks. |
| `share_nested_offsets`    | The `share_nested_offsets` of the table, `1` by default. It decides the names of the offsets streams of a `Nested` column, so a part written with `share_nested_offsets = 0` cannot be read without passing it here. |

## Returned value {#returned-value}

A table with the columns given in `structure`, containing the rows of the requested mark ranges of the
requested parts. `PREWHERE` is supported and is pushed down to the part readers.

The function configures a disk from its arguments, so it is not allowed in readonly mode, and the
restrictions on dynamically configured disks apply: a local disk must live under
`custom_local_disks_base_directory`, and an S3 disk must not resolve the server's own credentials.
The `type` of the disk (and `object_storage_type` when `type = object_storage`) must be a literal
value: it decides what kind of source the access of the query is checked for, and the disk is only
created after that check passes. The disk is local to the query and is not registered on the server.

## Usage example {#usage-example}

```sql
SELECT count()
FROM mergeTreeParts(
    structure('dt Date, id Int64, data String'),
    parts(Wide(path = 'store/707/70794cd7-9505-4011-9400-fde425bb25d1/20000101_1_1_0/',
               marks_count = 1954,
               ranges = [(0, 1953)],
               has_lightweight_delete = 0)),
    disk(type = s3, endpoint = 'https://mybucket.s3.amazonaws.com/data/',
         access_key_id = '...', secret_access_key = '...'),
    table_settings(index_granularity_bytes = 10485760))
```
)DOCS_MD",
         .category = FunctionDocumentation::Category::TableFunction});
}

}
