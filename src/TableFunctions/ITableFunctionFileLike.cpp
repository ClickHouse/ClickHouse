#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Storages/StorageFile.h>
#include <Storages/StorageURL.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Common/Exception.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

void ITableFunctionFileLike::parseFirstArguments(const ASTPtr & arg, const ContextPtr &)
{
    filename = checkAndGetLiteralArgument<String>(arg, "source");
}

std::optional<String> ITableFunctionFileLike::tryGetFormatFromFirstArgument()
{
    return FormatFactory::instance().tryGetFormatFromFileName(filename);
}

bool ITableFunctionFileLike::supportsReadingSubsetOfColumns(const ContextPtr & context)
{
    return format != "auto" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format, context);
}

NameSet ITableFunctionFileLike::getVirtualsToCheckBeforeUsingStructureHint() const
{
    return VirtualColumnUtils::getVirtualNamesForFileLikeStorage();
}

void ITableFunctionFileLike::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    /// Parse args
    ASTs & args_func = ast_function->children;

    if (args_func.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", getName());

    ASTs & args = args_func.at(0)->children;
    parseArgumentsImpl(args, context);
}

void ITableFunctionFileLike::parseArgumentsImpl(ASTs & args, const ContextPtr & context)
{
    if (args.empty() || args.size() > getMaxNumberOfArguments())
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "The signature of table function {} shall be the following:\n{}", getName(), getSignature());

    for (auto & arg : args)
        arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

    parseFirstArguments(args[0], context);

    if (args.size() > 1)
        format = checkAndGetLiteralArgument<String>(args[1], "format");

    if (format == "auto")
    {
        if (auto format_from_first_argument = tryGetFormatFromFirstArgument())
            format = *format_from_first_argument;
    }

    if (args.size() > 2)
    {
        structure = checkAndGetLiteralArgument<String>(args[2], "structure");
        if (structure.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Table structure is empty for table function '{}'. If you want to use automatic schema inference, use 'auto'",
                getName());
    }

    if (args.size() > 3)
        compression_method = checkAndGetLiteralArgument<String>(args[3], "compression_method");
}

StoragePtr ITableFunctionFileLike::executeImpl(const ASTPtr & /*ast_function*/, ContextPtr context, const std::string & table_name, ColumnsDescription cached_columns, bool is_insert_query) const
{
    ColumnsDescription columns;
    if (structure != "auto")
        columns = parseColumnsListFromString(structure, context);
    else if (!structure_hint.empty())
        columns = structure_hint;

    StoragePtr storage = getStorage(filename, format, columns, context, table_name, compression_method, is_insert_query);

    /// Without a structure argument, the storage infers the columns of the data, and the stored columns of a table, given in
    /// `CREATE` or inferred then, are read from it by name (see `StorageTableFunctionProxy`), so `(c2 String, c1 Int64)` of a CSV
    /// file reads its second and first columns, and a query reads a stored column only if the storage has it. If a query can
    /// reach none of the inferred columns, as for `id` and `s` of `CREATE TABLE t (id UInt32, s String) AS file('data.csv',
    /// 'CSV')`, the table could not be read at all, so its storage is created with the ordinary stored columns instead, as the
    /// object storage table functions do, except those that it has as virtual columns, such as `_file`, which stay virtual. The
    /// storage does not support the other kinds of columns. It is created so only when it can parse the data with them: the
    /// format can read a subset of its columns, or there are as many of them as inferred columns. Otherwise, as for
    /// `(id UInt32)` of a CSV file with two columns, a query of only virtual columns, such as `_row_number`, reads the data
    /// with the inferred columns, while the stored columns could not parse its rows. A table that can read some of the
    /// inferred columns keeps reading them by name: a query names an ordinary or a `MATERIALIZED` stored column or a
    /// subcolumn of one, also a dynamic one, and the storage has that name as a column or a subcolumn, such as `nested.value`
    /// of `nested Tuple(value UInt32)` or `j.a` of `j JSON` for a `JSONEachRow` file with the key `nested.value` or `j.a`.
    if (columns.empty() && !cached_columns.empty())
    {
        const auto metadata = storage->getInMemoryMetadataPtr(context, /*bypass_metadata_cache=*/false);
        const auto is_inferred = [&](const String & name)
        {
            return metadata->columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name);
        };
        const auto is_stored = [&](const String & name)
        {
            return cached_columns.hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, name);
        };

        bool has_inferred_column = false;
        for (const auto & inferred_column : metadata->columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()))
            has_inferred_column = has_inferred_column || is_stored(inferred_column.name);

        bool has_other_column = false;
        ColumnsDescription stored_columns;
        for (const auto & column : cached_columns)
        {
            const auto kind = column.default_desc.kind;
            if (kind != ColumnDefaultKind::Default && kind != ColumnDefaultKind::Materialized)
                continue;

            const bool inferred = is_inferred(column.name);
            has_inferred_column = has_inferred_column || inferred;

            if (kind != ColumnDefaultKind::Default || (!inferred && metadata->virtuals.has(column.name)))
                continue;
            has_other_column = has_other_column || !inferred;
            stored_columns.add(column);
        }

        const auto can_parse_data_with_stored_columns = [&]
        {
            /// The inferred columns also have the columns of the hive-style partitions of the path, which are not in the data.
            const NamesAndTypesList * partition_columns = nullptr;
            if (const auto * storage_file = dynamic_cast<const StorageFile *>(storage.get()))
                partition_columns = &storage_file->getHivePartitionColumnsToReadFromFilePath();
            else if (const auto * storage_url = dynamic_cast<const IStorageURLBase *>(storage.get()))
                partition_columns = &storage_url->getHivePartitionColumnsToReadFromFilePath();
            size_t num_data_columns = 0;
            for (const auto & inferred_column : metadata->columns.getOrdinary())
            {
                if (!partition_columns || !partition_columns->contains(inferred_column.name))
                    ++num_data_columns;
            }
            if (stored_columns.size() == num_data_columns)
                return true;
            /// With the settings of the context that creates the storage, as `input_format_with_names_use_header` for
            /// `CSVWithNames`, which a query could change, as for a table with these columns given as the structure argument.
            return format != "auto" && FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(format, context);
        };
        if (has_other_column && !has_inferred_column && can_parse_data_with_stored_columns())
            storage = getStorage(filename, format, stored_columns, context, table_name, compression_method, is_insert_query);
    }

    storage->startup();
    return storage;
}

}
