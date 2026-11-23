#include <Interpreters/evaluateConstantExpression.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include <Storages/StorageFile.h>
#include <Storages/StorageURL.h>
#include <Storages/VirtualColumnUtils.h>
#include <Storages/checkAndGetLiteralArgument.h>

#include <Common/Exception.h>
#include <Common/assert_cast.h>
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

    const bool is_table_definition = isTableDefinition(cached_columns);
    StoragePtr storage = getStorage(
        filename, format, columns, context, table_name, compression_method, is_insert_query, is_table_definition);

    /// Without a structure argument, a table reads its stored columns by name from the inferred ones (see
    /// `StorageTableFunctionProxy`). If it has none of them, as `CREATE TABLE t (id UInt32, s String) AS file('data.csv', 'CSV')`
    /// with columns `c1` and `c2`, it could not read anything, so create the storage with the ordinary stored columns instead,
    /// as the object storage table functions do, if it can evaluate their `DEFAULT`s and parse the data with them.
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

        /// The inferred columns also have the columns of the hive-style partitions of the path, which are not in the data. The
        /// storage created with the stored columns also reads those of them that it has from the path. It is created with the
        /// format that the storage has detected if the format is `auto`, so it does not read the data to detect it again.
        const NamesAndTypesList * partition_columns = nullptr;
        String detected_format = format;
        /// NOLINT(storage-cast): the storage `getStorage` has just built, never a proxy.
        auto * storage_file = dynamic_cast<StorageFile *>(storage.get());
        if (storage_file)
        {
            partition_columns = &storage_file->getHivePartitionColumnsToReadFromFilePath();
            detected_format = storage_file->getFormatName();
        }
        /// NOLINT(storage-cast): the same storage.
        else if (const auto * storage_url = dynamic_cast<const IStorageURLBase *>(storage.get()))
        {
            partition_columns = &storage_url->getHivePartitionColumnsToReadFromFilePath();
            detected_format = storage_url->getFormatName();
        }
        const auto is_partition_column = [&](const String & name)
        {
            return partition_columns && partition_columns->contains(name);
        };

        /// A partition column does not make the table read the inferred columns by name, as for `key` of
        /// `(id UInt32, s String, key UInt32)` of `.../key=7/data.csv`, unless it is a `MATERIALIZED` stored column.
        bool has_inferred_column = false;
        for (const auto & inferred_column : metadata->columns.get(GetColumnsOptions(GetColumnsOptions::AllPhysical).withSubcolumns()))
        {
            if (!is_partition_column(inferred_column.getNameInStorage()))
                has_inferred_column = has_inferred_column || is_stored(inferred_column.name);
        }

        bool has_other_column = false;
        ColumnsDescription stored_columns;
        size_t num_stored_data_columns = 0;
        for (const auto & column : cached_columns)
        {
            const auto kind = column.default_desc.kind;
            if (kind != ColumnDefaultKind::Default && kind != ColumnDefaultKind::Materialized)
                continue;

            const bool partition = kind == ColumnDefaultKind::Default && is_partition_column(column.name);
            const bool inferred = !partition && is_inferred(column.name);
            has_inferred_column = has_inferred_column || inferred;

            if (kind != ColumnDefaultKind::Default || (!inferred && !partition && metadata->virtuals.has(column.name)))
                continue;
            has_other_column = has_other_column || (!inferred && !partition);
            num_stored_data_columns += !partition;
            stored_columns.add(column);
        }

        /// The storage could not evaluate a `DEFAULT` expression that uses a column it does not have, as `helper` of
        /// `(id UInt32 DEFAULT helper, helper UInt32 ALIAS 42)`.
        const auto defaults_use_only_stored_columns = [&]
        {
            for (const auto & column : stored_columns)
            {
                if (!column.default_desc.expression)
                    continue;
                IdentifierNameSet identifiers;
                column.default_desc.expression->collectIdentifierNames(identifiers);
                for (const auto & identifier : identifiers)
                {
                    if (!stored_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, identifier)
                        && cached_columns.hasColumnOrSubcolumn(GetColumnsOptions::All, identifier))
                        return false;
                }
            }
            return true;
        };

        const auto can_parse_data_with_stored_columns = [&]
        {
            /// `StorageFile` always takes the columns of a `Distributed` file from its header, not the given ones.
            if (detected_format == "Distributed")
                return false;
            size_t num_data_columns = 0;
            for (const auto & inferred_column : metadata->columns.getOrdinary())
                num_data_columns += !is_partition_column(inferred_column.name);
            if (num_stored_data_columns == num_data_columns)
                return true;
            /// With the settings of the context that creates the storage, as `input_format_with_names_use_header` for
            /// `CSVWithNames`, which a query could change, as for a table with these columns given as the structure argument.
            return FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(detected_format, context);
        };
        if (has_other_column && !has_inferred_column && defaults_use_only_stored_columns() && can_parse_data_with_stored_columns())
        {
            auto storage_with_stored_columns = getStorage(
                filename, detected_format, stored_columns, context, table_name, compression_method, is_insert_query,
                is_table_definition);
            /// The data of a file descriptor, such as `stdin`, that the first storage has read to infer the columns cannot be
            /// read again.
            if (storage_file)
            {
                /// NOLINT(storage-cast): the storage `getStorage` has just built for the same file.
                storage_file->moveReadBufferFromFileDescriptorTo(assert_cast<StorageFile &>(*storage_with_stored_columns));
            }
            storage = std::move(storage_with_stored_columns);
        }
    }

    storage->startup();
    return storage;
}

}
