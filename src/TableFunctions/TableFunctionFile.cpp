#include <Interpreters/parseColumnsListForTableFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/TableFunctionFile.h>

#include <Core/Field.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>
#include <Storages/HivePartitioningUtils.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_archive_path_syntax;
    extern const SettingsString rename_files_after_processing;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

StorageFile::FileSource parseFileSourceFromStringArray(const Array & sources, const ContextPtr & context)
{
    if (sources.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of table function 'file' must contain at least one path");

    StorageFile::FileSource result;
    bool is_first_source = true;
    bool has_different_formats = false;
    for (const auto & source_field : sources)
    {
        if (source_field.getType() != Field::Types::String)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "All elements of the first argument of table function 'file' must have type String, got {}",
                fieldTypeToString(source_field.getType()));

        auto source = StorageFile::FileSource::parse(source_field.safeGet<String>(), context);
        if (source.archive_info)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Array source for table function 'file' does not support archive path syntax");

        result.paths.insert(result.paths.end(), source.paths.begin(), source.paths.end());
        result.total_bytes_to_read += source.total_bytes_to_read;

        if (is_first_source)
        {
            result.format_from_filenames = source.format_from_filenames;
            is_first_source = false;
        }
        else if (result.format_from_filenames != source.format_from_filenames)
        {
            has_different_formats = true;
        }
    }

    if (has_different_formats)
        result.format_from_filenames = {};

    /// Array sources are supported only for reading, even if the array contains a single path.
    result.with_globs = true;
    if (!result.paths.empty())
        result.path_for_partitioned_write = result.paths.front();

    return result;
}

}

void TableFunctionFile::parseFirstArguments(const ASTPtr & arg, const ContextPtr & context)
{
    const auto * literal = arg->as<ASTLiteral>();
    if (!literal)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "The first argument of table function '{}' must be a path, an array of paths, or a file descriptor",
            getName());

    auto type = literal->value.getType();
    if (type == Field::Types::Array)
    {
        if (getName() != name)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of table function '{}' must be a path, not an array", getName());

        filename = arg->formatForErrorMessage();
        file_source = parseFileSourceFromStringArray(literal->value.safeGet<Array>(), context);
        return;
    }

    if (context->getApplicationType() != Context::ApplicationType::LOCAL)
    {
        ITableFunctionFileLike::parseFirstArguments(arg, context);
        file_source = StorageFile::FileSource::parse(filename, context);
        return;
    }

    if (type == Field::Types::String)
    {
        filename = literal->value.safeGet<String>();
        if (filename == "stdin" || filename == "-")
            fd = STDIN_FILENO;
        else if (filename == "stdout")
            fd = STDOUT_FILENO;
        else if (filename == "stderr")
            fd = STDERR_FILENO;
        else
            file_source = StorageFile::FileSource::parse(filename, context);
    }
    else if (type == Field::Types::Int64 || type == Field::Types::UInt64)
    {
        fd = static_cast<int>(
            (type == Field::Types::Int64) ? literal->value.safeGet<Int64>() : literal->value.safeGet<UInt64>());
        if (fd < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "File descriptor must be non-negative");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of table function '{}' must be path or file descriptor", getName());
}

std::optional<String> TableFunctionFile::tryGetFormatFromFirstArgument()
{
    if (fd >= 0)
        return FormatFactory::instance().tryGetFormatFromFileDescriptor(fd);

    chassert(file_source); /// TableFunctionFile::parseFirstArguments() initializes either `fd` or `file_source`.
    return file_source->format_from_filenames;
}

StoragePtr TableFunctionFile::getStorage(
    const String & /*source*/,
    const String & format_,
    const ColumnsDescription & columns,
    ContextPtr global_context,
    const std::string & table_name,
    const std::string & compression_method_,
    bool /*is_insert_query*/) const
{
    // For `file` table function, we are going to use format settings from the
    // query context.
    StorageFile::CommonArguments args{
        WithContext(global_context),
        StorageID(getDatabaseName(), table_name),
        format_,
        std::nullopt /*format settings*/,
        compression_method_,
        columns,
        ConstraintsDescription{},
        String{},
        global_context->getSettingsRef()[Setting::rename_files_after_processing],
    };

    if (fd >= 0)
        return std::make_shared<StorageFile>(fd, args);

    chassert(file_source); /// TableFunctionFile::parseFirstArguments() initializes either `fd` or `file_source`.
    return std::make_shared<StorageFile>(*file_source, args);
}

ColumnsDescription TableFunctionFile::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        if (fd >= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema inference is not supported for table function '{}' with file descriptor", getName());

        chassert(file_source); /// TableFunctionFile::parseFirstArguments() initializes either `fd` or `file_source`.

        ColumnsDescription columns;
        if (format == "auto")
            columns = StorageFile::getTableStructureAndFormatFromFile(file_source->paths, compression_method, std::nullopt, context, file_source->archive_info).first;
        else
            columns = StorageFile::getTableStructureFromFile(format, file_source->paths, compression_method, std::nullopt, context, file_source->archive_info);

        auto sample_path = file_source->paths.empty() ? String{} : file_source->paths.front();

        HivePartitioningUtils::setupHivePartitioningForFileURLLikeStorage(
            columns,
            sample_path,
            /* inferred_schema */ true,
            /* format_settings */ std::nullopt,
            context);

        return columns;
    }

    return parseColumnsListFromString(structure, context);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>({});
}

}
