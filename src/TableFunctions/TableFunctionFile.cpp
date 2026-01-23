#include <Interpreters/parseColumnsListForTableFunction.h>
#include <TableFunctions/ITableFunctionFileLike.h>
#include <TableFunctions/TableFunctionFile.h>

#include <TableFunctions/registerTableFunctions.h>
#include <Access/Common/AccessFlags.h>
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

void TableFunctionFile::parseFirstArguments(const ASTPtr & arg, const ContextPtr & context)
{
    if (context->getApplicationType() != Context::ApplicationType::LOCAL)
    {
        ITableFunctionFileLike::parseFirstArguments(arg, context);
        file_source = StorageFile::FileSource::parse(filename, context);
        return;
    }

    const auto * literal = arg->as<ASTLiteral>();
    auto type = literal->value.getType();
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of table function '{}' mush be path or file descriptor", getName());
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
    factory.registerFunction<TableFunctionFile>();
}

}
