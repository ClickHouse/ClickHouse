#include <TableFunctions/TableFunctionFile.h>
#include <Interpreters/parseColumnsListForTableFunction.h>

#include "Parsers/IAST_fwd.h"
#include "registerTableFunctions.h"
#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/StorageFile.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Formats/FormatFactory.h>
#include <Parsers/ASTIdentifier_fwd.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void TableFunctionFile::parseFirstArguments(const ASTPtr & arg, const ContextPtr & context)
{
    if (context->getApplicationType() != Context::ApplicationType::LOCAL)
    {
        ITableFunctionFileLike::parseFirstArguments(arg, context);
        StorageFile::parseFileSource(std::move(filename), filename, path_to_archive);
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
            StorageFile::parseFileSource(std::move(filename), filename, path_to_archive);
    }
    else if (type == Field::Types::Int64 || type == Field::Types::UInt64)
    {
        fd = static_cast<int>(
            (type == Field::Types::Int64) ? literal->value.get<Int64>() : literal->value.get<UInt64>());
        if (fd < 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "File descriptor must be non-negative");
    }
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The first argument of table function '{}' mush be path or file descriptor", getName());
}

String TableFunctionFile::getFormatFromFirstArgument()
{
    if (fd >= 0)
        return FormatFactory::instance().getFormatFromFileDescriptor(fd);
    else
        return FormatFactory::instance().getFormatFromFileName(filename, true);
}

StoragePtr TableFunctionFile::getStorage(const String & source,
    const String & format_, const ColumnsDescription & columns,
    ContextPtr global_context, const std::string & table_name,
    const std::string & compression_method_) const
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
        global_context->getSettingsRef().rename_files_after_processing,
        path_to_archive,
    };

    if (fd >= 0)
        return std::make_shared<StorageFile>(fd, args);

    return std::make_shared<StorageFile>(source, global_context->getUserFilesPath(), args);
}

ColumnsDescription TableFunctionFile::getActualTableStructure(ContextPtr context, bool /*is_insert_query*/) const
{
    if (structure == "auto")
    {
        if (fd >= 0)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Schema inference is not supported for table function '{}' with file descriptor", getName());
        size_t total_bytes_to_read = 0;

        Strings paths;
        std::optional<StorageFile::ArchiveInfo> archive_info;
        if (path_to_archive.empty())
            paths = StorageFile::getPathsList(filename, context->getUserFilesPath(), context, total_bytes_to_read);
        else
            archive_info
                = StorageFile::getArchiveInfo(path_to_archive, filename, context->getUserFilesPath(), context, total_bytes_to_read);

        return StorageFile::getTableStructureFromFile(format, paths, compression_method, std::nullopt, context, archive_info);
    }


    return parseColumnsListFromString(structure, context);
}

void registerTableFunctionFile(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionFile>();
}

}
