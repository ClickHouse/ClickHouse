#include <Processors/Formats/InputFormatErrorsLogger.h>
#include <Processors/Formats/IRowOutputFormat.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/filesystemHelpers.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>


namespace DB
{
namespace Setting
{
    extern const SettingsString errors_output_format;
    extern const SettingsString input_format_record_errors_file_path;
    extern const SettingsUInt64 max_block_size;
}

namespace ErrorCodes
{
    extern const int DATABASE_ACCESS_DENIED;
}

namespace
{
    const String DEFAULT_OUTPUT_FORMAT = "CSV";
}

InputFormatErrorsLogger::InputFormatErrorsLogger(const ContextPtr & context) : max_block_size(context->getSettingsRef()[Setting::max_block_size])
{
    String output_format = context->getSettingsRef()[Setting::errors_output_format];
    if (!FormatFactory::instance().isOutputFormat(output_format))
        output_format = DEFAULT_OUTPUT_FORMAT;
    if (context->hasInsertionTable())
        table = context->getInsertionTable().getTableName();
    if (context->getInsertionTable().hasDatabase())
        database = context->getInsertionTable().getDatabaseName();

    String path_in_setting = context->getSettingsRef()[Setting::input_format_record_errors_file_path];

    if (context->getApplicationType() == Context::ApplicationType::SERVER)
    {
        auto user_files_path = context->getUserFilesPath();
        errors_file_path = fs::path(user_files_path) / path_in_setting;
        if (!fileOrSymlinkPathStartsWith(errors_file_path, user_files_path))
            throw Exception(ErrorCodes::DATABASE_ACCESS_DENIED,
                            "Cannot log errors in path `{}`, because it is not inside `{}`",
                            errors_file_path, user_files_path);
    }
    else
    {
        errors_file_path = path_in_setting;
    }

    while (fs::exists(errors_file_path))
    {
        errors_file_path += "_new";
    }
    write_buf = std::make_shared<WriteBufferFromFile>(errors_file_path);

    header = Block{
        {std::make_shared<DataTypeDateTime>(), "time"},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "database"},
        {std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "table"},
        {std::make_shared<DataTypeUInt32>(), "offset"},
        {std::make_shared<DataTypeString>(), "reason"},
        {std::make_shared<DataTypeString>(), "raw_data"}};
    errors_columns = header.cloneEmptyColumns();

    writer = context->getOutputFormat(output_format, *write_buf, header);
}


InputFormatErrorsLogger::~InputFormatErrorsLogger()
{
    try
    {
        if (!errors_columns[0]->empty())
            writeErrors();
        writer->finalize();
        writer->flush();
        write_buf->finalize();
    }
    catch (...)
    {
        tryLogCurrentException("InputFormatErrorsLogger");
    }
}

void InputFormatErrorsLogger::logErrorImpl(ErrorEntry entry)
{
    errors_columns[0]->insert(entry.time);
    database.empty() ? errors_columns[1]->insertDefault() : errors_columns[1]->insert(database);
    table.empty() ? errors_columns[2]->insertDefault() : errors_columns[2]->insert(table);
    errors_columns[3]->insert(entry.offset);
    errors_columns[4]->insert(entry.reason);
    errors_columns[5]->insert(entry.raw_data);

    if (errors_columns[0]->size() >= max_block_size)
        writeErrors();
}

void InputFormatErrorsLogger::writeErrors()
{
    auto block = header.cloneEmpty();
    block.setColumns(std::move(errors_columns));
    writer->write(block);
    errors_columns = header.cloneEmptyColumns();
}

void InputFormatErrorsLogger::logError(ErrorEntry entry)
{
    logErrorImpl(entry);
}

ParallelInputFormatErrorsLogger::~ParallelInputFormatErrorsLogger() = default;

void ParallelInputFormatErrorsLogger::logError(ErrorEntry entry)
{
    std::lock_guard lock(write_mutex);
    logErrorImpl(entry);
}

}
