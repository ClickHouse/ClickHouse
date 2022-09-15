#include <Processors/Formats/InputFormatErrorsLogger.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

namespace
{
    const String DEFAULT_OUTPUT_FORMAT = "CSV";
}

InputFormatErrorsLogger::InputFormatErrorsLogger(const ContextPtr & context)
{
    String output_format = context->getSettingsRef().errors_output_format;
    if (!FormatFactory::instance().isOutputFormat(output_format))
        output_format = DEFAULT_OUTPUT_FORMAT;
    if (context->hasInsertionTable())
        table = context->getInsertionTable().getTableName();
    if (context->getInsertionTable().hasDatabase())
        database = context->getInsertionTable().getDatabaseName();

    String path_in_setting = context->getSettingsRef().input_format_record_errors_file_path;
    errors_file_path = context->getApplicationType() == Context::ApplicationType::SERVER ? context->getUserFilesPath() + path_in_setting
                                                                                         : path_in_setting;
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

    writer = context->getOutputFormat(output_format, *write_buf, header);
}

InputFormatErrorsLogger::~InputFormatErrorsLogger()
{
    writer->finalize();
    writer->flush();
    write_buf->finalize();
}

void InputFormatErrorsLogger::logErrorImpl(ErrorEntry entry)
{
    auto error = header.cloneEmpty();
    auto columns = error.mutateColumns();
    columns[0]->insert(entry.time);
    database.empty() ? columns[1]->insertDefault() : columns[1]->insert(database);
    table.empty() ? columns[2]->insertDefault() : columns[2]->insert(table);
    columns[3]->insert(entry.offset);
    columns[4]->insert(entry.reason);
    columns[5]->insert(entry.raw_data);
    error.setColumns(std::move(columns));

    writer->write(error);
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
