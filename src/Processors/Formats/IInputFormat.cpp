#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/Impl/CSVRowOutputFormat.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>


namespace DB
{

InputFormatErrorsLogger::InputFormatErrorsLogger(
    Context::ApplicationType app_type,
    const String & user_files_path,
    String & path_in_setting,
    bool is_changed,
    FormatSettings::ErrorsOutputFormat output_format,
    const String & database_,
    const String & table_)
    : database(database_), table(table_)
{
    if (app_type == Context::ApplicationType::SERVER)
    {
        trimLeft(path_in_setting, '/');
    }
    else if (!is_changed)
    {
        path_in_setting = "/tmp/" + path_in_setting;
    }
    errors_file_path = user_files_path + path_in_setting;
    if (is_changed)
    {
        while (fs::exists(errors_file_path))
        {
            errors_file_path += "_new";
        }
    }
    write_buf = std::make_shared<WriteBufferFromFile>(errors_file_path);

    Block header{
        {std::make_shared<DataTypeString>(), "time"},
        {std::make_shared<DataTypeString>(), "database"},
        {std::make_shared<DataTypeString>(), "table"},
        {std::make_shared<DataTypeUInt32>(), "offset"},
        {std::make_shared<DataTypeString>(), "reason"},
        {std::make_shared<DataTypeString>(), "raw_data"}};
    FormatSettings format_settings;
    RowOutputFormatParams out_params;

    if (output_format == FormatSettings::ErrorsOutputFormat::CSV)
        writer = std::make_shared<CSVRowOutputFormat>(*write_buf, header, false, false, out_params, format_settings);
}

void InputFormatErrorsLogger::logErrorImpl(ErrorEntry entry)
{
    for (auto & ch : entry.reason)
    {
        if (ch == '\"')
            ch = '\'';
    }
    for (auto & ch : entry.raw_data)
    {
        if (ch == '\"')
            ch = '\'';
    }
    Block error{
        {DataTypeString().createColumnConst(1, entry.time)->convertToFullColumnIfConst(), std::make_shared<DataTypeString>(), "time"},
        {DataTypeString().createColumnConst(1, database)->convertToFullColumnIfConst(), std::make_shared<DataTypeString>(), "database"},
        {DataTypeString().createColumnConst(1, table)->convertToFullColumnIfConst(), std::make_shared<DataTypeString>(), "table"},
        {DataTypeUInt32().createColumnConst(1, entry.offset)->convertToFullColumnIfConst(), std::make_shared<DataTypeUInt32>(), "offset"},
        {DataTypeString().createColumnConst(1, entry.reason)->convertToFullColumnIfConst(), std::make_shared<DataTypeString>(), "reason"},
        {DataTypeString().createColumnConst(1, entry.raw_data)->convertToFullColumnIfConst(),
         std::make_shared<DataTypeString>(),
         "raw_data"}};
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

IInputFormat::IInputFormat(Block header, ReadBuffer & in_)
    : ISource(std::move(header)), in(&in_)
{
    column_mapping = std::make_shared<ColumnMapping>();
}

void IInputFormat::resetParser()
{
    in->ignoreAll();
    // those are protected attributes from ISource (I didn't want to propagate resetParser up there)
    finished = false;
    got_exception = false;

    getPort().getInputPort().reopen();
}

void IInputFormat::setReadBuffer(ReadBuffer & in_)
{
    in = &in_;
}

}
