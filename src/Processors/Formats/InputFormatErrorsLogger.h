#pragma once

#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>


namespace DB
{

class InputFormatErrorsLogger
{
public:
    struct ErrorEntry
    {
        time_t time;
        size_t offset;
        String reason;
        String raw_data;
    };

    InputFormatErrorsLogger(const ContextPtr & context);

    virtual ~InputFormatErrorsLogger();

    virtual void logError(ErrorEntry entry);
    void logErrorImpl(ErrorEntry entry);

private:
    Block header;

    String errors_file_path;
    std::shared_ptr<WriteBufferFromFile> write_buf;
    OutputFormatPtr writer;

    String database;
    String table;
};

using InputFormatErrorsLoggerPtr = std::shared_ptr<InputFormatErrorsLogger>;

class ParallelInputFormatErrorsLogger : public InputFormatErrorsLogger
{
public:
    ParallelInputFormatErrorsLogger(const ContextPtr & context) : InputFormatErrorsLogger(context) { }

    ~ParallelInputFormatErrorsLogger() override;

    void logError(ErrorEntry entry) override;

private:
    std::mutex write_mutex;
};

}
