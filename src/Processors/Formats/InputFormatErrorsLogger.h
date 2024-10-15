#pragma once

#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>

#include <Core/Block.h>


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

    explicit InputFormatErrorsLogger(const ContextPtr & context);

    virtual ~InputFormatErrorsLogger();

    virtual void logError(ErrorEntry entry);
    void logErrorImpl(ErrorEntry entry);
    void writeErrors();

private:
    Block header;

    String errors_file_path;
    std::shared_ptr<WriteBufferFromFile> write_buf;
    OutputFormatPtr writer;

    String database;
    String table;

    MutableColumns errors_columns;
    size_t max_block_size;
};

using InputFormatErrorsLoggerPtr = std::shared_ptr<InputFormatErrorsLogger>;

class ParallelInputFormatErrorsLogger : public InputFormatErrorsLogger
{
public:
    explicit ParallelInputFormatErrorsLogger(const ContextPtr & context) : InputFormatErrorsLogger(context) { }

    ~ParallelInputFormatErrorsLogger() override;

    void logError(ErrorEntry entry) override;

private:
    std::mutex write_mutex;
};

}
