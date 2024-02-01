#pragma once

#include <vector>
#include <IO/WriteBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Processors/Formats/IInputFormatErrorsHandler.h>


namespace DB
{

class InputFormatErrorsLogger : public IInputFormatErrorsHandler
{
public:
    explicit InputFormatErrorsLogger(const ContextPtr & context);
    ~InputFormatErrorsLogger() override;
    void logError(ErrorEntry entry) override;

protected:
    void logErrorImpl(ErrorEntry entry);

private:
    Block header;

    String errors_file_path;
    std::shared_ptr<WriteBufferFromFile> write_buf;
    OutputFormatPtr writer;

    String database;
    String table;

    MutableColumns errors_columns;
    size_t max_block_size;

    void writeErrors();
};

class ParallelInputFormatErrorsLogger : public InputFormatErrorsLogger
{
public:
    explicit ParallelInputFormatErrorsLogger(const ContextPtr & context)
        : InputFormatErrorsLogger(context)
    {}
    ~ParallelInputFormatErrorsLogger() override;
    void logError(ErrorEntry entry) override;

private:
    std::mutex write_mutex;
};

}
