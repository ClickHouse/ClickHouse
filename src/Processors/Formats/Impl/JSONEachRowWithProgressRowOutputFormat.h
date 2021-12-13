#pragma once
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <mutex>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeSuffix() override;
    void onProgress(const Progress & value) override;
    void flush() override;

    void writeProgress();

private:
    Progress progress;
    std::vector<String> progress_lines;
    std::mutex progress_lines_mutex;
    /// To not lock mutex and check progress_lines every row,
    /// we will use atomic flag that progress_lines is not empty.
    std::atomic_bool has_progress = false;
};

}
