#pragma once
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <mutex>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat final : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

    void onProgress(const Progress & value) override;
    void flush() override;

private:
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void writeSuffix() override;

    void writeProgress();

    Progress progress;
    std::mutex progress_mutex;
    std::unique_lock<std::mutex> progress_lock{progress_mutex, std::defer_lock};
    std::atomic_bool has_progress = false;
};

}
