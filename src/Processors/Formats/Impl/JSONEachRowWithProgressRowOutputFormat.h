#pragma once

#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>
#include <mutex>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat final : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

    bool writesProgressConcurrently() const override { return true; }
    void writeProgress(const Progress & value) override;

private:
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
};

}
