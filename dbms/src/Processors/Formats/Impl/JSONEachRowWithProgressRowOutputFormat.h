#pragma once
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void onProgress(const Progress & value) override;

private:
    Progress progress;
};

}
