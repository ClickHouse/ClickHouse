#pragma once
#include <Processors/Formats/Impl/JSONEachRowRowOutputFormat.h>

namespace DB
{

class JSONEachRowWithProgressRowOutputFormat : public JSONEachRowRowOutputFormat
{
public:
    using JSONEachRowRowOutputFormat::JSONEachRowRowOutputFormat;

    void onProgress(const Progress & value) override;

private:
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    Progress progress;
};

}
