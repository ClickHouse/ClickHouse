#pragma once

#include <IO/Progress.h>
#include <Formats/JSONEachRowRowOutputStream.h>


namespace DB
{

/** The stream for outputting data in JSON format, by object per line
  * that includes progress rows. Does not validate UTF-8.
  */
class JSONEachRowWithProgressRowOutputStream : public JSONEachRowRowOutputStream
{
public:
    using JSONEachRowRowOutputStream::JSONEachRowRowOutputStream;

    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;
    void onProgress(const Progress & value) override;

private:
    Progress progress;
};

}

