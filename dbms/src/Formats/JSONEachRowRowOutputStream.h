#pragma once

#include <Core/Block.h>
#include <IO/WriteBuffer.h>
#include <Formats/IRowOutputStream.h>
#include <Formats/FormatSettings.h>


namespace DB
{

/** The stream for outputting data in JSON format, by object per line.
  * Does not validate UTF-8.
  */
class JSONEachRowRowOutputStream : public IRowOutputStream
{
public:
    JSONEachRowRowOutputStream(WriteBuffer & ostr_, const Block & sample, const FormatSettings & settings);

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writeFieldDelimiter() override;
    void writeRowStartDelimiter() override;
    void writeRowEndDelimiter() override;

    void flush() override
    {
        ostr.next();
    }

private:
    WriteBuffer & ostr;
    size_t field_number = 0;
    Names fields;

    FormatSettings settings;
};

}

