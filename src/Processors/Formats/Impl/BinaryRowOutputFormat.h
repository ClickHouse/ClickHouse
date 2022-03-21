#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Core/Block.h>


namespace DB
{

class IColumn;
class IDataType;
class WriteBuffer;


/** A stream for outputting data in a binary line-by-line format.
  */
class BinaryRowOutputFormat final: public IRowOutputFormat
{
public:
    BinaryRowOutputFormat(WriteBuffer & out_, const Block & header, bool with_names_, bool with_types_, const RowOutputFormatParams & params_);

    String getName() const override { return "BinaryRowOutputFormat"; }

    String getContentType() const override { return "application/octet-stream"; }

private:
    void writeField(const IColumn & column, const ISerialization & serialization, size_t row_num) override;
    void writePrefix() override;

    bool with_names;
    bool with_types;
};

}
