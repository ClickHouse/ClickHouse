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
class BinaryRowOutputFormat: public IRowOutputFormat
{
public:
    BinaryRowOutputFormat(WriteBuffer & out_, const Block & header, bool with_names_, bool with_types_, FormatFactory::WriteCallback callback);

    String getName() const override { return "BinaryRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType & type, size_t row_num) override;
    void writePrefix() override;

    String getContentType() const override { return "application/octet-stream"; }

protected:
    bool with_names;
    bool with_types;
};

}
