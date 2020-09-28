#pragma once

#include <Core/Block.h>
#include <Processors/Formats/IRowOutputFormat.h>


namespace DB
{

class WriteBuffer;


/** This format only allows to output one column of type String or similar.
  * It is output as raw bytes without any delimiters or escaping.
  *
  * The difference between RawBLOB and TSVRaw:
  * - only single column dataset is supported;
  * - data is output in binary;
  * - no newline at the end of each value.
  *
  * The difference between RawBLOB and RowBinary:
  * - only single column dataset is supported;
  * - strings are output without their lengths.
  *
  * If you are output more than one value, the output format is ambiguous and you may not be able to read data back.
  */
class RawBLOBRowOutputFormat : public IRowOutputFormat
{
public:
    RawBLOBRowOutputFormat(
        WriteBuffer & out_,
        const Block & header_,
        FormatFactory::WriteCallback callback);

    String getName() const override { return "RawBLOBRowOutputFormat"; }

    void writeField(const IColumn & column, const IDataType &, size_t row_num) override;
};

}

