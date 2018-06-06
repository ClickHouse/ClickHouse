#pragma once

#include <string>
#include <DataStreams/IBlockOutputStream.h>
#include <Core/Block.h>

namespace DB
{

class WriteBuffer;


/** A data format designed to simplify the implementation of the ODBC driver.
  * ODBC driver is designed to be build for different platforms without dependencies from the main code,
  *  so the format is made that way so that it can be as easy as possible to parse it.
  * A header is displayed with the required information.
  * The data is then output in the order of the rows. Each value is displayed as follows: length in VarUInt format, then data in text form.
  */
class ODBCDriverBlockOutputStream : public IBlockOutputStream
{
public:
    ODBCDriverBlockOutputStream(WriteBuffer & out_, const Block & header_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;
    void writePrefix() override;

    void flush() override;
    std::string getContentType() const override { return "application/octet-stream"; }

private:
    WriteBuffer & out;
    const Block header;
};

}
