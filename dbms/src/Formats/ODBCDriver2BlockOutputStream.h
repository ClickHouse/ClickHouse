#pragma once

#include <string>
#include <Core/Block.h>
#include <DataStreams/IBlockOutputStream.h>
#include <Formats/FormatSettings.h>


namespace DB
{
class WriteBuffer;


/** A data format designed to simplify the implementation of the ODBC driver.
  * ODBC driver is designed to be build for different platforms without dependencies from the main code,
  *  so the format is made that way so that it can be as easy as possible to parse it.
  * A header is displayed with the required information.
  * The data is then output in the order of the rows. Each value is displayed as follows: length in Int32 format (-1 for NULL), then data in text form.
  */
class ODBCDriver2BlockOutputStream final : public IBlockOutputStream
{
public:
    ODBCDriver2BlockOutputStream(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings);

    Block getHeader() const override
    {
        return header;
    }
    void write(const Block & block) override;
    void writePrefix() override;
    void writeSuffix() override;

    void flush() override;
    std::string getContentType() const override
    {
        return "application/octet-stream";
    }
    void setTotals(const Block & totals_) override { totals = totals_; }

private:
    WriteBuffer & out;
    const Block header;
    const FormatSettings format_settings;

protected:
    Block totals;
};



}
