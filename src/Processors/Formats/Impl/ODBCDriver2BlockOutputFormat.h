#pragma once

#include <string>
#include <Core/Block.h>
#include <Processors/Formats/IOutputFormat.h>
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
class ODBCDriver2BlockOutputFormat final : public IOutputFormat
{
public:
    ODBCDriver2BlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ODBCDriver2BlockOutputFormat"; }

    void consume(Chunk) override;
    void consumeTotals(Chunk) override;
    void finalize() override;

    std::string getContentType() const override
    {
        return "application/octet-stream";
    }

private:
    const FormatSettings format_settings;
    bool prefix_written = false;

    void writePrefixIfNot()
    {
        if (!prefix_written)
            writePrefix();

        prefix_written = true;
    }

    void writeRow(const Block & header, const Columns & columns, size_t row_idx, std::string & buffer);
    void write(Chunk chunk, PortKind port_kind);
    void writePrefix();
};


}
