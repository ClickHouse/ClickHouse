#pragma once

#include <string>
#include <Processors/Formats/IOutputFormat.h>
#include <Formats/FormatSettings.h>
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
class ODBCDriverBlockOutputFormat : public IOutputFormat
{
public:
    ODBCDriverBlockOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & format_settings_);

    String getName() const override { return "ODBCDriverBlockOutputFormat"; }

    void consume(Chunk) override;
    void finalize() override;

    std::string getContentType() const override { return "application/octet-stream"; }

private:
    const FormatSettings format_settings;
    bool prefix_written = false;

    void writePrefixIfNot()
    {
        if (!prefix_written)
            writePrefix();

        prefix_written = true;
    }

    void writePrefix();
};

}
