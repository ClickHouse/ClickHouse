#include <IO/WriteHelpers.h>
#include <IO/WriteBufferValidUTF8.h>
#include <Formats/JSONEachRowWithProgressRowOutputStream.h>
#include <Formats/FormatFactory.h>
#include <Formats/BlockOutputStreamFromRowOutputStream.h>


namespace DB
{


void JSONEachRowWithProgressRowOutputStream::writeRowStartDelimiter()
{
    writeCString("{\"row\":{", ostr);
}


void JSONEachRowWithProgressRowOutputStream::writeRowEndDelimiter()
{
    writeCString("}}\n", ostr);
    field_number = 0;
}


void JSONEachRowWithProgressRowOutputStream::onProgress(const Progress & value)
{
    progress.incrementPiecewiseAtomically(value);
    writeCString("{\"progress\":", ostr);
    progress.writeJSON(ostr);
    writeCString("}\n", ostr);
}


void registerOutputFormatJSONEachRowWithProgress(FormatFactory & factory)
{
    factory.registerOutputFormat("JSONEachRowWithProgress", [](
        WriteBuffer & buf,
        const Block & sample,
        const Context &,
        const FormatSettings & format_settings)
    {
        return std::make_shared<BlockOutputStreamFromRowOutputStream>(
            std::make_shared<JSONEachRowWithProgressRowOutputStream>(buf, sample, format_settings), sample);
    });
}

}
