#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/WriteBufferFromString.h>


namespace DB
{
String quoteString(std::string_view x)
{
    String res(x.size(), '\0');
    WriteBufferFromString wb(res);
    writeQuotedString(x, wb);
    return res;
}


String doubleQuoteString(StringRef x)
{
    String res(x.size, '\0');
    WriteBufferFromString wb(res);
    writeDoubleQuotedString(x, wb);
    return res;
}


String backQuote(StringRef x)
{
    String res(x.size, '\0');
    {
        WriteBufferFromString wb(res);
        writeBackQuotedString(x, wb);
    }
    return res;
}


String backQuoteIfNeed(StringRef x)
{
    String res(x.size, '\0');
    {
        WriteBufferFromString wb(res);
        writeProbablyBackQuotedString(x, wb);
    }
    return res;
}

}
