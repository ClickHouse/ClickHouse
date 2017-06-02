#include <DataStreams/verbosePrintString.h>
#include <Common/hex.h>
#include <IO/Operators.h>


namespace DB
{

void verbosePrintString(const char * begin, const char * end, WriteBuffer & out)
{
    if (end == begin)
    {
        out << "<EMPTY>";
        return;
    }

    out << "\"";

    for (auto pos = begin; pos < end; ++pos)
    {
        switch (*pos)
        {
            case '\0':
                out << "<ASCII NUL>";
                break;
            case '\b':
                out << "<BACKSPACE>";
                break;
            case '\f':
                out << "<FORM FEED>";
                break;
            case '\n':
                out << "<LINE FEED>";
                break;
            case '\r':
                out << "<CARRIAGE RETURN>";
                break;
            case '\t':
                out << "<TAB>";
                break;
            case '\\':
                out << "<BACKSLASH>";
                break;
            case '"':
                out << "<DOUBLE QUOTE>";
                break;
            case '\'':
                out << "<SINGLE QUOTE>";
                break;

            default:
            {
                if (*pos >= 0 && *pos < 32)
                    out << "<0x" << hexUppercase(*pos / 16) << hexUppercase(*pos % 16) << ">";
                else
                    out << *pos;
            }
        }
    }

    out << "\"";
}

}
