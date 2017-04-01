#include <DataStreams/verbosePrintString.h>
#include <IO/Operators.h>


namespace DB
{

void verbosePrintString(BufferBase::Position begin, BufferBase::Position end, WriteBuffer & out)
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
                {
                    static const char * hex = "0123456789ABCDEF";
                    out << "<0x" << hex[*pos / 16] << hex[*pos % 16] << ">";
                }
                else
                    out << *pos;
            }
        }
    }

    out << "\"";
}

}
