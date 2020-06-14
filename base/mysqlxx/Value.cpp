#include <mysqlxx/Value.h>
#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Query.h>
#include <mysqlxx/Exception.h>


namespace mysqlxx
{

UInt64 Value::readUIntText(const char * buf, size_t length) const
{
    UInt64 x = 0;
    const char * end = buf + length;

    while (buf != end)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                x *= 10;
                x += *buf - '0';
                break;
            default:
                throwException("Cannot parse unsigned integer");
        }
        ++buf;
    }

    return x;
}


Int64 Value::readIntText(const char * buf, size_t length) const
{
    bool negative = false;
    UInt64 x = 0;
    const char * end = buf + length;

    while (buf != end)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                x *= 10;
                x += *buf - '0';
                break;
            default:
                throwException("Cannot parse signed integer");
        }
        ++buf;
    }

    return negative ? -x : x;
}


double Value::readFloatText(const char * buf, size_t length) const
{
    bool negative = false;
    double x = 0;
    bool after_point = false;
    double power_of_ten = 1;
    const char * end = buf + length;

    while (buf != end)
    {
        switch (*buf)
        {
            case '+':
                break;
            case '-':
                negative = true;
                break;
            case '.':
                after_point = true;
                break;
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
                if (after_point)
                {
                    power_of_ten /= 10;
                    x += (*buf - '0') * power_of_ten;
                }
                else
                {
                    x *= 10;
                    x += *buf - '0';
                }
                break;
            case 'e':
            case 'E':
            {
                ++buf;
                Int32 exponent = readIntText(buf, end - buf);
                x *= preciseExp10(exponent);
                if (negative)
                    x = -x;
                return x;
            }
            case 'i':
            case 'I':
                x = std::numeric_limits<double>::infinity();
                if (negative)
                    x = -x;
                return x;
            case 'n':
            case 'N':
                x = std::numeric_limits<double>::quiet_NaN();
                return x;
            default:
                throwException("Cannot parse floating point number");
        }
        ++buf;
    }
    if (negative)
        x = -x;

    return x;
}


void Value::throwException(const char * text) const
{
    static constexpr size_t preview_length = 1000;

    std::stringstream info;
    info << text;

    if (!isNull())
    {
        info << ": ";
        info.write(m_data, m_length);
    }

    if (res && res->getQuery())
        info << ", query: " << res->getQuery()->str().substr(0, preview_length);

    throw CannotParseValue(info.str());
}

}
