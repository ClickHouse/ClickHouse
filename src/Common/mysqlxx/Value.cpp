#include <mysqlxx/Value.h>
#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Query.h>
#include <mysqlxx/Exception.h>

#include <base/preciseExp10.h>

#include <Common/DateLUTImpl.h>


namespace mysqlxx
{

time_t Value::getDateTimeImpl() const
{
    const auto & date_lut = DateLUT::instance();

    if (m_length == 10)
    {
        return date_lut.makeDate(
            (m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
            (m_data[5] - '0') * 10 + (m_data[6] - '0'),
            (m_data[8] - '0') * 10 + (m_data[9] - '0'));
    }
    if (m_length == 19)
    {
        return date_lut.makeDateTime(
            (m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
            (m_data[5] - '0') * 10 + (m_data[6] - '0'),
            (m_data[8] - '0') * 10 + (m_data[9] - '0'),
            (m_data[11] - '0') * 10 + (m_data[12] - '0'),
            (m_data[14] - '0') * 10 + (m_data[15] - '0'),
            (m_data[17] - '0') * 10 + (m_data[18] - '0'));
    }
    throwException("Cannot parse DateTime");
}


time_t Value::getDateImpl() const
{
    const auto & date_lut = DateLUT::instance();

    if (m_length == 10 || m_length == 19)
    {
        return date_lut.makeDate(
            (m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
            (m_data[5] - '0') * 10 + (m_data[6] - '0'),
            (m_data[8] - '0') * 10 + (m_data[9] - '0'));
    }
    throwException("Cannot parse Date");
}

Int64 Value::getIntOrDate() const
{
    if (unlikely(isNull()))
        throwException("Value is NULL");

    if (checkDateTime())
        return getDateImpl();

    const auto & date_lut = DateLUT::instance();
    return date_lut.toDate(getIntImpl());
}

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
                Int32 exponent = static_cast<Int32>(readIntText(buf, end - buf));
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

    std::string info(text);

    if (!isNull())
    {
        info.append(": '");
        info.append(m_data, m_length);
        info.append("'");
    }

    if (res && res->getQuery())
    {
        info.append(", query: '");
        info.append(res->getQuery()->str().substr(0, preview_length));
        info.append("'");
    }

    throw CannotParseValue(info);
}

}
