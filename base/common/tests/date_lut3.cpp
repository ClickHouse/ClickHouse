#include <iostream>
#include <cstring>

#include <Poco/Exception.h>

#include <common/DateLUT.h>


static std::string toString(time_t Value)
{
    struct tm tm;
    char buf[96];

    localtime_r(&Value, &tm);
    snprintf(buf, sizeof(buf), "%04d-%02d-%02d %02d:%02d:%02d",
             tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);

    return buf;
}

static time_t orderedIdentifierToDate(unsigned value)
{
    struct tm tm;

    memset(&tm, 0, sizeof(tm));

    tm.tm_year = value / 10000 - 1900;
    tm.tm_mon = (value % 10000) / 100 - 1;
    tm.tm_mday = value % 100;
    tm.tm_isdst = -1;

    return mktime(&tm);
}


void loop(time_t begin, time_t end, int step)
{
    const auto & date_lut = DateLUT::instance();

    for (time_t t = begin; t < end; t += step)
    {
        time_t t2 = date_lut.makeDateTime(date_lut.toYear(t), date_lut.toMonth(t), date_lut.toDayOfMonth(t),
            date_lut.toHour(t), date_lut.toMinute(t), date_lut.toSecond(t));

        std::string s1 = toString(t);
        std::string s2 = toString(t2);

        std::cerr << s1 << ", " << s2 << std::endl;

        if (s1 != s2)
            throw Poco::Exception("Test failed.");
    }
}


int main(int, char **)
{
    loop(orderedIdentifierToDate(20101031), orderedIdentifierToDate(20101101), 15 * 60);
    loop(orderedIdentifierToDate(20100328), orderedIdentifierToDate(20100330), 15 * 60);

    return 0;
}
