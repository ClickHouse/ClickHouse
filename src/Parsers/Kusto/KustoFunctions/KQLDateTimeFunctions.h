#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/Kusto/KustoFunctions/IParserKQLFunction.h>
namespace DB
{

class TimeSpan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "timespan()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "datetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Ago : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "ago()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatetimeAdd : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "datetime_add()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatetimePart : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "datetime_part()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DatetimeDiff : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "datetime_diff()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DayOfMonth : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "dayofmonth()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DayOfWeek : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "dayofweek()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class DayOfYear : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "dayofyear()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class EndOfDay : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "endofday()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class EndOfWeek : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "endofweek()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class EndOfYear : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "endofyear()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class FormatDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "format_datetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class FormatTimeSpan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "format_timespan()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class GetMonth : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "getmonth()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class GetYear : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "getyear()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class HoursOfDay : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "hoursofday()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class MakeTimeSpan : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_timespan()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class MakeDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "make_datetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class Now : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "now()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class StartOfDay : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "startofday()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class StartOfMonth : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "startofmonth()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class StartOfWeek : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "startofweek()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class StartOfYear : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "startofyear()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class UnixTimeMicrosecondsToDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "unixtime_microseconds_todatetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class UnixTimeMillisecondsToDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "unixtime_milliseconds_todatetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class UnixTimeNanosecondsToDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "unixtime_nanoseconds_todatetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class UnixTimeSecondsToDateTime : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "unixtime_seconds_todatetime()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

class WeekOfYear : public IParserKQLFunction
{
protected:
    const char * getName() const override { return "weekofyear()"; }
    bool convertImpl(String &out,IParser::Pos &pos) override;
};

}

