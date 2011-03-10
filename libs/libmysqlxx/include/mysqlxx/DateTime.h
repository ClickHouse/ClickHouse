#ifndef MYSQLXX_DATETIME_H
#define MYSQLXX_DATETIME_H

#include <string>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Exception.h>


namespace mysqlxx
{

class DateTime
{
private:
	unsigned short m_year;
	unsigned char m_month;
	unsigned char m_day;
	unsigned char m_hour;
	unsigned char m_minute;
	unsigned char m_second;
	
	void init(time_t time)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		const Yandex::DateLUT::Values & values = date_lut.getValues(time);

		m_year = values.year;
		m_month = values.month;
		m_day = values.day_of_month;
		m_hour = date_lut.toHourInaccurate(time);
		m_minute = date_lut.toMinute(time);
		m_second = date_lut.toSecond(time);
	}

	void init(const char * s, size_t length)
	{
		if (length < 19)
			throw Exception("Cannot parse DateTime: " + std::string(s, length));

		m_year = s[0] * 1000 + s[1] * 100 + s[2] * 10 + s[3];
		m_month = s[5] * 10 + s[6];
		m_day = s[8] * 10 + s[9];

		m_hour = s[11] * 10 + s[12];
		m_minute = s[14] * 10 + s[15];
		m_second = s[17] * 10 + s[18];
	}

public:
	DateTime(time_t time)
	{
		init(time);
	}

	DateTime(unsigned short year_, unsigned char month_, unsigned char day_,
		unsigned char hour_, unsigned char minute_, unsigned char second_)
		: m_year(year_), m_month(month_), m_day(day_), m_hour(hour_), m_minute(minute_), m_second(second_)
	{
	}

	DateTime(const std::string & s)
	{
		if (s.size() < 19)
			throw Exception("Cannot parse DateTime: " + s);

		m_year = s[0] * 1000 + s[1] * 100 + s[2] * 10 + s[3];
		m_month = s[5] * 10 + s[6];
		m_day = s[8] * 10 + s[9];

		m_hour = s[11] * 10 + s[12];
		m_minute = s[14] * 10 + s[15];
		m_second = s[17] * 10 + s[18];
	}

	DateTime()
	{
		init(time(0));
	}

	DateTime(const char * data, size_t length)
	{
		init(data, length);
	}

	operator time_t() const
	{
		return Yandex::DateLUTSingleton::instance().makeDateTime(m_year, m_month, m_day, m_hour, m_minute, m_second);
	}

	unsigned short year() const 	{ return m_year; }
	unsigned char month() const 	{ return m_month; }
	unsigned char day() const 		{ return m_day; }
	unsigned char hour() const 		{ return m_hour; }
	unsigned char minute() const 	{ return m_minute; }
	unsigned char second() const 	{ return m_second; }

	void year(unsigned short x) 	{ m_year = x; }
	void month(unsigned char x) 	{ m_month = x; }
	void day(unsigned char x) 		{ m_day = x; }
	void hour(unsigned char x) 		{ m_hour = x; }
	void minute(unsigned char x) 	{ m_minute = x; }
	void second(unsigned char x) 	{ m_second = x; }

	bool operator< (const DateTime & other) const
	{
		if (m_year < other.m_year)
			return true;
		else if (m_month < other.m_month)
			return true;
		else if (m_day < other.m_day)
			return true;
		else if (m_hour < other.m_hour)
			return true;
		else if (m_minute < other.m_minute)
			return true;
		else if (m_second < other.m_second)
			return true;
		else
			return false;
	}

	bool operator== (const DateTime & other) const
	{
		return m_year == other.m_year && m_month == other.m_month && m_day == other.m_day
			&& m_hour == other.m_hour && m_minute == other.m_minute && m_second == other.m_second;
	}

	bool operator!= (const DateTime & other) const
	{
		return !(*this == other);
	}
};

inline std::ostream & operator<< (std::ostream & ostr, const DateTime & datetime)
{
	return ostr << datetime.year()
		<< '-' << (datetime.month() / 10) 	<< (datetime.month() % 10)
		<< '-' << (datetime.day() / 10) 	<< (datetime.day() % 10)
		<< ' ' << (datetime.hour() / 10) 	<< (datetime.hour() % 10)
		<< ':' << (datetime.minute() / 10) 	<< (datetime.minute() % 10)
		<< ':' << (datetime.second() / 10) 	<< (datetime.second() % 10);
}

}

#endif
