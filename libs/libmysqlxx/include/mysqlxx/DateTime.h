#pragma once

#include <string>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Date.h>
#include <iomanip>


namespace mysqlxx
{

/** Хранит дату и время в broken-down виде.
  * Может быть инициализирован из даты и времени в текстовом виде '2011-01-01 00:00:00' и из time_t.
  * Неявно преобразуется в time_t.
  * Сериализуется в ostream в текстовом виде.
  * Внимание: преобразование в unix timestamp и обратно производится в текущей тайм-зоне!
  * При переводе стрелок назад, возникает неоднозначность - преобразование производится в меньшее значение.
  *
  * packed - для memcmp (из-за того, что m_year - 2 байта, little endian, работает корректно только до 2047 года)
  */
class __attribute__ ((__packed__)) DateTime
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
		if (unlikely(time > DATE_LUT_MAX || time == 0))
		{
			m_year 		= 0;
			m_month 	= 0;
			m_day 		= 0;
			m_hour 		= 0;
			m_minute 	= 0;
			m_second 	= 0;

			return;
		}

		DateLUT & date_lut = DateLUT::instance();
		const DateLUT::Values & values = date_lut.getValues(time);

		m_year = values.year;
		m_month = values.month;
		m_day = values.day_of_month;
		m_hour = date_lut.toHourInaccurate(time);
		m_minute = date_lut.toMinuteInaccurate(time);
		m_second = date_lut.toSecondInaccurate(time);
	}

	void init(const char * s, size_t length)
	{
		if (length < 19)
			throw Exception("Cannot parse DateTime: " + std::string(s, length));

		m_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
		m_month = (s[5] - '0') * 10 + (s[6] - '0');
		m_day = (s[8] - '0') * 10 + (s[9] - '0');

		m_hour = (s[11] - '0') * 10 + (s[12] - '0');
		m_minute = (s[14] - '0') * 10 + (s[15] - '0');
		m_second = (s[17] - '0') * 10 + (s[18] - '0');
	}

public:
	explicit DateTime(time_t time)
	{
		init(time);
	}

	DateTime(unsigned short year_, unsigned char month_, unsigned char day_,
		unsigned char hour_, unsigned char minute_, unsigned char second_)
		: m_year(year_), m_month(month_), m_day(day_), m_hour(hour_), m_minute(minute_), m_second(second_)
	{
	}

	explicit DateTime(const std::string & s)
	{
		if (s.size() < 19)
			throw Exception("Cannot parse DateTime: " + s);

		init(s.data(), s.size());
	}

	DateTime() : m_year(0), m_month(0), m_day(0), m_hour(0), m_minute(0), m_second(0)
	{
	}

	DateTime(const char * data, size_t length)
	{
		init(data, length);
	}

	DateTime(const DateTime & x)
	{
		operator=(x);
	}

	DateTime & operator= (const DateTime & x)
	{
		m_year = x.m_year;
		m_month = x.m_month;
		m_day = x.m_day;
		m_hour = x.m_hour;
		m_minute = x.m_minute;
		m_second = x.m_second;

		return *this;
	}

	DateTime & operator= (time_t time)
	{
		init(time);
		return *this;
	}

	operator time_t() const
	{
		return m_year == 0
			? 0
			: DateLUT::instance().makeDateTime(m_year, m_month, m_day, m_hour, m_minute, m_second);
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

	Date toDate() const { return Date(m_year, m_month, m_day); }

	DateTime toStartOfDate() { return DateTime(m_year, m_month, m_day, 0, 0, 0); }

	bool operator< (const DateTime & other) const
	{
		return 0 > memcmp(this, &other, sizeof(*this));
	}

	bool operator> (const DateTime & other) const
	{
		return 0 < memcmp(this, &other, sizeof(*this));
	}

	bool operator<= (const DateTime & other) const
	{
		return 0 >= memcmp(this, &other, sizeof(*this));
	}

	bool operator>= (const DateTime & other) const
	{
		return 0 <= memcmp(this, &other, sizeof(*this));
	}

	bool operator== (const DateTime & other) const
	{
		return 0 == memcmp(this, &other, sizeof(*this));
	}

	bool operator!= (const DateTime & other) const
	{
		return !(*this == other);
	}
};

inline std::ostream & operator<< (std::ostream & ostr, const DateTime & datetime)
{
	ostr << std::setfill('0') << std::setw(4) << datetime.year();

	ostr << '-' << (datetime.month() / 10) 	<< (datetime.month() % 10)
		<< '-' << (datetime.day() / 10) 	<< (datetime.day() % 10)
		<< ' ' << (datetime.hour() / 10) 	<< (datetime.hour() % 10)
		<< ':' << (datetime.minute() / 10) 	<< (datetime.minute() % 10)
		<< ':' << (datetime.second() / 10) 	<< (datetime.second() % 10);

	return ostr;
}

}


namespace std
{
inline string to_string(const mysqlxx::DateTime & datetime)
{
	stringstream str;
	str << datetime;
	return str.str();
}
}
