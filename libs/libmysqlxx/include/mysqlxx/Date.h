#ifndef MYSQLXX_DATE_H
#define MYSQLXX_DATE_H

#include <string>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Exception.h>


namespace mysqlxx
{

class Date
{
private:
	unsigned short m_year;
	unsigned char m_month;
	unsigned char m_day;

	void init(time_t time)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		const Yandex::DateLUT::Values & values = date_lut.getValues(time);

		m_year = values.year;
		m_month = values.month;
		m_day = values.day_of_month;
	}

	void init(const char * s, size_t length)
	{
		if (length < 10)
			throw Exception("Cannot parse Date: " + std::string(s, length));

		m_year = s[0] * 1000 + s[1] * 100 + s[2] * 10 + s[3];
		m_month = s[5] * 10 + s[6];
		m_day = s[8] * 10 + s[9];
	}

public:
	Date(time_t time)
	{
		init(time);
	}

	Date(unsigned short year_, unsigned char month_, unsigned char day_)
		: m_year(year_), m_month(month_), m_day(day_)
	{
	}

	Date(const std::string & s)
	{
		init(s.data(), s.size());
	}

	Date(const char * data, size_t length)
	{
		init(data, length);
	}

	Date()
	{
		init(time(0));
	}

	operator time_t() const
	{
		return Yandex::DateLUTSingleton::instance().makeDate(m_year, m_month, m_day);
	}

	unsigned short year() const { return m_year; }
	unsigned char month() const { return m_month; }
	unsigned char day() const { return m_day; }

	void year(unsigned short x) { m_year = x; }
	void month(unsigned char x) { m_month = x; }
	void day(unsigned char x) { m_day = x; }

	bool operator< (const Date & other) const
	{
		return m_year < other.m_year
			|| (m_year == other.m_year && m_month < other.m_month)
			|| (m_year == other.m_year && m_month == other.m_month && m_day < other.m_day);
	}

	bool operator== (const Date & other) const
	{
		return m_year == other.m_year && m_month == other.m_month && m_day == other.m_day;
	}

	bool operator!= (const Date & other) const
	{
		return !(*this == other);
	}
};

inline std::ostream & operator<< (std::ostream & ostr, const Date & date)
{
	return ostr << date.year()
		<< '-' << (date.month() / 10) << (date.month() % 10)
		<< '-' << (date.day() / 10) << (date.day() % 10);
}

}

#endif
