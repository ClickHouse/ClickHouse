#pragma once

#include <string.h>
#include <string>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Exception.h>


namespace mysqlxx
{

/** Хранит дату в broken-down виде.
  * Может быть инициализирован из даты в текстовом виде '2011-01-01' и из time_t.
  * Может быть инициализирован из даты в текстовом виде '20110101...(юзаются первые 8 символов)
  * Неявно преобразуется в time_t.
  * Сериализуется в ostream в текстовом виде.
  * Внимание: преобразование в unix timestamp и обратно производится в текущей тайм-зоне!
  * При переводе стрелок назад, возникает неоднозначность - преобразование производится в меньшее значение.
  *
  * packed - для memcmp (из-за того, что m_year - 2 байта, little endian, работает корректно только до 2047 года)
  */
class __attribute__ ((__packed__)) Date
{
private:
	unsigned short m_year;
	unsigned char m_month;
	unsigned char m_day;

	void init(time_t time)
	{
		DateLUT & date_lut = DateLUT::instance();
		const DateLUT::Values & values = date_lut.getValues(time);

		m_year = values.year;
		m_month = values.month;
		m_day = values.day_of_month;
	}

	void init(const char * s, size_t length)
	{
		if (length < 8)
			throw Exception("Cannot parse Date: " + std::string(s, length));

		m_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');

		if (s[4] == '-')
		{
			if (length < 10)
				throw Exception("Cannot parse Date: " + std::string(s, length));
			m_month = (s[5] - '0') * 10 + (s[6] - '0');
			m_day = (s[8] - '0') * 10 + (s[9] - '0');
		}
		else
		{
			m_month = (s[4] -'0') * 10 + (s[5] -'0');
			m_day = (s[6] - '0')* 10 + (s[7] -'0');
		}
	}

public:
	explicit Date(time_t time)
	{
		init(time);
	}

	Date(DayNum_t day_num)
	{
		const DateLUT::Values & values = DateLUT::instance().getValues(day_num);
		m_year 	= values.year;
		m_month = values.month;
		m_day 	= values.day_of_month;
	}

	Date(unsigned short year_, unsigned char month_, unsigned char day_)
		: m_year(year_), m_month(month_), m_day(day_)
	{
	}

	explicit Date(const std::string & s)
	{
		init(s.data(), s.size());
	}

	Date(const char * data, size_t length)
	{
		init(data, length);
	}

	Date() : m_year(0), m_month(0), m_day(0)
	{
	}

	Date(const Date & x)
	{
		operator=(x);
	}

	Date & operator= (const Date & x)
	{
		m_year = x.m_year;
		m_month = x.m_month;
		m_day = x.m_day;

		return *this;
	}

	Date & operator= (time_t time)
	{
		init(time);
		return *this;
	}

	operator time_t() const
	{
		return DateLUT::instance().makeDate(m_year, m_month, m_day);
	}

	DayNum_t getDayNum() const
	{
		return DateLUT::instance().makeDayNum(m_year, m_month, m_day);
	}

	operator DayNum_t() const
	{
		return getDayNum();
	}

	unsigned short year() const { return m_year; }
	unsigned char month() const { return m_month; }
	unsigned char day() const { return m_day; }

	void year(unsigned short x) { m_year = x; }
	void month(unsigned char x) { m_month = x; }
	void day(unsigned char x) { m_day = x; }

	bool operator< (const Date & other) const
	{
		return 0 > memcmp(this, &other, sizeof(*this));
	}

	bool operator> (const Date & other) const
	{
		return 0 < memcmp(this, &other, sizeof(*this));
	}

	bool operator<= (const Date & other) const
	{
		return 0 >= memcmp(this, &other, sizeof(*this));
	}

	bool operator>= (const Date & other) const
	{
		return 0 <= memcmp(this, &other, sizeof(*this));
	}

	bool operator== (const Date & other) const
	{
		return 0 == memcmp(this, &other, sizeof(*this));
	}

	bool operator!= (const Date & other) const
	{
		return !(*this == other);
	}

	std::string toString(char separator = '-') const
	{
		std::stringstream ss;
		if (separator)
			ss << year() << separator << (month() / 10) << (month() % 10)
				<< separator << (day() / 10) << (day() % 10);
		else
			ss << year() << (month() / 10) << (month() % 10)
				<< (day() / 10) << (day() % 10);
		return ss.str();
	}
};

inline std::ostream & operator<< (std::ostream & ostr, const Date & date)
{
	return ostr << date.year()
		<< '-' << (date.month() / 10) << (date.month() % 10)
		<< '-' << (date.day() / 10) << (date.day() % 10);
}

}


namespace std
{
inline string to_string(const mysqlxx::Date & date)
{
	return date.toString();
}
}
