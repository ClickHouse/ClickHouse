#ifndef MYSQLXX_TYPES_H
#define MYSQLXX_TYPES_H

#include <string>
#include <mysql/mysql.h>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Exception.h>


namespace mysqlxx
{

typedef unsigned long long UInt64;
typedef long long Int64;
typedef unsigned UInt32;
typedef int Int32;

typedef unsigned long * MYSQL_LENGTHS;
typedef MYSQL_FIELD * MYSQL_FIELDS;

/// Для совместимости с mysql++
typedef time_t sql_datetime;
typedef time_t sql_timestamp;
typedef time_t sql_date;
typedef std::string sql_char;

class Date
{
private:
	unsigned char m_day;
	unsigned char m_month;
	unsigned short m_year;

public:
	Date(time_t time)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		const Yandex::DateLUT::Values & values = date_lut.getValues(time);

		m_day = values.day_of_month;
		m_month = values.month;
		m_year = values.year;
	}

	/// Для совместимости
	Date(unsigned short year_, unsigned char month_, unsigned char day_)
		: m_day(day_), m_month(month_), m_year(year_)
	{
	}

	/// Для совместимости
	Date(const std::string & s)
	{
		if (s.size() < 10)
			throw Exception("Cannot parse Date: " + s);

		m_year = s[0] * 1000 + s[1] * 100 + s[2] * 10 + s[3];
		m_month = s[5] * 10 + s[6];
		m_day = s[8] * 10 + s[9];
	}

	/// Для совместимости
	Date() : m_day(1), m_month(1), m_year(2000)
	{
	}

	/// Для совместимости
	operator time_t() const
	{
		return Yandex::DateLUTSingleton::instance().makeDate(m_year, m_month, m_day);
	}

	unsigned char day() const { return m_day; }
	unsigned char month() const { return m_month; }
	unsigned short year() const { return m_year; }

	void day(unsigned char x) { m_day = x; }
	void month(unsigned char x) { m_month = x; }
	void year(unsigned short x) { m_year = x; }

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
};

inline std::ostream & operator<< (std::ostream & ostr, const Date & date)
{
	return ostr << date.year()
		<< '-' << (date.month() < 10 ? "0" : "") << date.month()
		<< '-' << (date.day() < 10 ? "0" : "") << date.day();
}


struct NullType
{
};

extern NullType null;


template <typename T>
class Null
{
public:
	T data;
	bool is_null;
	
	Null() : is_null(true) {}
	Null(NullType data) : is_null(true) {}
	Null(const T & data_) : data(data_), is_null(false) {}

	operator T()
	{
		if (is_null)
			throw Exception("Value is NULL");
		return data;
	}

	Null<T> & operator= (const T & data_) { is_null = false; data = data_; return *this; }

	bool isNull() { return is_null; }

	bool operator< (const Null<T> & other)
	{
		return is_null < other.is_null
			|| (is_null == other.is_null && data < other.data);
	}

	bool operator< (const NullType other) { return false; }

	bool operator== (const Null<T> & other)
	{
		return is_null == other.is_null && data == other.data;
	}

	bool operator== (const NullType other) { return is_null; }
};


}

#endif
