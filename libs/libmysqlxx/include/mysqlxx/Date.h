#ifndef MYSQLXX_DATE_H
#define MYSQLXX_DATE_H

#include <string.h>
#include <string>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Exception.h>


namespace mysqlxx
{

/** Хранит дату в broken-down виде.
  * Может быть инициализирован из даты в текстовом виде '2011-01-01' и из time_t.
  * Неявно преобразуется в time_t.
  * Сериализуется в ostream в текстовом виде.
  * Внимание: преобразование в unix timestamp и обратно производится в текущей тайм-зоне!
  * При переводе стрелок назад, возникает неоднозначность - преобразование производится в меньшее значение.
  *
  * packed - для memcmp
  */
class __attribute__ ((__packed__)) Date
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

		m_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
		m_month = (s[5] - '0') * 10 + (s[6] - '0');
		m_day = (s[8] - '0') * 10 + (s[9] - '0');
	}

public:
	explicit Date(time_t time)
	{
		init(time);
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

	Date()
	{
		init(time(0));
	}

	Date & operator= (time_t time)
	{
		init(time);
		return *this;
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
		return -1 == memcmp(this, &other, sizeof(*this));
	}

	bool operator== (const Date & other) const
	{
		return 0 == memcmp(this, &other, sizeof(*this));
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
