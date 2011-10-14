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
  * Может быть инициализирован из даты в текстовом виде '20110101...(юзаются первые 8 символов)
  * Неявно преобразуется в time_t.
  * Сериализуется в ostream в текстовом виде.
  * Внимание: преобразование в unix timestamp и обратно производится в текущей тайм-зоне!
  * При переводе стрелок назад, возникает неоднозначность - преобразование производится в меньшее значение.
  *
  * packed - для memcmp (из-за того, что m_year - 2 байта, little endian, работает корректно только до 2047 года)
  */
class Date
{
private:
	struct __attribute__ ((__packed__)) {
		
		unsigned short m_year;
		unsigned char m_month;
		unsigned char m_day;
	} data;

	void init(time_t time)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		const Yandex::DateLUT::Values & values = date_lut.getValues(time);

		data.m_year = values.year;
		data.m_month = values.month;
		data.m_day = values.day_of_month;
	}

	void init(const char * s, size_t length)
	{
		if(length < 8)
			throw Exception("Cannot parse Date: " + std::string(s, length));
		data.m_year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
		if(s[4] == '-')
		{
			if(length < 10)
				throw Exception("Cannot parse Date: " + std::string(s, length));
			data.m_month = (s[5] - '0') * 10 + (s[6] - '0');
			data.m_day = (s[8] - '0') * 10 + (s[9] - '0');
		}
		else
		{
			data.m_month = (s[4] -'0') * 10 + (s[5] -'0');
			data.m_day = (s[6] - '0')* 10 + (s[7] -'0');
		}
	}

public:
	explicit Date(time_t time)
	{
		init(time);
	}

	Date(unsigned short year_, unsigned char month_, unsigned char day_)
	{
		data.m_year = year_;
		data.m_month = month_;
		data.m_day = day_;
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

	Date(const Date & x)
	{
		operator=(x);
	}

	Date & operator= (const Date & x)
	{
		data.m_year = x.data.m_year;
		data.m_month = x.data.m_month;
		data.m_day = x.data.m_day;

		return *this;
	}

	Date & operator= (time_t time)
	{
		init(time);
		return *this;
	}

	operator time_t() const
	{
		return Yandex::DateLUTSingleton::instance().makeDate(data.m_year, data.m_month, data.m_day);
	}

	Yandex::DayNum_t getDayNum() const
	{
		return Yandex::DateLUTSingleton::instance().makeDayNum(data.m_year, data.m_month, data.m_day);
	}

	operator Yandex::DayNum_t() const
	{
		return getDayNum();
	}

	unsigned short year() const { return data.m_year; }
	unsigned char month() const { return data.m_month; }
	unsigned char day() const { return data.m_day; }

	void year(unsigned short x) { data.m_year = x; }
	void month(unsigned char x) { data.m_month = x; }
	void day(unsigned char x) { data.m_day = x; }

	bool operator< (const Date & other) const
	{
		if(memcmp( &data, &other.data, sizeof(data)) < 0)
			return true;
		return false;
	}

	bool operator> (const Date & other) const
	{
		if(memcmp( &data, &other.data, sizeof(data)) > 0)
			return true;
		return false;
	}

	bool operator<= (const Date & other) const
	{
		if(memcmp(&data, &other.data, sizeof(data)) <= 0)
			return true;
		return false;
	}

	bool operator>= (const Date & other) const
	{
		if(memcmp(&data, &other.data, sizeof(data)) >= 0)
			return true;
		return false;
	}

	bool operator== (const Date & other) const
	{
		if(memcmp(&data, &other.data, sizeof(data)) == 0)
			return true;
		return false;
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
