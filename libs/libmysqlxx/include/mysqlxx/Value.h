#pragma once

#include <string.h>
#include <stdio.h>
#include <time.h>
#include <math.h>

#include <string>
#include <limits>

#include <Yandex/Common.h>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Types.h>

/// Обрезать длинный запрос до указанной длины для текста исключения.
#define MYSQLXX_QUERY_PREVIEW_LENGTH 1000


namespace mysqlxx
{


class ResultBase;


/** Представляет одно значение, считанное из MySQL.
  * Объект сам не хранит данные, а является всего лишь обёрткой над парой (const char *, size_t).
  * Если уничтожить UseQueryResult/StoreQueryResult или Connection,
  *  или считать следующий Row при использовании UseQueryResult, то объект станет некорректным.
  * Позволяет преобразовать значение (распарсить) в различные типы данных:
  * - с помощью функций вида getUInt(), getString(), ... (рекомендуется);
  * - с помощью шаблонной функции get<Type>(), которая специализирована для многих типов (для шаблонного кода);
  * - шаблонная функция get<Type> работает также для всех типов, у которых есть конструктор из Value
  *   (это сделано для возможности расширения);
  * - с помощью operator Type() - но этот метод реализован лишь для совместимости и не рекомендуется
  *   к использованию, так как неудобен (часто возникают неоднозначности).
  *
  * При ошибке парсинга, выкидывается исключение.
  * При попытке достать значение, которое равно nullptr, выкидывается исключение
  * - используйте метод isNull() для проверки.
  *
  * Во всех распространённых системах, time_t - это всего лишь typedef от Int64 или Int32.
  * Для того, чтобы можно было писать row[0].get<time_t>(), ожидая, что значение вида '2011-01-01 00:00:00'
  *  корректно распарсится согласно текущей тайм-зоне, сделано так, что метод getUInt и соответствующие методы get<>()
  *  также умеют парсить дату и дату-время.
  */
class Value
{
public:
	/** Параметр res_ используется только для генерации подробной информации в исключениях.
	  * Можно передать NULL - тогда подробной информации в исключениях не будет.
	  */
	Value(const char * data_, size_t length_, const ResultBase * res_) : m_data(data_), m_length(length_), res(res_)
	{
	}

	/// Получить значение bool.
	bool getBool() const
	{
		if (unlikely(isNull()))
			throwException("Value is NULL");
		
		return m_length > 0 && m_data[0] != '0';
	}

	/// Получить беззнаковое целое.
	UInt64 getUInt() const
	{
		if (unlikely(isNull()))
			throwException("Value is NULL");
		
		return readUIntText(m_data, m_length);;
	}

	/// Получить целое со знаком или дату или дату-время (в unix timestamp согласно текущей тайм-зоне).
	Int64 getInt() const
	{
		return getIntOrDateTime();
	}

	/// Получить число с плавающей запятой.
	double getDouble() const
	{
		if (unlikely(isNull()))
			throwException("Value is NULL");
		
		return readFloatText(m_data, m_length);
	}

	/// Получить дату-время (из значения вида '2011-01-01 00:00:00').
	DateTime getDateTime() const
	{
		return DateTime(data(), size());
	}

	/// Получить дату (из значения вида '2011-01-01' или '2011-01-01 00:00:00').
	Date getDate() const
	{
		return Date(data(), size());
	}

	/// Получить строку.
	std::string getString() const
	{
		if (unlikely(isNull()))
			throwException("Value is NULL");
		
		return std::string(m_data, m_length);
	}

	/// Является ли NULL.
	bool isNull() const
	{
		return m_data == nullptr;
	}

	/// Для совместимости (используйте вместо этого метод isNull())
	bool is_null() const { return isNull(); }

	/** Получить любой поддерживаемый тип (для шаблонного кода).
	  * Поддерживаются основные типы, а также любые типы с конструктором от Value (для удобства расширения).
	  */
	template <typename T> T get() const;

	/// Для совместимости. Не рекомендуется к использованию, так как неудобен (часто возникают неоднозначности).
	template <typename T> operator T() const { return get<T>(); }

	const char * data() const 	{ return m_data; }
	size_t length() const 		{ return m_length; }
	size_t size() const 		{ return m_length; }
	bool empty() const 			{ return 0 == m_length; }

private:
	const char * m_data;
	size_t m_length;
	const ResultBase * res;

	
	bool checkDateTime() const
	{
		return (m_length == 10 || m_length == 19) && m_data[4] == '-' && m_data[7] == '-';
	}


	time_t getDateTimeImpl() const
	{
		DateLUT & date_lut = DateLUT::instance();

		if (m_length == 10)
		{
			return date_lut.makeDate(
				(m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
				(m_data[5] - '0') * 10 + (m_data[6] - '0'),
				(m_data[8] - '0') * 10 + (m_data[9] - '0'));
		}
		else if (m_length == 19)
		{
			return date_lut.makeDateTime(
				(m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
				(m_data[5] - '0') * 10 + (m_data[6] - '0'),
				(m_data[8] - '0') * 10 + (m_data[9] - '0'),
				(m_data[11] - '0') * 10 + (m_data[12] - '0'),
				(m_data[14] - '0') * 10 + (m_data[15] - '0'),
				(m_data[17] - '0') * 10 + (m_data[18] - '0'));
		}
		else
			throwException("Cannot parse DateTime");

		return 0;	/// чтобы не было warning-а.
	}


	time_t getDateImpl() const
	{
		DateLUT & date_lut = DateLUT::instance();

		if (m_length == 10 || m_length == 19)
		{
			return date_lut.makeDate(
				(m_data[0] - '0') * 1000 + (m_data[1] - '0') * 100 + (m_data[2] - '0') * 10 + (m_data[3] - '0'),
				(m_data[5] - '0') * 10 + (m_data[6] - '0'),
				(m_data[8] - '0') * 10 + (m_data[9] - '0'));
		}
		else
			throwException("Cannot parse Date");

		return 0;	/// чтобы не было warning-а.
	}


	Int64 getIntImpl() const
	{
		return readIntText(m_data, m_length);;
	}


	Int64 getIntOrDateTime() const
	{
		if (unlikely(isNull()))
			throwException("Value is NULL");

		if (checkDateTime())
			return getDateTimeImpl();
		else
			return getIntImpl();
	}


	Int64 getIntOrDate() const
	{
		if (unlikely(isNull()))
			throwException("Value is NULL");

		if (checkDateTime())
			return getDateImpl();
		else
		{
			DateLUT & date_lut = DateLUT::instance();
			return date_lut.toDate(getIntImpl());
		}
	}


	/// Прочитать беззнаковое целое в простом формате из не-0-terminated строки.
	UInt64 readUIntText(const char * buf, size_t length) const
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


	/// Прочитать знаковое целое в простом формате из не-0-terminated строки.
	Int64 readIntText(const char * buf, size_t length) const
	{
		bool negative = false;
		Int64 x = 0;
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
		if (negative)
			x = -x;

		return x;
	}


	/// Прочитать число с плавающей запятой в простом формате, с грубым округлением, из не-0-terminated строки.
	double readFloatText(const char * buf, size_t length) const
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
					Int32 exponent = readIntText(buf, end - buf);
					x *= exp10(exponent);
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


	/// Выкинуть исключение с подробной информацией
	void throwException(const char * text) const;
};


template <> inline bool 				Value::get<bool					>() const { return getBool(); }
template <> inline char 				Value::get<char					>() const { return getInt(); }
template <> inline signed char 			Value::get<signed char			>() const { return getInt(); }
template <> inline unsigned char		Value::get<unsigned char		>() const { return getUInt(); }
template <> inline short 				Value::get<short				>() const { return getInt(); }
template <> inline unsigned short		Value::get<unsigned short		>() const { return getUInt(); }
template <> inline int 					Value::get<int					>() const { return getInt(); }
template <> inline unsigned int 		Value::get<unsigned int			>() const { return getUInt(); }
template <> inline long 				Value::get<long					>() const { return getInt(); }
template <> inline unsigned long 		Value::get<unsigned long		>() const { return getUInt(); }
template <> inline long long 			Value::get<long long			>() const { return getInt(); }
template <> inline unsigned long long 	Value::get<unsigned long long	>() const { return getUInt(); }
template <> inline float 				Value::get<float				>() const { return getDouble(); }
template <> inline double 				Value::get<double				>() const { return getDouble(); }
template <> inline std::string			Value::get<std::string			>() const { return getString(); }
template <> inline Date					Value::get<Date					>() const { return getDate(); }
template <> inline DateTime				Value::get<DateTime				>() const { return getDateTime(); }

template <> inline VisitID_t			Value::get<VisitID_t			>() const { return VisitID_t(getUInt()); }

template <typename T> inline T			Value::get() 						const { return T(*this); }


inline std::ostream & operator<< (std::ostream & ostr, const Value & x)
{
	return ostr.write(x.data(), x.size());
}


}
