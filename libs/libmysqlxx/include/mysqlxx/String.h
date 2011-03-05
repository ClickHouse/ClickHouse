#ifndef MYSQLXX_STRING_H
#define MYSQLXX_STRING_H

#include <string.h>
#include <stdio.h>
#include <time.h>

#include <string>
#include <limits>

#include <Yandex/DateLUT.h>

#include <mysqlxx/Types.h>
#include <mysqlxx/Exception.h>


namespace mysqlxx
{


template <typename T>
static void readIntText(T & x, const char * buf, size_t length)
{
	bool negative = false;
	x = 0;
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
			    throw Exception("Cannot parse integer: " + std::string(buf, length));
		}
		++buf;
	}
	if (negative)
		x = -x;
}


/// грубо
template <typename T>
static void readFloatText(T & x, const char * buf, size_t length)
{
	bool negative = false;
	x = 0;
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
				Int32 exponent = 0;
				readIntText(exponent, buf, end - buf);
				if (exponent == 0)
				{
					if (negative)
						x = -x;
					return;
				}
				else if (exponent > 0)
				{
					for (Int32 i = 0; i < exponent; ++i)
						x *= 10;
					if (negative)
						x = -x;
					return;
				}
				else
				{
					for (Int32 i = 0; i < exponent; ++i)
						x /= 10;
					if (negative)
						x = -x;
					return;
				}
			}
			case 'i':
			case 'I':
				x = std::numeric_limits<T>::infinity();
				if (negative)
					x = -x;
				return;
			case 'n':
			case 'N':
				x = std::numeric_limits<T>::quiet_NaN();
				return;
			default:
			    throw Exception("Cannot parse floating point number: " + std::string(buf, length));
		}
		++buf;
	}
	if (negative)
		x = -x;
}


class String
{
public:
	String(const char * data_, size_t length_) : m_data(data_), m_length(length_)
	{
	}

	bool getBool() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");
		
		return m_length > 0 && m_data[0] != '0';
	}

	UInt64 getUInt() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");
		
		UInt64 res;
		readIntText(res, m_data, m_length);
		return res;
	}

	Int64 getInt() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");
		
		Int64 res;
		readIntText(res, m_data, m_length);
		return res;
	}

	double getDouble() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");
		
		double res;
		readFloatText(res, m_data, m_length);
		return res;
	}

	time_t getDateTime() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");
		
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		
		if (m_length == 10)
		{
			return date_lut.makeDate(
				m_data[0] * 1000 + m_data[1] * 100 + m_data[2] * 10 + m_data[3],
				m_data[5] * 10 + m_data[6],
				m_data[8] * 10 + m_data[9]);
		}
		else if (m_length == 19)
		{
			return date_lut.makeDateTime(
				m_data[0] * 1000 + m_data[1] * 100 + m_data[2] * 10 + m_data[3],
				m_data[5] * 10 + m_data[6],
				m_data[8] * 10 + m_data[9],
				m_data[11] * 10 + m_data[12],
				m_data[14] * 10 + m_data[15],
				m_data[17] * 10 + m_data[18]);
		}
		else
			throw Exception("Cannot parse DateTime: " + getString());
	}

	time_t getDate() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");

		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();

		if (m_length == 10 || m_length == 19)
		{
			return date_lut.makeDate(
				m_data[0] * 1000 + m_data[1] * 100 + m_data[2] * 10 + m_data[3],
				m_data[5] * 10 + m_data[6],
				m_data[8] * 10 + m_data[9]);
		}
		else
			throw Exception("Cannot parse Date: " + getString());
	}

	std::string getString() const
	{
		if (unlikely(isNull()))
			throw Exception("Value is NULL");
		
		return std::string(m_data, m_length);
	}

	bool isNull() const
	{
		return m_data == NULL;
	}

	/// Для совместимости
	bool is_null() const { return isNull(); }

	template <typename T> T get() const;

	const char * data() const 	{ return m_data; }
	size_t length() const 		{ return m_length; }
	size_t size() const 		{ return m_length; }

private:
	const char * m_data;
	size_t m_length;

	bool checkDateTime() const
	{
		return m_length >= 10 && m_data[4] == '-' && m_data[7] == '-';
	}
};


template <> inline bool 				String::get<bool				>() const { return getBool(); }
template <> inline char 				String::get<char				>() const { return getInt(); }
template <> inline signed char 			String::get<signed char			>() const { return getInt(); }
template <> inline unsigned char		String::get<unsigned char		>() const { return getUInt(); }
template <> inline short 				String::get<short				>() const { return getInt(); }
template <> inline unsigned short		String::get<unsigned short		>() const { return getUInt(); }
template <> inline int 					String::get<int					>() const { return getInt(); }
template <> inline unsigned int 		String::get<unsigned int		>() const { return getUInt(); }
template <> inline long 				String::get<long				>() const { return getInt(); }
template <> inline unsigned long 		String::get<unsigned long		>() const { return getUInt(); }
template <> inline long long 			String::get<long long			>() const { return getInt(); }
template <> inline unsigned long long 	String::get<unsigned long long	>() const { return getUInt(); }
template <> inline float 				String::get<float				>() const { return getDouble(); }
template <> inline double 				String::get<double				>() const { return getDouble(); }
template <> inline std::string			String::get<std::string			>() const { return getString(); }


inline std::ostream & operator<< (std::ostream & ostr, const String & x)
{
	return ostr.write(x.data(), x.size());
}


}

#endif
