#ifndef MYSQLXX_MANIP_H
#define MYSQLXX_MANIP_H

#include <cstring>
#include <iostream>
#include <mysqlxx/Types.h>


namespace mysqlxx
{

/** @brief Манипулятор ostream, который escape-ит строки для записи в tab delimited файл. */
enum escape_enum
{
	escape
};


/** @brief Манипулятор ostream, который quote-ит строки для записи в MySQL запрос.
  * Внимание! Не использует функции MySQL API, а использует свой метод quote-инга,
  * который может быть некорректным при использовании некоторых японских кодировок
  * (multi-byte attack), а также может оказаться некорректным при изменении libmysqlclient.
  * Это сделано для увеличения производительности и это имеет значение.
  */
enum quote_enum
{
	quote
};


struct EscapeManipResult
{
	std::ostream & ostr;

	EscapeManipResult(std::ostream & ostr_) : ostr(ostr_) {}

	std::ostream & operator<< (bool value) 					{ return ostr << value; }
	std::ostream & operator<< (char value) 					{ return ostr << value; }
	std::ostream & operator<< (unsigned char value) 		{ return ostr << value; }
	std::ostream & operator<< (signed char value)			{ return ostr << value; }
	std::ostream & operator<< (short value)					{ return ostr << value; }
	std::ostream & operator<< (unsigned short value)		{ return ostr << value; }
	std::ostream & operator<< (int value)					{ return ostr << value; }
	std::ostream & operator<< (unsigned int value)			{ return ostr << value; }
	std::ostream & operator<< (long value)					{ return ostr << value; }
	std::ostream & operator<< (unsigned long value)			{ return ostr << value; }
	std::ostream & operator<< (float value)					{ return ostr << value; }
	std::ostream & operator<< (double value)				{ return ostr << value; }
	std::ostream & operator<< (long long value)				{ return ostr << value; }
	std::ostream & operator<< (unsigned long long value)	{ return ostr << value; }

	std::ostream & operator<< (const std::string & value)
	{
		writeEscapedData(value.data(), value.length());
		return ostr;
	}


	std::ostream & operator<< (const char * value)
	{
		while (const char * it = std::strpbrk(value, "\t\n\\"))
		{
			ostr.write(value, it - value);
			switch (*it)
			{
				case '\t':
					ostr.write("\\t", 2);
					break;
				case '\n':
					ostr.write("\\n", 2);
					break;
				case '\\':
					ostr.write("\\\\", 2);
					break;
				default:
					;
			}
			value = it + 1;
		}
		return ostr << value;
	}

private:

	void writeEscapedData(const char * data, size_t length)
	{
		size_t i = 0;

		while (true)
		{
			size_t remaining_length = std::strlen(data);
			(*this) << data;
			if (i + remaining_length == length)
				break;

			ostr.write("\\0", 2);
			i += remaining_length + 1;
			data += remaining_length + 1;
		}
	}
};

inline EscapeManipResult operator<< (std::ostream & ostr, escape_enum manip)
{
	return EscapeManipResult(ostr);
}


struct QuoteManipResult
{
public:
	std::ostream & ostr;

	QuoteManipResult(std::ostream & ostr_) : ostr(ostr_) {}

	std::ostream & operator<< (bool value) 					{ return ostr << value; }
	std::ostream & operator<< (char value) 					{ return ostr << value; }
	std::ostream & operator<< (unsigned char value) 		{ return ostr << value; }
	std::ostream & operator<< (signed char value)			{ return ostr << value; }
	std::ostream & operator<< (short value)					{ return ostr << value; }
	std::ostream & operator<< (unsigned short value)		{ return ostr << value; }
	std::ostream & operator<< (int value)					{ return ostr << value; }
	std::ostream & operator<< (unsigned int value)			{ return ostr << value; }
	std::ostream & operator<< (long value)					{ return ostr << value; }
	std::ostream & operator<< (unsigned long value)			{ return ostr << value; }
	std::ostream & operator<< (float value)					{ return ostr << value; }
	std::ostream & operator<< (double value)				{ return ostr << value; }
	std::ostream & operator<< (long long value)				{ return ostr << value; }
	std::ostream & operator<< (unsigned long long value)	{ return ostr << value; }

	std::ostream & operator<< (const std::string & value)
	{
		ostr.put('\'');
		writeEscapedData(value.data(), value.length());
		ostr.put('\'');

		return ostr;
	}


	std::ostream & operator<< (const char * value)
	{
		ostr.put('\'');
		writeEscapedCString(value);
		ostr.put('\'');
		return ostr;
	}


/*	template <typename T1, typename T2>
	std::ostream & operator<< (const std::pair<T1, T2> & pair)
	{
		return ostr << quote << pair.first
			<< ',' << quote << pair.second;
	}


	template <typename T1, typename T2, typename T3>
	std::ostream & operator<< (const Poco::Tuple<T1, T2, T3> & tuple)
	{
		return ostr << quote << tuple.template get<0>()
			<< ',' << quote << tuple.template get<1>()
			<< ',' << quote << tuple.template get<2>();
	}*/

private:

	void writeEscapedCString(const char * value)
	{
		while (const char * it = std::strpbrk(value, "'\\\""))
		{
			ostr.write(value, it - value);
			switch (*it)
			{
				case '"':
					ostr.write("\\\"", 2);
					break;
				case '\'':
					ostr.write("\\'", 2);
					break;
				case '\\':
					ostr.write("\\\\", 2);
					break;
				default:
					;
			}
			value = it + 1;
		}
		ostr << value;
	}


	void writeEscapedData(const char * data, size_t length)
	{
		size_t i = 0;

		while (true)
		{
			size_t remaining_length = std::strlen(data);
			writeEscapedCString(data);
			if (i + remaining_length == length)
				break;

			ostr.write("\\0", 2);
			i += remaining_length + 1;
			data += remaining_length + 1;
		}
	}
};

inline QuoteManipResult operator<< (std::ostream & ostr, quote_enum manip)
{
	return QuoteManipResult(ostr);
}
	
}

#endif
