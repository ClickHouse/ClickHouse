#pragma once

#include <string.h>
#include <sql.h>

#include "Log.h"


/// Парсит строку вида key1=value1;key2=value2... TODO Парсинг значений в фигурных скобках.
static const char * nextKeyValuePair(const char * data, const char * end, StringRef & out_key, StringRef & out_value)
{
	if (data >= end)
		return nullptr;

	const char * key_begin = data;
	const char * key_end = reinterpret_cast<const char *>(memchr(key_begin, '=', end - key_begin));
	if (!key_end)
		return nullptr;

	const char * value_begin = key_end + 1;
	const char * value_end;
	if (value_begin >= end)
		value_end = value_begin;
	else
	{
		value_end = reinterpret_cast<const char *>(memchr(value_begin, ';', end - value_begin));
		if (!value_end)
			value_end = end;
	}

	out_key.data = key_begin;
	out_key.size = key_end - key_begin;

	out_value.data = value_begin;
	out_value.size = value_end - value_begin;

	if (value_end < end && *value_end == ';')
		return value_end + 1;
	return value_end;
}


template <typename SIZE_TYPE>
std::string stringFromSQLChar(SQLCHAR * data, SIZE_TYPE size)
{
	if (!data)
		return {};

	if (size < 0)
		size = strlen(reinterpret_cast<const char *>(data));

	return { reinterpret_cast<const char *>(data), static_cast<size_t>(size) };
}


template <typename PTR, typename LENGTH>
RETCODE fillOutputString(const char * value, size_t size_without_zero,
	PTR out_value, LENGTH out_value_max_length, LENGTH * out_value_length)
{
	if (out_value_length)
		*out_value_length = size_without_zero;

	if (out_value_max_length < 0)
		return SQL_ERROR;

	bool res = SQL_SUCCESS;

	if (out_value)
	{
		if (out_value_max_length >= static_cast<LENGTH>(size_without_zero + 1))
		{
			memcpy(out_value, value, size_without_zero + 1);
		}
		else
		{
			if (out_value_max_length > 0)
			{
				memcpy(out_value, value, out_value_max_length - 1);
				reinterpret_cast<char *>(out_value)[out_value_max_length - 1] = 0;

				LOG((char*)(out_value));
			}
			res = SQL_SUCCESS_WITH_INFO;
		}
	}

	return res;
}

template <typename PTR, typename LENGTH>
RETCODE fillOutputString(const char * value,
	PTR out_value, LENGTH out_value_max_length, LENGTH * out_value_length)
{
	return fillOutputString(value, strlen(value), out_value, out_value_max_length, out_value_length);
}

template <typename PTR, typename LENGTH>
RETCODE fillOutputString(const std::string & value,
	PTR out_value, LENGTH out_value_max_length, LENGTH * out_value_length)
{
	return fillOutputString(value.data(), value.size(), out_value, out_value_max_length, out_value_length);
}


template <typename NUM, typename PTR, typename LENGTH>
RETCODE fillOutputNumber(NUM num,
	PTR out_value, LENGTH out_value_max_length, LENGTH * out_value_length)
{
	if (out_value_length)
		*out_value_length = sizeof(num);

	if (out_value_max_length < 0)
		return SQL_ERROR;

	bool res = SQL_SUCCESS;

	if (out_value)
	{
		if (out_value_max_length >= static_cast<LENGTH>(sizeof(num)))
		{
			memcpy(out_value, &num, sizeof(num));
		}
		else
		{
			if (out_value_max_length > 0)
				memcpy(out_value, &num, out_value_max_length);

			res = SQL_SUCCESS_WITH_INFO;
		}
	}

	return res;
}
