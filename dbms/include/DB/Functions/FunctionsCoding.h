#pragma once

#include <DB/IO/ReadBufferFromString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>
#include <arpa/inet.h>


namespace DB
{

/** Функции кодирования:
  *
  * IPv4NumToString(num) - См. ниже.
  * IPv4StringToNum(string) - Преобразуют, например, '192.168.0.1' в 3232235521 и наоборот.
  *
  * hex(x) -	Возвращает hex; буквы заглавные; префиксов 0x или суффиксов h нет.
  * 			Для чисел возвращает строку переменной длины - hex в "человеческом" (big endian) формате, с вырезанием старших нулей, но только по целым байтам. Для дат и дат-с-временем - как для чисел.
  * 			Например, hex(257) = '0101'.
  * unhex(string) -	Возвращает строку, hex от которой равен string с точностью до регистра и отбрасывания одного ведущего нуля.
  * 				Если такой строки не существует, оставляет за собой право вернуть любой мусор.
  *
  * bitmaskToArray(x) - Возвращает массив степеней двойки в двоичной записи x. Например, bitmaskToArray(50) = [2, 16, 32].
  */


/// Включая нулевой символ в конце.
#define MAX_UINT_HEX_LENGTH 20

const auto ipv6_fixed_string_length = 16;

class FunctionIPv6NumToString : public IFunction
{
public:
	String getName() const { return "IPv6NumToString"; }

	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const auto ptr = typeid_cast<const DataTypeFixedString *>(arguments[0].get());
		if (!ptr || ptr->getN() != ipv6_fixed_string_length)
			throw Exception("Illegal type " + arguments[0]->getName() +
							" of argument of function " + getName() +
							", expected FixedString(" + toString(ipv6_fixed_string_length) + ")",
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result)
	{
		const auto & col_name_type = block.getByPosition(arguments[0]);
		const ColumnPtr & column = col_name_type.column;

		if (const auto col_in = typeid_cast<const ColumnFixedString *>(column.get()))
		{
			if (col_in->getN() != ipv6_fixed_string_length)
				throw Exception("Illegal type " + col_name_type.type->getName() +
								" of column " + col_in->getName() +
								" argument of function " + getName() +
								", expected FixedString(" + toString(ipv6_fixed_string_length) + ")",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto size = col_in->size();
			const auto & vec_in = col_in->getChars();

			auto col_res = new ColumnString;
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & vec_res = col_res->getChars();
			ColumnString::Offsets_t & offsets_res = col_res->getOffsets();
			vec_res.resize(size * INET6_ADDRSTRLEN);
			offsets_res.resize(size);

			auto begin = reinterpret_cast<char *>(&vec_res[0]);
			auto pos = begin;

			for (size_t i = 0; i < vec_in.size(); i += ipv6_fixed_string_length)
			{
				inet_ntop(AF_INET6, &vec_in[i], pos, INET6_ADDRSTRLEN);
				pos = static_cast<char *>(memchr(pos, 0, INET6_ADDRSTRLEN)) + 1;
				offsets_res[i] = pos - begin;
			}

			vec_res.resize(pos - begin);
		}
		else if (const auto col_in = typeid_cast<const ColumnConst<String> *>(column.get()))
		{
			const auto data_type_fixed_string = typeid_cast<const DataTypeFixedString *>(col_in->getDataType().get());
			if (!data_type_fixed_string || data_type_fixed_string->getN() != ipv6_fixed_string_length)
				throw Exception("Illegal type " + col_name_type.type->getName() +
								" of column " + col_in->getName() +
								" argument of function " + getName() +
								", expected FixedString(" + toString(ipv6_fixed_string_length) + ")",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto & data_in = col_in->getData();

			char buf[INET6_ADDRSTRLEN];
			inet_ntop(AF_INET6, data_in.data(), buf, sizeof(buf));

			block.getByPosition(result).column = new ColumnConstString{col_in->size(), buf};
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
	}
};

class FunctionIPv4NumToString : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "IPv4NumToString";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeUInt32 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected UInt32",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	static void formatIP(UInt32 ip, char *& out)
	{
		char * begin = out;

		/// Запишем все задом наперед.
		for (size_t offset = 0; offset <= 24; offset += 8)
		{
			if (offset > 0)
				*(out++) = '.';

			/// Достаем очередной байт.
			UInt32 value = (ip >> offset) & static_cast<UInt32>(255);

			/// Быстрее, чем sprintf.
			if (value == 0)
			{
				*(out++) = '0';
			}
			else
			{
				while (value > 0)
				{
					*(out++) = '0' + value % 10;
					value /= 10;
				}
			}
		}

		/// И развернем.
		std::reverse(begin, out);

		*(out++) = '\0';
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnVector<UInt32> * col = typeid_cast<const ColumnVector<UInt32> *>(&*column))
		{
			const ColumnVector<UInt32>::Container_t & vec_in = col->getData();

			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & vec_res = col_res->getChars();
			ColumnString::Offsets_t & offsets_res = col_res->getOffsets();

			vec_res.resize(vec_in.size() * INET_ADDRSTRLEN); /// самое длинное значение: 255.255.255.255\0
			offsets_res.resize(vec_in.size());
			char * begin = reinterpret_cast<char *>(&vec_res[0]);
			char * pos = begin;

			for (size_t i = 0; i < vec_in.size(); ++i)
			{
				formatIP(vec_in[i], pos);
				offsets_res[i] = pos - begin;
			}

			vec_res.resize(pos - begin);
		}
		else if (const ColumnConst<UInt32> * col = typeid_cast<const ColumnConst<UInt32> *>(&*column))
		{
			char buf[16];
			char * pos = buf;
			formatIP(col->getData(), pos);

			ColumnConstString * col_res = new ColumnConstString(col->size(), buf);
			block.getByPosition(result).column = col_res;
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
	}
};

class FunctionIPv4StringToNum : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "IPv4StringToNum";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt32;
	}

	static inline bool isDigit(char c)
	{
		return c >= '0' && c <= '9';
	}

	static UInt32 parseIPv4(const char * pos)
	{
		UInt32 res = 0;
		for (int offset = 24; offset >= 0; offset -= 8)
		{
			UInt32 value = 0;
			size_t len = 0;
			while (isDigit(*pos) && len <= 3)
			{
				value = value * 10 + (*pos - '0');
				++len;
				++pos;
			}
			if (len == 0 || value > 255 || (offset > 0 && *pos != '.'))
				return 0;
			res |= value << offset;
			++pos;
		}
		if (*(pos - 1) != '\0')
			return 0;
		return res;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			ColumnVector<UInt32> * col_res = new ColumnVector<UInt32>;
			block.getByPosition(result).column = col_res;

			ColumnVector<UInt32>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());

			const ColumnString::Chars_t & vec_src = col->getChars();
			const ColumnString::Offsets_t & offsets_src = col->getOffsets();
			size_t prev_offset = 0;

			for (size_t i = 0; i < vec_res.size(); ++i)
			{
				vec_res[i] = parseIPv4(reinterpret_cast<const char *>(&vec_src[prev_offset]));
				prev_offset = offsets_src[i];
			}
		}
		else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
		{
			ColumnConst<UInt32> * col_res = new ColumnConst<UInt32>(col->size(), parseIPv4(col->getData().c_str()));
			block.getByPosition(result).column = col_res;
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
	}
};


class FunctionHex : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "hex";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeFixedString *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeDate *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeDateTime *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt8 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt16 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt32 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt64 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	template <typename T>
	void executeOneUInt(T x, char *& out)
	{
		const char digit[17] = "0123456789ABCDEF";
		bool was_nonzero = false;
		for (int offset = (sizeof(T) - 1) * 8; offset >= 0; offset -= 8)
		{
			UInt8 byte = static_cast<UInt8>((x >> offset) & 255);

			/// Ведущие нули.
			if (byte == 0 && !was_nonzero && offset)
				continue;

			was_nonzero = true;

			*(out++) = digit[byte >> 4];
			*(out++) = digit[byte & 15];
		}
		*(out++) = '\0';
	}

	template <typename T>
	bool tryExecuteUInt(const IColumn * col, ColumnPtr & col_res)
	{
		const ColumnVector<T> * col_vec = typeid_cast<const ColumnVector<T> *>(col);
		const ColumnConst<T> * col_const = typeid_cast<const ColumnConst<T> *>(col);

		if (col_vec)
		{
			ColumnString * col_str = new ColumnString;
			col_res = col_str;
			ColumnString::Chars_t & out_vec = col_str->getChars();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();

			const typename ColumnVector<T>::Container_t & in_vec = col_vec->getData();

			size_t size = in_vec.size();
			out_offsets.resize(size);
			out_vec.resize(size * 3 + MAX_UINT_HEX_LENGTH);

			size_t pos = 0;
			for (size_t i = 0; i < size; ++i)
			{
				/// Ручной экспоненциальный рост, чтобы не полагаться на линейное амортизированное время работы resize (его никто не гарантирует).
				if (pos + MAX_UINT_HEX_LENGTH > out_vec.size())
					out_vec.resize(out_vec.size() * 2 + MAX_UINT_HEX_LENGTH);

				char * begin = reinterpret_cast<char *>(&out_vec[pos]);
				char * end = begin;
				executeOneUInt<T>(in_vec[i], end);

				pos += end - begin;
				out_offsets[i] = pos;
			}

			out_vec.resize(pos);

			return true;
		}
		else if(col_const)
		{
			char buf[MAX_UINT_HEX_LENGTH];
			char * pos = buf;
			executeOneUInt<T>(col_const->getData(), pos);

			col_res = new ColumnConstString(col_const->size(), buf);

			return true;
		}
		else
		{
			return false;
		}
	}

	void executeOneString(const UInt8 * pos, const UInt8 * end, char *& out)
	{
		const char digit[17] = "0123456789ABCDEF";
		while (pos < end)
		{
			UInt8 byte = *(pos++);
			*(out++) = digit[byte >> 4];
			*(out++) = digit[byte & 15];
		}
		*(out++) = '\0';
	}

	bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
	{
		const ColumnString * col_str_in = typeid_cast<const ColumnString *>(col);
		const ColumnConstString * col_const_in = typeid_cast<const ColumnConstString *>(col);

		if (col_str_in)
		{
			ColumnString * col_str = new ColumnString;
			col_res = col_str;
			ColumnString::Chars_t & out_vec = col_str->getChars();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();

			const ColumnString::Chars_t & in_vec = col_str_in->getChars();
			const ColumnString::Offsets_t & in_offsets = col_str_in->getOffsets();

			size_t size = in_offsets.size();
			out_offsets.resize(size);
			out_vec.resize(in_vec.size() * 2 - size);

			char * begin = reinterpret_cast<char *>(&out_vec[0]);
			char * pos = begin;
			size_t prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				size_t new_offset = in_offsets[i];

				executeOneString(&in_vec[prev_offset], &in_vec[new_offset - 1], pos);

				out_offsets[i] = pos - begin;

				prev_offset = new_offset;
			}

			if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
				throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

			return true;
		}
		else if(col_const_in)
		{
			const std::string & src = col_const_in->getData();
			std::string res(src.size() * 2, '\0');
			char * pos = &res[0];
			const UInt8 * src_ptr = reinterpret_cast<const UInt8 *>(src.c_str());
			/// Запишем ноль в res[res.size()]. Начиная с C++11, это корректно.
			executeOneString(src_ptr, src_ptr + src.size(), pos);

			col_res = new ColumnConstString(col_const_in->size(), res);

			return true;
		}
		else
		{
			return false;
		}
	}

	bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
	{
		const ColumnFixedString * col_fstr_in = typeid_cast<const ColumnFixedString *>(col);

		if (col_fstr_in)
		{
			ColumnString * col_str = new ColumnString;

			col_res = col_str;

			ColumnString::Chars_t & out_vec = col_str->getChars();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();

			const ColumnString::Chars_t & in_vec = col_fstr_in->getChars();

			size_t size = col_fstr_in->size();

			out_offsets.resize(size);
			out_vec.resize(in_vec.size() * 2 + size);

			char * begin = reinterpret_cast<char *>(&out_vec[0]);
			char * pos = begin;

			size_t n = col_fstr_in->getN();

			size_t prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				size_t new_offset = prev_offset + n;

				executeOneString(&in_vec[prev_offset], &in_vec[new_offset], pos);

				out_offsets[i] = pos - begin;
				prev_offset = new_offset;
			}

			if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
				throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

			return true;
		}
		else
		{
			return false;
		}
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn * column = &*block.getByPosition(arguments[0]).column;
		ColumnPtr & res_column = block.getByPosition(result).column;

		if (tryExecuteUInt<UInt8>(column, res_column) ||
			tryExecuteUInt<UInt16>(column, res_column) ||
			tryExecuteUInt<UInt32>(column, res_column) ||
			tryExecuteUInt<UInt64>(column, res_column) ||
			tryExecuteString(column, res_column) ||
			tryExecuteFixedString(column, res_column))
			return;

		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
	}
};


class FunctionUnhex : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "unhex";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}

	UInt8 undigitUnsafe(char c)
	{
		if (c <= '9')
			return c - '0';
		if (c <= 'Z')
			return c - ('A' - 10);
		return c - ('a' - 10);
	}

	void unhexOne(const char * pos, const char * end, char *& out)
	{
		if ((end - pos) & 1)
		{
			*(out++) = undigitUnsafe(*(pos++));
		}
		while (pos < end)
		{
			UInt8 major = undigitUnsafe(*(pos++));
			UInt8 minor = undigitUnsafe(*(pos++));
			*(out++) = (major << 4) | minor;
		}
		*(out++) = '\0';
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & out_vec = col_res->getChars();
			ColumnString::Offsets_t & out_offsets = col_res->getOffsets();

			const ColumnString::Chars_t & in_vec = col->getChars();
			const ColumnString::Offsets_t & in_offsets = col->getOffsets();

			size_t size = in_offsets.size();
			out_offsets.resize(size);
			out_vec.resize(in_vec.size() / 2 + size);

			char * begin = reinterpret_cast<char *>(&out_vec[0]);
			char * pos = begin;
			size_t prev_offset = 0;

			for (size_t i = 0; i < size; ++i)
			{
				size_t new_offset = in_offsets[i];

				unhexOne(reinterpret_cast<const char *>(&in_vec[prev_offset]), reinterpret_cast<const char *>(&in_vec[new_offset - 1]), pos);

				out_offsets[i] = pos - begin;

				prev_offset = new_offset;
			}

			out_vec.resize(pos - begin);
		}
		else if(const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*column))
		{
			const std::string & src = col->getData();
			std::string res(src.size(), '\0');
			char * pos = &res[0];
			unhexOne(src.c_str(), src.c_str() + src.size(), pos);
			res = res.substr(0, pos - &res[0] - 1);

			block.getByPosition(result).column = new ColumnConstString(col->size(), res);
		}
		else
		{
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
							+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
		}
	}
};


class FunctionBitmaskToArray : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "bitmaskToArray";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeUInt8 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt16 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt32 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeUInt64 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeInt8 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeInt16 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeInt32 *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeInt64 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(arguments[0]);
	}

	template <typename T>
	bool tryExecute(const IColumn * column, ColumnPtr & out_column)
	{
		if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(column))
		{
			ColumnVector<T> * col_values = new ColumnVector<T>;
			ColumnArray * col_array = new ColumnArray(col_values);
			out_column = col_array;

			ColumnArray::Offsets_t & res_offsets = col_array->getOffsets();
			typename ColumnVector<T>::Container_t & res_values = col_values->getData();

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			size_t size = vec_from.size();
			res_offsets.resize(size);
			res_values.reserve(size * 2);

			for (size_t row = 0; row < size; ++row)
			{
				T x = vec_from[row];
				while (x)
				{
					T y = (x & (x - 1));
					T bit = x ^ y;
					x = y;
					res_values.push_back(bit);
				}
				res_offsets[row] = res_values.size();
			}

			return true;
		}
		else if (const ColumnConst<T> * col_from = typeid_cast<const ColumnConst<T> *>(column))
		{
			Array res;

			T x = col_from->getData();
			for (size_t i = 0; i < sizeof(T) * 8; ++i)
			{
				T bit = static_cast<T>(1) << i;
				if (x & bit)
				{
					res.push_back(static_cast<UInt64>(bit));
				}
			}

			out_column = new ColumnConstArray(col_from->size(), res, new DataTypeArray(new typename DataTypeFromFieldType<T>::Type));

			return true;
		}
		else
		{
			return false;
		}
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn * in_column = &*block.getByPosition(arguments[0]).column;
		ColumnPtr & out_column = block.getByPosition(result).column;

		if (tryExecute<UInt8>(in_column, out_column) ||
			tryExecute<UInt16>(in_column, out_column) ||
			tryExecute<UInt32>(in_column, out_column) ||
			tryExecute<UInt64>(in_column, out_column) ||
			tryExecute<Int8>(in_column, out_column) ||
			tryExecute<Int16>(in_column, out_column) ||
			tryExecute<Int32>(in_column, out_column) ||
			tryExecute<Int64>(in_column, out_column))
			return;

		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of first argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
	}
};

class FunctionToStringCutToZero : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "toStringCutToZero";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeFixedString *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeString;
	}


	bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
	{
		const ColumnString * col_str_in = typeid_cast<const ColumnString *>(col);
		const ColumnConstString * col_const_in = typeid_cast<const ColumnConstString *>(col);

		if (col_str_in)
		{
			ColumnString * col_str = new ColumnString;
			col_res = col_str;
			ColumnString::Chars_t & out_vec = col_str->getChars();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();

			const ColumnString::Chars_t & in_vec = col_str_in->getChars();
			const ColumnString::Offsets_t & in_offsets = col_str_in->getOffsets();

			size_t size = in_offsets.size();
			out_offsets.resize(size);
			out_vec.resize(in_vec.size());

			char * begin = reinterpret_cast<char *>(&out_vec[0]);
			char * pos = begin;
			const char * pos_in = reinterpret_cast<const char *>(&in_vec[0]);

			for (size_t i = 0; i < size; ++i)
			{
				size_t current_size = strlen(pos_in);
				memcpy(pos, pos_in, current_size);
				pos += current_size;
				*pos = '\0';
				out_offsets[i] = ++pos - begin;
				pos_in += in_offsets[i];
			}
			out_vec.resize(pos - begin);

			if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
				throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

			return true;
		}
		else if(col_const_in)
		{
			std::string res(col_const_in->getData().c_str());
			col_res = new ColumnConstString(col_const_in->size(), res);

			return true;
		}
		else
		{
			return false;
		}
	}

	bool tryExecuteFixedString(const IColumn * col, ColumnPtr & col_res)
	{
		const ColumnFixedString * col_fstr_in = typeid_cast<const ColumnFixedString *>(col);

		if (col_fstr_in)
		{
			ColumnString * col_str = new ColumnString;

			col_res = col_str;

			ColumnString::Chars_t & out_vec = col_str->getChars();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();

			const ColumnString::Chars_t & in_vec = col_fstr_in->getChars();

			size_t size = col_fstr_in->size();

			out_offsets.resize(size);
			out_vec.resize(in_vec.size() + size);

			char * begin = reinterpret_cast<char *>(&out_vec[0]);
			char * pos = begin;
			const char * pos_in = reinterpret_cast<const char *>(&in_vec[0]);

			size_t n = col_fstr_in->getN();

			for (size_t i = 0; i < size; ++i)
			{
				size_t current_size = strnlen(pos_in, n);
				memcpy(pos, pos_in, current_size);
				pos += current_size;
				*pos = '\0';
				out_offsets[i] = ++pos - begin;
				pos_in += n;
			}
			out_vec.resize(pos - begin);

			if (!out_offsets.empty() && out_offsets.back() != out_vec.size())
				throw Exception("Column size mismatch (internal logical error)", ErrorCodes::LOGICAL_ERROR);

			return true;
		}
		else
		{
			return false;
		}
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn * column = &*block.getByPosition(arguments[0]).column;
		ColumnPtr & res_column = block.getByPosition(result).column;

		if (tryExecuteFixedString(column, res_column) || tryExecuteString(column, res_column))
			return;

		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
	}
};

}
