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
#include <ext/range.hpp>
#include <array>


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

const auto ipv4_bytes_length = 4;
const auto ipv6_bytes_length = 16;

class IPv6Format
{
private:
	/// integer logarithm, return ceil(log(value, base)) (the smallest integer greater or equal  than log(value, base)
	static constexpr uint32_t int_log(const uint32_t value, const uint32_t base, const bool carry = false)
	{
		return value >= base ? 1 + int_log(value / base, base, value % base || carry) : value % base > 1 || carry;
	}

	/// mapping of digits up to base 16
	static constexpr auto && digits = "0123456789abcdef";

	/// print integer in desired base, faster than sprintf
	template <uint32_t base, typename T, uint32_t buffer_size = sizeof(T) * int_log(256, base, false)>
	static void print_integer(char *& out, T value)
	{
		if (value == 0)
			*out++ = '0';
		else
		{
			char buf[buffer_size];
			auto ptr = buf;

			while (value > 0)
			{
				*ptr++ = digits[value % base];
				value /= base;
			}

			while (ptr != buf)
				*out++ = *--ptr;
		}
	}

	/// print IPv4 address as %u.%u.%u.%u
	static void ipv4_format(const unsigned char * src, char *& dst, UInt8 zeroed_tail_bytes_count)
	{
		const auto limit = ipv4_bytes_length - zeroed_tail_bytes_count;

		for (const auto i : ext::range(0, ipv4_bytes_length))
		{
			UInt8 byte = (i < limit) ? src[i] : 0;
			print_integer<10, UInt8>(dst, byte);

			if (i != ipv4_bytes_length - 1)
				*dst++ = '.';
		}
	}

public:
	/** rewritten inet_ntop6 from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
	 *	performs significantly faster than the reference implementation due to the absence of sprintf calls,
	 *	bounds checking, unnecessary string copying and length calculation */
	static const void apply(const unsigned char * src, char *& dst, UInt8 zeroed_tail_bytes_count = 0)
	{
		struct { int base, len; } best{-1}, cur{-1};
		std::array<uint16_t, ipv6_bytes_length / sizeof(uint16_t)> words{};

		/** Preprocess:
		 *	Copy the input (bytewise) array into a wordwise array.
		 *	Find the longest run of 0x00's in src[] for :: shorthanding. */
		for (const auto i : ext::range(0, ipv6_bytes_length - zeroed_tail_bytes_count))
			words[i / 2] |= src[i] << ((1 - (i % 2)) << 3);

		for (const auto i : ext::range(0, words.size()))
		{
			if (words[i] == 0) {
				if (cur.base == -1)
					cur.base = i, cur.len = 1;
				else
					cur.len++;
			}
			else
			{
				if (cur.base != -1)
				{
					if (best.base == -1 || cur.len > best.len)
						best = cur;
					cur.base = -1;
				}
			}
		}

		if (cur.base != -1)
		{
			if (best.base == -1 || cur.len > best.len)
				best = cur;
		}

		if (best.base != -1 && best.len < 2)
			best.base = -1;

		/// Format the result.
		for (const int i : ext::range(0, words.size()))
		{
			/// Are we inside the best run of 0x00's?
			if (best.base != -1 && i >= best.base && i < (best.base + best.len))
			{
				if (i == best.base)
					*dst++ = ':';
				continue;
			}

			/// Are we following an initial run of 0x00s or any real hex?
			if (i != 0)
				*dst++ = ':';

			/// Is this address an encapsulated IPv4?
			if (i == 6 && best.base == 0 && (best.len == 6 || (best.len == 5 && words[5] == 0xffffu)))
			{
				ipv4_format(src + 12, dst, std::min(zeroed_tail_bytes_count, static_cast<UInt8>(ipv4_bytes_length)));
				break;
			}

			print_integer<16>(dst, words[i]);
		}

		/// Was it a trailing run of 0x00's?
		if (best.base != -1 && (best.base + best.len) == words.size())
			*dst++ = ':';

		*dst++ = '\0';
	}
};

class FunctionIPv6NumToString : public IFunction
{
public:
	static constexpr auto name = "IPv6NumToString";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIPv6NumToString>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const auto ptr = typeid_cast<const DataTypeFixedString *>(arguments[0].get());
		if (!ptr || ptr->getN() != ipv6_bytes_length)
			throw Exception("Illegal type " + arguments[0]->getName() +
							" of argument of function " + getName() +
							", expected FixedString(" + toString(ipv6_bytes_length) + ")",
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto & col_name_type = block.getByPosition(arguments[0]);
		const ColumnPtr & column = col_name_type.column;

		if (const auto col_in = typeid_cast<const ColumnFixedString *>(column.get()))
		{
			if (col_in->getN() != ipv6_bytes_length)
				throw Exception("Illegal type " + col_name_type.type->getName() +
								" of column " + col_in->getName() +
								" argument of function " + getName() +
								", expected FixedString(" + toString(ipv6_bytes_length) + ")",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto size = col_in->size();
			const auto & vec_in = col_in->getChars();

			auto col_res = std::make_shared<ColumnString>();
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & vec_res = col_res->getChars();
			ColumnString::Offsets_t & offsets_res = col_res->getOffsets();
			vec_res.resize(size * INET6_ADDRSTRLEN);
			offsets_res.resize(size);

			auto begin = reinterpret_cast<char *>(&vec_res[0]);
			auto pos = begin;

			for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += ipv6_bytes_length, ++i)
			{
				IPv6Format::apply(&vec_in[offset], pos);
				offsets_res[i] = pos - begin;
			}

			vec_res.resize(pos - begin);
		}
		else if (const auto col_in = typeid_cast<const ColumnConst<String> *>(column.get()))
		{
			const auto data_type_fixed_string = typeid_cast<const DataTypeFixedString *>(col_in->getDataType().get());
			if (!data_type_fixed_string || data_type_fixed_string->getN() != ipv6_bytes_length)
				throw Exception("Illegal type " + col_name_type.type->getName() +
								" of column " + col_in->getName() +
								" argument of function " + getName() +
								", expected FixedString(" + toString(ipv6_bytes_length) + ")",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto & data_in = col_in->getData();

			char buf[INET6_ADDRSTRLEN];
			char * dst = buf;
			IPv6Format::apply(reinterpret_cast<const unsigned char *>(data_in.data()), dst);

			block.getByPosition(result).column = std::make_shared<ColumnConstString>(col_in->size(), buf);
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
	}
};

class FunctionCutIPv6 : public IFunction
{
public:
	static constexpr auto name = "cutIPv6";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionCutIPv6>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 3.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const auto ptr = typeid_cast<const DataTypeFixedString *>(arguments[0].get());
		if (!ptr || ptr->getN() != ipv6_bytes_length)
			throw Exception("Illegal type " + arguments[0]->getName() +
							" of argument 1 of function " + getName() +
							", expected FixedString(" + toString(ipv6_bytes_length) + ")",
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeUInt8 *>(arguments[1].get()))
			throw Exception("Illegal type " + arguments[1]->getName() +
							" of argument 2 of function " + getName(),
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeUInt8 *>(arguments[2].get()))
			throw Exception("Illegal type " + arguments[2]->getName() +
							" of argument 3 of function " + getName(),
							ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto & col_name_type = block.getByPosition(arguments[0]);
		const ColumnPtr & column = col_name_type.column;

		const auto & col_ipv6_zeroed_tail_bytes_type = block.getByPosition(arguments[1]);
		const auto & col_ipv6_zeroed_tail_bytes = col_ipv6_zeroed_tail_bytes_type.column;
		const auto & col_ipv4_zeroed_tail_bytes_type = block.getByPosition(arguments[2]);
		const auto & col_ipv4_zeroed_tail_bytes = col_ipv4_zeroed_tail_bytes_type.column;

		if (const auto col_in = typeid_cast<const ColumnFixedString *>(column.get()))
		{
			if (col_in->getN() != ipv6_bytes_length)
				throw Exception("Illegal type " + col_name_type.type->getName() +
								" of column " + col_in->getName() +
								" argument of function " + getName() +
								", expected FixedString(" + toString(ipv6_bytes_length) + ")",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto ipv6_zeroed_tail_bytes = typeid_cast<const ColumnConst<UInt8> *>(col_ipv6_zeroed_tail_bytes.get());
			if (!ipv6_zeroed_tail_bytes)
				throw Exception("Illegal type " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
								" of argument 2 of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			UInt8 ipv6_zeroed_tail_bytes_count = ipv6_zeroed_tail_bytes->getData();
			if (ipv6_zeroed_tail_bytes_count > ipv6_bytes_length)
				throw Exception("Illegal value for argument 2 " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
								" of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto ipv4_zeroed_tail_bytes = typeid_cast<const ColumnConst<UInt8> *>(col_ipv4_zeroed_tail_bytes.get());
			if (!ipv4_zeroed_tail_bytes)
				throw Exception("Illegal type " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
								" of argument 3 of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			UInt8 ipv4_zeroed_tail_bytes_count = ipv4_zeroed_tail_bytes->getData();
			if (ipv4_zeroed_tail_bytes_count > ipv6_bytes_length)
				throw Exception("Illegal value for argument 3 " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
								" of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto size = col_in->size();
			const auto & vec_in = col_in->getChars();

			auto col_res = std::make_shared<ColumnString>();
			block.getByPosition(result).column = col_res;

			ColumnString::Chars_t & vec_res = col_res->getChars();
			ColumnString::Offsets_t & offsets_res = col_res->getOffsets();
			vec_res.resize(size * INET6_ADDRSTRLEN);
			offsets_res.resize(size);

			auto begin = reinterpret_cast<char *>(&vec_res[0]);
			auto pos = begin;

			for (size_t offset = 0, i = 0; offset < vec_in.size(); offset += ipv6_bytes_length, ++i)
			{
				const auto address = &vec_in[offset];
				UInt8 zeroed_tail_bytes_count = isIPv4Mapped(address) ? ipv4_zeroed_tail_bytes_count : ipv6_zeroed_tail_bytes_count;
				cutAddress(address, pos, zeroed_tail_bytes_count);
				offsets_res[i] = pos - begin;
			}

			vec_res.resize(pos - begin);
		}
		else if (const auto col_in = typeid_cast<const ColumnConst<String> *>(column.get()))
		{
			const auto data_type_fixed_string = typeid_cast<const DataTypeFixedString *>(col_in->getDataType().get());
			if (!data_type_fixed_string || data_type_fixed_string->getN() != ipv6_bytes_length)
				throw Exception("Illegal type " + col_name_type.type->getName() +
								" of column " + col_in->getName() +
								" argument of function " + getName() +
								", expected FixedString(" + toString(ipv6_bytes_length) + ")",
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto ipv6_zeroed_tail_bytes = typeid_cast<const ColumnConst<UInt8> *>(col_ipv6_zeroed_tail_bytes.get());
			if (!ipv6_zeroed_tail_bytes)
				throw Exception("Illegal type " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
								" of argument 2 of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			UInt8 ipv6_zeroed_tail_bytes_count = ipv6_zeroed_tail_bytes->getData();
			if (ipv6_zeroed_tail_bytes_count > ipv6_bytes_length)
				throw Exception("Illegal value for argument 2 " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
								" of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto ipv4_zeroed_tail_bytes = typeid_cast<const ColumnConst<UInt8> *>(col_ipv4_zeroed_tail_bytes.get());
			if (!ipv4_zeroed_tail_bytes)
				throw Exception("Illegal type " + col_ipv4_zeroed_tail_bytes_type.type->getName() +
								" of argument 3 of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			UInt8 ipv4_zeroed_tail_bytes_count = ipv4_zeroed_tail_bytes->getData();
			if (ipv4_zeroed_tail_bytes_count > ipv6_bytes_length)
				throw Exception("Illegal value for argument 3 " + col_ipv6_zeroed_tail_bytes_type.type->getName() +
								" of function " + getName(),
								ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			const auto & data_in = col_in->getData();

			char buf[INET6_ADDRSTRLEN];
			char * dst = buf;

			const auto address = reinterpret_cast<const unsigned char *>(data_in.data());
			UInt8 zeroed_tail_bytes_count = isIPv4Mapped(address) ? ipv4_zeroed_tail_bytes_count : ipv6_zeroed_tail_bytes_count;
			cutAddress(address, dst, zeroed_tail_bytes_count);

			block.getByPosition(result).column = std::make_shared<ColumnConstString>(col_in->size(), buf);
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	bool isIPv4Mapped(const unsigned char * address) const
	{
		return (*reinterpret_cast<const UInt64 *>(&address[0]) == 0) &&
			((*reinterpret_cast<const UInt64 *>(&address[8]) & 0x00000000FFFFFFFFull) == 0x00000000FFFF0000ull);
	}

	void cutAddress(const unsigned char * address, char *& dst, UInt8 zeroed_tail_bytes_count)
	{
		IPv6Format::apply(address, dst, zeroed_tail_bytes_count);
	}
};

class FunctionIPv6StringToNum : public IFunction
{
public:
	static constexpr auto name = "IPv6StringToNum";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIPv6StringToNum>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeFixedString>(ipv6_bytes_length);
	}


	static bool ipv4_scan(const char * src, unsigned char * dst)
	{
		constexpr auto size = sizeof(UInt32);
		char bytes[size]{};

		for (const auto i : ext::range(0, size))
		{
			UInt32 value = 0;
			size_t len = 0;
			while (isNumericASCII(*src) && len <= 3)
			{
				value = value * 10 + (*src - '0');
				++len;
				++src;
			}

			if (len == 0 || value > 255 || (i < size - 1 && *src != '.'))
			{
				memset(dst, 0, size);
				return false;
			}
			bytes[i] = value;
			++src;
		}

		if (src[-1] != '\0')
		{
			memset(dst, 0, size);
			return false;
		}

		memcpy(dst, bytes, sizeof(bytes));
		return true;
	}

	/// slightly altered implementation from http://svn.apache.org/repos/asf/apr/apr/trunk/network_io/unix/inet_pton.c
	static void ipv6_scan(const char *  src, unsigned char * dst)
	{
		const auto clear_dst = [dst] {
			memset(dst, '\0', ipv6_bytes_length);
		};

		/// Leading :: requires some special handling.
		if (*src == ':')
			if (*++src != ':')
				return clear_dst();

		/// get integer value for a hexademical char digit, or -1
		const auto number_by_char = [] (const char ch) {
			if ('A' <= ch && ch <= 'F')
				return 10 + ch - 'A';

			if ('a' <= ch && ch <= 'f')
				return 10 + ch - 'a';

			if ('0' <= ch && ch <= '9')
				return ch - '0';

			return -1;
		};

		unsigned char tmp[ipv6_bytes_length]{};
		auto tp = tmp;
		auto endp = tp + ipv6_bytes_length;
		auto curtok = src;
		auto saw_xdigit = false;
		uint16_t val{};
		unsigned char * colonp = nullptr;

		while (const auto ch = *src++)
		{
			const auto num = number_by_char(ch);

			if (num != -1)
			{
				val <<= 4;
				val |= num;
				if (val > 0xffffu)
					return clear_dst();

				saw_xdigit = 1;
				continue;
			}

			if (ch == ':')
			{
				curtok = src;
				if (!saw_xdigit)
				{
					if (colonp)
						return clear_dst();

					colonp = tp;
					continue;
				}

				if (tp + sizeof(uint16_t) > endp)
					return clear_dst();

				*tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
				*tp++ = static_cast<unsigned char>(val & 0xffu);
				saw_xdigit = false;
				val = 0;
				continue;
			}

			if (ch == '.' && (tp + ipv4_bytes_length) <= endp)
			{
				if (!ipv4_scan(curtok, tp))
					return clear_dst();

				tp += ipv4_bytes_length;
				saw_xdigit = false;
				break;	/* '\0' was seen by ipv4_scan(). */
			}

			return clear_dst();
		}

		if (saw_xdigit)
		{
			if (tp + sizeof(uint16_t) > endp)
				return clear_dst();

			*tp++ = static_cast<unsigned char>((val >> 8) & 0xffu);
			*tp++ = static_cast<unsigned char>(val & 0xffu);
		}

		if (colonp)
		{
			/*
			 * Since some memmove()'s erroneously fail to handle
			 * overlapping regions, we'll do the shift by hand.
			 */
			const auto n = tp - colonp;

			for (int i = 1; i <= n; i++)
			{
				endp[- i] = colonp[n - i];
				colonp[n - i] = 0;
			}
			tp = endp;
		}

		if (tp != endp)
			return clear_dst();

		memcpy(dst, tmp, sizeof(tmp));
	}

	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnPtr & column = block.getByPosition(arguments[0]).column;

		if (const auto col_in = typeid_cast<const ColumnString *>(&*column))
		{
		    const auto col_res = std::make_shared<ColumnFixedString>(ipv6_bytes_length);
			block.getByPosition(result).column = col_res;

			auto & vec_res = col_res->getChars();
			vec_res.resize(col_in->size() * ipv6_bytes_length);

			const ColumnString::Chars_t & vec_src = col_in->getChars();
			const ColumnString::Offsets_t & offsets_src = col_in->getOffsets();
			size_t src_offset = 0;

			for (size_t out_offset = 0, i = 0;
				 out_offset < vec_res.size();
				 out_offset += ipv6_bytes_length, ++i)
			{
				ipv6_scan(reinterpret_cast<const char* >(&vec_src[src_offset]), &vec_res[out_offset]);
				src_offset = offsets_src[i];
			}
		}
		else if (const auto col_in = typeid_cast<const ColumnConstString *>(&*column))
		{
			String out(ipv6_bytes_length, 0);
			ipv6_scan(col_in->getData().data(), reinterpret_cast<unsigned char *>(&out[0]));

			block.getByPosition(result).column = std::make_shared<ColumnConst<String>>(
				col_in->size(),
				out,
				std::make_shared<DataTypeFixedString>(ipv6_bytes_length));
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
	static constexpr auto name = "IPv4NumToString";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIPv4NumToString>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeUInt32 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected UInt32",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnUInt32 * col = typeid_cast<const ColumnUInt32 *>(&*column))
		{
			const ColumnUInt32::Container_t & vec_in = col->getData();

			std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
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

			auto col_res = std::make_shared<ColumnConstString>(col->size(), buf);
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
	static constexpr auto name = "IPv4StringToNum";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIPv4StringToNum>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeUInt32>();
	}

	static UInt32 parseIPv4(const char * pos)
	{
		UInt32 res = 0;
		for (int offset = 24; offset >= 0; offset -= 8)
		{
			UInt32 value = 0;
			size_t len = 0;
			while (isNumericASCII(*pos) && len <= 3)
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			auto col_res = std::make_shared<ColumnUInt32>();
			block.getByPosition(result).column = col_res;

			ColumnUInt32::Container_t & vec_res = col_res->getData();
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
			auto col_res = std::make_shared<ColumnConst<UInt32>>(col->size(), parseIPv4(col->getData().c_str()));
			block.getByPosition(result).column = col_res;
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
							ErrorCodes::ILLEGAL_COLUMN);
	}
};


class FunctionIPv4NumToStringClassC : public IFunction
{
public:
	static constexpr auto name = "IPv4NumToStringClassC";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIPv4NumToStringClassC>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeUInt32 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName() + ", expected UInt32",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	static void formatIP(UInt32 ip, char *& out)
	{
		char * begin = out;

		for (auto i = 0; i < 3; ++i)
			*(out++) = 'x';

		/// Запишем все задом наперед.
		for (size_t offset = 8; offset <= 24; offset += 8)
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnUInt32 * col = typeid_cast<const ColumnUInt32 *>(&*column))
		{
			const ColumnUInt32::Container_t & vec_in = col->getData();

			std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
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

			auto col_res = std::make_shared<ColumnConstString>(col->size(), buf);
			block.getByPosition(result).column = col_res;
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
	}
};

class FunctionIPv4ToIPv6 : public IFunction
{
public:
 	static constexpr auto name = "IPv4ToIPv6";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionIPv4ToIPv6>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (typeid_cast<const DataTypeUInt32 *>(arguments[0].get()) == nullptr)
			throw Exception("Illegal type " + arguments[0]->getName() +
							" of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeFixedString>(16);
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto & col_name_type = block.getByPosition(arguments[0]);
		const ColumnPtr & column = col_name_type.column;

		if (const auto col_in = typeid_cast<const ColumnUInt32 *>(column.get()))
		{
			const auto col_res = std::make_shared<ColumnFixedString>(ipv6_bytes_length);
			block.getByPosition(result).column = col_res;

			auto & vec_res = col_res->getChars();
			vec_res.resize(col_in->size() * ipv6_bytes_length);

			const auto & vec_in = col_in->getData();

			for (size_t out_offset = 0, i = 0; out_offset < vec_res.size(); out_offset += ipv6_bytes_length, ++i)
				mapIPv4ToIPv6(vec_in[i], &vec_res[out_offset]);
		}
		else if (const auto col_in = typeid_cast<const ColumnConst<UInt32> *>(column.get()))
		{
			std::string buf;
			buf.resize(ipv6_bytes_length);
			mapIPv4ToIPv6(col_in->getData(), reinterpret_cast<unsigned char *>(&buf[0]));

			auto col_res = std::make_shared<ColumnConstString>(
				ipv6_bytes_length, buf,
				std::make_shared<DataTypeFixedString>(ipv6_bytes_length));
			block.getByPosition(result).column = col_res;
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
	}

private:
	void mapIPv4ToIPv6(UInt32 in, unsigned char * buf) const
	{
		*reinterpret_cast<UInt64 *>(&buf[0]) = 0;
		*reinterpret_cast<UInt64 *>(&buf[8]) = 0x00000000FFFF0000ull | (static_cast<UInt64>(ntohl(in)) << 32);
	}
};

class FunctionHex : public IFunction
{
public:
	static constexpr auto name = "hex";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionHex>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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

		return std::make_shared<DataTypeString>();
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
			auto col_str = std::make_shared<ColumnString>();
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

			col_res = std::make_shared<ColumnConstString>(col_const->size(), buf);

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
			auto col_str = std::make_shared<ColumnString>();
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

			col_res = std::make_shared<ColumnConstString>(col_const_in->size(), res);

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
			auto col_str = std::make_shared<ColumnString>();

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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const IColumn * column = block.getByPosition(arguments[0]).column.get();
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
	static constexpr auto name = "unhex";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionUnhex>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const ColumnPtr column = block.getByPosition(arguments[0]).column;

		if (const ColumnString * col = typeid_cast<const ColumnString *>(&*column))
		{
			std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
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

			block.getByPosition(result).column = std::make_shared<ColumnConstString>(col->size(), res);
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
	static constexpr auto name = "bitmaskToArray";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionBitmaskToArray>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
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

		return std::make_shared<DataTypeArray>(arguments[0]);
	}

	template <typename T>
	bool tryExecute(const IColumn * column, ColumnPtr & out_column)
	{
		if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(column))
		{
			auto col_values = std::make_shared<ColumnVector<T>>();
			auto col_array = std::make_shared<ColumnArray>(col_values);
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

			out_column = std::make_shared<ColumnConstArray>(
				col_from->size(), res, std::make_shared<DataTypeArray>(std::make_shared<typename DataTypeFromFieldType<T>::Type>()));

			return true;
		}
		else
		{
			return false;
		}
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const IColumn * in_column = block.getByPosition(arguments[0]).column.get();
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
	static constexpr auto name = "toStringCutToZero";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionToStringCutToZero>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
			+ toString(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeFixedString *>(&*arguments[0]) &&
			!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}


	bool tryExecuteString(const IColumn * col, ColumnPtr & col_res)
	{
		const ColumnString * col_str_in = typeid_cast<const ColumnString *>(col);
		const ColumnConstString * col_const_in = typeid_cast<const ColumnConstString *>(col);

		if (col_str_in)
		{
			auto col_str = std::make_shared<ColumnString>();
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
				memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
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
			col_res = std::make_shared<ColumnConstString>(col_const_in->size(), res);

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
			auto col_str = std::make_shared<ColumnString>();
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
				memcpySmallAllowReadWriteOverflow15(pos, pos_in, current_size);
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		const IColumn * column = block.getByPosition(arguments[0]).column.get();
		ColumnPtr & res_column = block.getByPosition(result).column;

		if (tryExecuteFixedString(column, res_column) || tryExecuteString(column, res_column))
			return;

		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
	}
};

namespace
{
	template <typename T1, typename T2> UInt8 bitTest(const T1 val, const T2 pos) { return (val >> pos) & 1; };
}

class FunctionBitTest : public IFunction
{
public:
	static constexpr auto name = "bitTest";
	static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitTest>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() != 2)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
			};

		const auto first_arg = arguments.front().get();
		if (!typeid_cast<const DataTypeUInt8 *>(first_arg) &&
			!typeid_cast<const DataTypeUInt16 *>(first_arg) &&
			!typeid_cast<const DataTypeUInt32 *>(first_arg) &&
			!typeid_cast<const DataTypeUInt64 *>(first_arg) &&
			!typeid_cast<const DataTypeInt8 *>(first_arg) &&
			!typeid_cast<const DataTypeInt16 *>(first_arg) &&
			!typeid_cast<const DataTypeInt32 *>(first_arg) &&
			!typeid_cast<const DataTypeInt64 *>(first_arg))
			throw Exception{
				"Illegal type " + first_arg->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};


		const auto second_arg = arguments.back().get();
		if (!typeid_cast<const DataTypeUInt8 *>(second_arg) &&
			!typeid_cast<const DataTypeUInt16 *>(second_arg) &&
			!typeid_cast<const DataTypeUInt32 *>(second_arg) &&
			!typeid_cast<const DataTypeUInt64 *>(second_arg))
			throw Exception{
				"Illegal type " + second_arg->getName() + " of second argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};

		return std::make_shared<DataTypeUInt8>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto value_col = block.getByPosition(arguments.front()).column.get();

		if (!execute<UInt8>(block, arguments, result, value_col) &&
			!execute<UInt16>(block, arguments, result, value_col) &&
			!execute<UInt32>(block, arguments, result, value_col) &&
			!execute<UInt64>(block, arguments, result, value_col) &&
			!execute<Int8>(block, arguments, result, value_col) &&
			!execute<Int16>(block, arguments, result, value_col) &&
			!execute<Int32>(block, arguments, result, value_col) &&
			!execute<Int64>(block, arguments, result, value_col))
			throw Exception{
				"Illegal column " + value_col->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
	}

private:
	template <typename T> bool execute(
		Block & block, const ColumnNumbers & arguments, const size_t result,
		const IColumn * const value_col_untyped)
	{
		if (const auto value_col = typeid_cast<const ColumnVector<T> *>(value_col_untyped))
		{
			const auto pos_col = block.getByPosition(arguments.back()).column.get();

			if (!execute(block, arguments, result, value_col, pos_col))
				throw Exception{
					"Illegal column " + pos_col->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN
				};

			return true;
		}
		else if (const auto value_col = typeid_cast<const ColumnConst<T> *>(value_col_untyped))
		{
			const auto pos_col = block.getByPosition(arguments.back()).column.get();

			if (!execute(block, arguments, result, value_col, pos_col))
				throw Exception{
					"Illegal column " + pos_col->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN
				};

			return true;
		}

		return false;
	}

	template <typename ValueColumn> bool execute(
		Block & block, const ColumnNumbers & arguments, const size_t result,
		const ValueColumn * const value_col, const IColumn * const pos_col)
	{
		return execute<UInt8>(block, arguments, result, value_col, pos_col) ||
			   execute<UInt16>(block, arguments, result, value_col, pos_col) ||
			   execute<UInt32>(block, arguments, result, value_col, pos_col) ||
			   execute<UInt64>(block, arguments, result, value_col, pos_col);
	}

	template <typename T, typename ValueType> bool execute(
		Block & block, const ColumnNumbers & arguments, const size_t result,
		const ColumnVector<ValueType> * const value_col, const IColumn * const pos_col_untyped)
	{
		if (const auto pos_col = typeid_cast<const ColumnVector<T> *>(pos_col_untyped))
		{
			const auto & values = value_col->getData();
			const auto & positions = pos_col->getData();

			const auto size = value_col->size();
			const auto out_col = std::make_shared<ColumnUInt8>(size);
			ColumnPtr out_col_ptr{out_col};
			block.getByPosition(result).column = out_col_ptr;

			auto & out = out_col->getData();

			for (const auto i : ext::range(0, size))
				out[i] = bitTest(values[i], positions[i]);

			return true;
		}
		else if (const auto pos_col = typeid_cast<const ColumnConst<T> *>(pos_col_untyped))
		{
			const auto & values = value_col->getData();

			const auto size = value_col->size();
			const auto out_col = std::make_shared<ColumnUInt8>(size);
			ColumnPtr out_col_ptr{out_col};
			block.getByPosition(result).column = out_col_ptr;

			auto & out = out_col->getData();

			for (const auto i : ext::range(0, size))
				out[i] = bitTest(values[i], pos_col->getData());

			return true;
		}

		return false;
	}

	template <typename T, typename ValueType> bool execute(
		Block & block, const ColumnNumbers & arguments, const size_t result,
		const ColumnConst<ValueType> * const value_col, const IColumn * const pos_col_untyped)
	{
		if (const auto pos_col = typeid_cast<const ColumnVector<T> *>(pos_col_untyped))
		{
			const auto & positions = pos_col->getData();

			const auto size = value_col->size();
			const auto out_col = std::make_shared<ColumnUInt8>(size);
			ColumnPtr out_col_ptr{out_col};
			block.getByPosition(result).column = out_col_ptr;

			auto & out = out_col->getData();

			for (const auto i : ext::range(0, size))
				out[i] = bitTest(value_col->getData(), positions[i]);

			return true;
		}
		else if (const auto pos_col = typeid_cast<const ColumnConst<T> *>(pos_col_untyped))
		{
			block.getByPosition(result).column = std::make_shared<ColumnConst<UInt8>>(
				value_col->size(),
				bitTest(value_col->getData(), pos_col->getData()));

			return true;
		}

		return false;
	}
};

template <typename Impl>
struct FunctionBitTestMany : public IFunction
{
public:
	static constexpr auto name = Impl::name;
	static FunctionPtr create(const Context &) { return std::make_shared<FunctionBitTestMany>(); }

	String getName() const override { return name; }

	DataTypePtr getReturnType(const DataTypes & arguments) const override
	{
		if (arguments.size() < 2)
			throw Exception{
				"Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be at least 2.",
				ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION
			};

		const auto first_arg = arguments.front().get();
		if (!typeid_cast<const DataTypeUInt8 *>(first_arg) && !typeid_cast<const DataTypeUInt16 *>(first_arg) &&
			!typeid_cast<const DataTypeUInt32 *>(first_arg) && !typeid_cast<const DataTypeUInt64 *>(first_arg) &&
			!typeid_cast<const DataTypeInt8 *>(first_arg) && !typeid_cast<const DataTypeInt16 *>(first_arg) &&
			!typeid_cast<const DataTypeInt32 *>(first_arg) && !typeid_cast<const DataTypeInt64 *>(first_arg))
			throw Exception{
				"Illegal type " + first_arg->getName() + " of first argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
			};


		for (const auto i : ext::range(1, arguments.size()))
		{
			const auto pos_arg = arguments[i].get();

			if (!typeid_cast<const DataTypeUInt8 *>(pos_arg) && !typeid_cast<const DataTypeUInt16 *>(pos_arg) &&
				!typeid_cast<const DataTypeUInt32 *>(pos_arg) && !typeid_cast<const DataTypeUInt64 *>(pos_arg))
				throw Exception{
					"Illegal type " + pos_arg->getName() + " of " + toString(i) + " argument of function " + getName(),
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
				};
		}

		return std::make_shared<DataTypeUInt8>();
	}

	void execute(Block & block, const ColumnNumbers & arguments, const size_t result) override
	{
		const auto value_col = block.getByPosition(arguments.front()).column.get();

		if (!execute<UInt8>(block, arguments, result, value_col) &&
			!execute<UInt16>(block, arguments, result, value_col) &&
			!execute<UInt32>(block, arguments, result, value_col) &&
			!execute<UInt64>(block, arguments, result, value_col) &&
			!execute<Int8>(block, arguments, result, value_col) &&
			!execute<Int16>(block, arguments, result, value_col) &&
			!execute<Int32>(block, arguments, result, value_col) &&
			!execute<Int64>(block, arguments, result, value_col))
			throw Exception{
				"Illegal column " + value_col->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN
			};
	}

private:
	template <typename T> bool execute(
		Block & block, const ColumnNumbers & arguments, const size_t result,
		const IColumn * const value_col_untyped)
	{
		if (const auto value_col = typeid_cast<const ColumnVector<T> *>(value_col_untyped))
		{
			const auto size = value_col->size();
			bool is_const;
			const auto mask = createConstMask<T>(size, block, arguments, is_const);
			const auto & val = value_col->getData();

			const auto out_col = std::make_shared<ColumnUInt8>(size);
			ColumnPtr out_col_ptr{out_col};
			block.getByPosition(result).column = out_col_ptr;

			auto & out = out_col->getData();

			if (is_const)
			{
				for (const auto i : ext::range(0, size))
					out[i] = Impl::combine(val[i], mask);
			}
			else
			{
				const auto mask = createMask<T>(size, block, arguments);

				for (const auto i : ext::range(0, size))
					out[i] = Impl::combine(val[i], mask[i]);
			}

			return true;
		}
		else if (const auto value_col = typeid_cast<const ColumnConst<T> *>(value_col_untyped))
		{
			const auto size = value_col->size();
			bool is_const;
			const auto mask = createConstMask<T>(size, block, arguments, is_const);
			const auto val = value_col->getData();

			if (is_const)
			{
				block.getByPosition(result).column = std::make_shared<ColumnConst<UInt8>>(
					size, Impl::combine(val, mask));
			}
			else
			{
				const auto mask = createMask<T>(size, block, arguments);
				const auto out_col = std::make_shared<ColumnUInt8>(size);
				ColumnPtr out_col_ptr{out_col};
				block.getByPosition(result).column = out_col_ptr;

				auto & out = out_col->getData();

				for (const auto i : ext::range(0, size))
					out[i] = Impl::combine(val, mask[i]);
			}

			return true;
		}

		return false;
	}

	template <typename ValueType>
	ValueType createConstMask(const std::size_t size, const Block & block, const ColumnNumbers & arguments, bool & is_const)
	{
		is_const = true;
		ValueType mask{};

		for (const auto i : ext::range(1, arguments.size()))
		{
			const auto pos_col = block.getByPosition(arguments[i]).column.get();

			if (pos_col->isConst())
			{
				const auto pos = static_cast<const ColumnConst<ValueType> *>(pos_col)->getData();
				mask = mask | 1 << pos;
			}
			else
			{
				is_const = false;
				return {};
			}
		}

		return mask;
	}

	template <typename ValueType>
	PaddedPODArray<ValueType> createMask(const std::size_t size, const Block & block, const ColumnNumbers & arguments)
	{
		PaddedPODArray<ValueType> mask(size, ValueType{});

		for (const auto i : ext::range(1, arguments.size()))
		{
			const auto pos_col = block.getByPosition(arguments[i]).column.get();

			if (!addToMaskImpl<UInt8>(mask, pos_col) && !addToMaskImpl<UInt16>(mask, pos_col) &&
				!addToMaskImpl<UInt32>(mask, pos_col) && !addToMaskImpl<UInt64>(mask, pos_col))
				throw Exception{
					"Illegal column " + pos_col->getName() + " of argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN
				};
		}

		return mask;
	}

	template <typename PosType, typename ValueType>
	bool addToMaskImpl(PaddedPODArray<ValueType> & mask, const IColumn * const pos_col_untyped)
	{
		if (const auto pos_col = typeid_cast<const ColumnVector<PosType> *>(pos_col_untyped))
		{
			const auto & pos = pos_col->getData();

			for (const auto i : ext::range(0, mask.size()))
				mask[i] = mask[i] | (1 << pos[i]);

			return true;
		}
		else if (const auto pos_col = typeid_cast<const ColumnConst<PosType> *>(pos_col_untyped))
		{
			const auto & pos = pos_col->getData();
			const auto new_mask = 1 << pos;

			for (const auto i : ext::range(0, mask.size()))
				mask[i] = mask[i] | new_mask;

			return true;
		}

		return false;
	}
};

struct BitTestAnyImpl
{
	static constexpr auto name = "bitTestAny";
	template <typename T> static UInt8 combine(const T val, const T mask) { return (val & mask) != 0; }
};

struct BitTestAllImpl
{
	static constexpr auto name = "bitTestAll";
	template <typename T> static UInt8 combine(const T val, const T mask) { return (val & mask) == mask; }
};

using FunctionBitTestAny = FunctionBitTestMany<BitTestAnyImpl>;
using FunctionBitTestAll = FunctionBitTestMany<BitTestAllImpl>;


}
