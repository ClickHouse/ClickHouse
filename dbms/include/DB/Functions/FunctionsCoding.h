#pragma once

#include <DB/IO/WriteBufferFromVector.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


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
  */


/// Включая нулевой символ в конце.
#define MAX_UINT_HEX_LENGTH 20

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
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		
		if (!dynamic_cast<const DataTypeUInt32 *>(&*arguments[0]))
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
		
		if (const ColumnVector<UInt32> * col = dynamic_cast<const ColumnVector<UInt32> *>(&*column))
		{
			const ColumnVector<UInt32>::Container_t & vec_in = col->getData();
			
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;
			
			ColumnUInt8::Container_t & vec_res = dynamic_cast<ColumnUInt8 &>(col_res->getData()).getData();
			ColumnString::Offsets_t & offsets_res = col_res->getOffsets();
			
			vec_res.resize(vec_in.size() * 16); /// самое длинное значение: 255.255.255.255\0
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
		else if (const ColumnConst<UInt32> * col = dynamic_cast<const ColumnConst<UInt32> *>(&*column))
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
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
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
		
		if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnVector<UInt32> * col_res = new ColumnVector<UInt32>;
			block.getByPosition(result).column = col_res;
			
			ColumnVector<UInt32>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());
			
			const ColumnString::DataVector_t & vec_src = col->getDataVector();
			const ColumnString::Offsets_t & offsets_src = col->getOffsets();
			size_t prev_offset = 0;
			
			for (size_t i = 0; i < vec_res.size(); ++i)
			{
				vec_res[i] = parseIPv4(reinterpret_cast<const char *>(&vec_src[prev_offset]));
				prev_offset = offsets_src[i];
			}
		}
		else if (const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*column))
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
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeDate *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeDateTime *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt8 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt16 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt32 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt64 *>(&*arguments[0]))
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
		const ColumnVector<T> * col_vec = dynamic_cast<const ColumnVector<T> *>(col);
		const ColumnConst<T> * col_const = dynamic_cast<const ColumnConst<T> *>(col);
		
		if (col_vec)
		{
			ColumnString * col_str = new ColumnString;
			col_res = col_str;
			ColumnString::DataVector_t & out_vec = col_str->getDataVector();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();
			
			const typename ColumnVector<T>::Container_t & in_vec = col_vec->getData();
			
			size_t size = in_vec.size();
			out_offsets.resize(size);
			out_vec.resize(size * 3 + MAX_UINT_HEX_LENGTH);
			
			size_t pos = 0;
			for (size_t i = 0; i < size; ++i)
			{
				/// Ручной экспоненциальный рост, чтобы не полагаться на линейное амортизированное время работы resize (его никто не гарантирует).
				if (pos + MAX_UINT_HEX_LENGTH < out_vec.size())
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
		const ColumnString * col_str_in = dynamic_cast<const ColumnString *>(col);
		const ColumnConstString * col_const_in = dynamic_cast<const ColumnConstString *>(col);
		
		if (col_str_in)
		{
			ColumnString * col_str = new ColumnString;
			col_res = col_str;
			ColumnString::DataVector_t & out_vec = col_str->getDataVector();
			ColumnString::Offsets_t & out_offsets = col_str->getOffsets();
			
			const ColumnString::DataVector_t & in_vec = col_str_in->getDataVector();
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
			
			if (out_offsets.back() != out_vec.size())
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
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const IColumn * column = &*block.getByPosition(arguments[0]).column;
		ColumnPtr & res_column = block.getByPosition(result).column;
		
		if (tryExecuteUInt<UInt8>(column, res_column) ||
			tryExecuteUInt<UInt16>(column, res_column) ||
			tryExecuteUInt<UInt32>(column, res_column) ||
			tryExecuteUInt<UInt64>(column, res_column) ||
			tryExecuteString(column, res_column))
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
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
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
		
		if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnString * col_res = new ColumnString;
			block.getByPosition(result).column = col_res;
			
			ColumnString::DataVector_t & out_vec = col_res->getDataVector();
			ColumnString::Offsets_t & out_offsets = col_res->getOffsets();
			
			const ColumnString::DataVector_t & in_vec = col->getDataVector();
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
		else if(const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*column))
		{
			const std::string & src = col->getData();
			std::string res(src.size(), '\0');
			char * pos = &res[0];
			unhexOne(src.c_str(), src.c_str() + src.size(), pos);
			res = res.substr(0, pos - src.c_str());
			
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
			+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
							ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
		if (!dynamic_cast<const DataTypeUInt8 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt16 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt32 *>(&*arguments[0]) &&
			!dynamic_cast<const DataTypeUInt64 *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		
		return new DataTypeArray(arguments[0]);
	}
	
	template <typename T>
	bool tryExecute(const IColumn * column, ColumnPtr & out_column)
	{
		if (const ColumnVector<T> * col_from = dynamic_cast<const ColumnVector<T> *>(column))
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
				for (size_t i = 0; i < sizeof(T) * 8; ++i)
				{
					T bit = static_cast<T>(1) << i;
					if (x & bit)
					{
						res_values.push_back(bit);
					}
				}
				res_offsets[row] = res_values.size();
			}
			
			return true;
		}
		else if (const ColumnConst<T> * col_from = dynamic_cast<const ColumnConst<T> *>(column))
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
			
			out_column = new ColumnConstArray(col_from->size(), res, new typename DataTypeFromFieldType<T>::Type);
			
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
			tryExecute<UInt64>(in_column, out_column))
			return;
		
		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
						+ " of first argument of function " + getName(),
						ErrorCodes::ILLEGAL_COLUMN);
	}
};

}
