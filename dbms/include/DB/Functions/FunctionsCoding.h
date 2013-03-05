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
	* IPv4NumToString(num) - см. ниже.
	* IPv4StringToNum(string) - преобразуют, например, '192.168.0.1' в 3232235521 и наоборот.
	*/


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
	
	/// Быстрее, чем std::reverse.
	static void reverseString(char *begin, char * end)
	{
		--end;
		while (end > begin)
		{
			char c = *begin;
			*begin = *end;
			*end = c;
			++begin;
			--end;
		}
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
		reverseString(begin, out);
		
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
		if (*pos != '\0')
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
	
}
