#pragma once

#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции, разделяющие строки на массив строк или наоборот.
  *
  * splitByChar(sep, s)
  * splitByString(sep, s)
  * splitByRegexp(regexp, s)
  *
  * extractAll(regexp, s) 	- выделить из строки подпоследовательности, соответствующие регекспу.
  * 
  * join(sep, arr)
  * join(arr)
  * 
  * alphaTokens(s)			- выделить из строки подпоследовательности [a-zA-Z]+.
  * 
  * Функции работы с URL расположены отдельно.
  */


typedef const char * Pos;


/// Генераторы подстрок. Все они обладают общим интерфейсом.

class AlphaTokensImpl
{
private:
	Pos pos;
	Pos end;

public:
	/// Получить имя фукнции.
	static String getName() { return "alphaTokens"; }
	
	/// Проверить типы агрументов функции.
	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}
	
	/// Инициализировать по аргументам функции.
	void init(Block & block, const ColumnNumbers & arguments) {}

	/// Вызывается для каждой следующей строки.
	void set(Pos pos_, Pos end_)
	{
		pos = pos_;
		end = end_;
	}

	/// Получить следующий токен, если есть, или вернуть false.
	bool get(Pos & token_begin, Pos & token_end)
	{
		/// Пропускаем мусор
		while (pos < end && !((*pos >= 'a' && *pos <= 'z') || (*pos >= 'A' && *pos <= 'Z')))
			++pos;

		if (pos == end)
			return false;

		token_begin = pos;

		while (pos < end && ((*pos >= 'a' && *pos <= 'z') || (*pos >= 'A' && *pos <= 'Z')))
			++pos;

		token_end = pos;

		return true;
	}
};


class SplitByCharImpl
{
private:
	Pos pos;
	Pos end;

	char sep;

public:
	static String getName() { return "splitByChar"; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column);

		if (!col)
			throw Exception("Illegal column " + col->getName() + " of first argument of function " + getName() + ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

		const String & sep_str = col->getData();

		if (sep_str.size() != 1)
			throw Exception("Illegal separator for function " + getName() + ". Must be exactly one byte.");

		sep = sep_str[0];
	}

	void set(Pos pos_, Pos end_)
	{
		pos = pos_;
		end = end_;
	}

	bool get(Pos & token_begin, Pos & token_end)
	{
		if (!pos)
			return false;

		token_begin = pos;
		pos = reinterpret_cast<Pos>(memchr(pos, sep, end - pos));

		if (pos)
		{
			token_end = pos;
			++pos;
		}
		else
			token_end = end;

		return true;
	}
};


class SplitByStringImpl
{
private:
	Pos pos;
	Pos end;

	String sep;

public:
	static String getName() { return "splitByString"; }

	static void checkArguments(const DataTypes & arguments)
	{
		SplitByCharImpl::checkArguments(arguments);
	}

	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column);

		if (!col)
			throw Exception("Illegal column " + col->getName() + " of first argument of function " + getName() + ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

		sep = col->getData();
	}

	/// Вызывается для каждой следующей строки.
	void set(Pos pos_, Pos end_)
	{
		pos = pos_;
		end = end_;
	}

	/// Получить следующий токен, если есть, или вернуть false.
	bool get(Pos & token_begin, Pos & token_end)
	{
		if (!pos)
			return false;

		token_begin = pos;
		pos = reinterpret_cast<Pos>(memmem(pos, end - pos, sep.data(), sep.size()));

		if (pos)
		{
			token_end = pos;
			pos += sep.size();
		}
		else
			token_end = end;

		return true;
	}
};


/// Функция, принимающая строку, и возвращающая массив подстрок, создаваемый некоторым генератором.
template <typename Generator>
class FunctionTokens : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return Generator::getName();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		Generator::checkArguments(arguments);
		
		return new DataTypeArray(new DataTypeString);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		Generator generator;
		generator.init(block, arguments);

		const ColumnString * col_str = dynamic_cast<const ColumnString *>(&*block.getByPosition(arguments.back()).column);
		const ColumnConstString * col_const_str = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments.back()).column);

		ColumnArray * col_res = new ColumnArray(new ColumnString);
		ColumnString & res_strings = dynamic_cast<ColumnString &>(col_res->getData());
		ColumnArray::Offsets_t & res_offsets = col_res->getOffsets();
		ColumnString::Chars_t & res_strings_chars = res_strings.getChars();
		ColumnString::Offsets_t & res_strings_offsets = res_strings.getOffsets();

		if (col_str)
		{
			const ColumnString::Chars_t & src_chars = col_str->getChars();
			const ColumnString::Offsets_t & src_offsets = col_str->getOffsets();

			res_offsets.reserve(src_offsets.size());
			res_strings_offsets.reserve(src_offsets.size() * 5);	/// Константа 5 - наугад.
			res_strings_chars.reserve(src_chars.size());

			Pos token_begin = NULL;
			Pos token_end = NULL;

			size_t size = src_offsets.size();
			ColumnString::Offset_t current_src_offset = 0;
			ColumnArray::Offset_t current_dst_offset = 0;
			ColumnString::Offset_t current_dst_strings_offset = 0;
			for (size_t i = 0; i < size; ++i)
			{
				Pos pos = reinterpret_cast<Pos>(&src_chars[current_src_offset]);
				current_src_offset = src_offsets[i];
				Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]) - 1;

				generator.set(pos, end);

				size_t j = 0;
				while (generator.get(token_begin, token_end))
				{
					size_t token_size = token_end - token_begin;

					res_strings_chars.resize(res_strings_chars.size() + token_size + 1);
					memcpy(&res_strings_chars[current_dst_strings_offset], token_begin, token_size);
					/// Нулевой байт после токена и так инициализирован std::vector-ом.
					
					current_dst_strings_offset += token_size + 1;
					res_strings_offsets.push_back(current_dst_strings_offset);
					++j;
				}

				current_dst_offset += j;
				res_offsets.push_back(current_dst_offset);
			}

			block.getByPosition(result).column = col_res;
		}
		else if (col_const_str)
		{
			String src = col_const_str->getData();
			Array dst;

			generator.set(src.data(), src.data() + src.size());
			Pos token_begin = NULL;
			Pos token_end = NULL;

			while (generator.get(token_begin, token_end))
				dst.push_back(String(token_begin, token_end - token_begin));

			block.getByPosition(result).column = new ColumnConstArray(col_const_str->size(), dst, new DataTypeArray(new DataTypeString));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arguments.back()).column->getName()
					+ ", " + block.getByPosition(arguments.back()).column->getName()
					+ " of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


typedef FunctionTokens<AlphaTokensImpl>	FunctionAlphaTokens;
typedef FunctionTokens<SplitByCharImpl>	FunctionSplitByChar;
typedef FunctionTokens<SplitByStringImpl>	FunctionSplitByString;

}
