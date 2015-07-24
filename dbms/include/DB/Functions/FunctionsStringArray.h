#pragma once

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Functions/IFunction.h>
#include <DB/Functions/FunctionsStringSearch.h>


namespace DB
{

/** Функции, разделяющие строки на массив строк или наоборот.
  *
  * splitByChar(sep, s)
  * splitByString(sep, s)
  * splitByRegexp(regexp, s)
  *
  * extractAll(s, regexp) 	- выделить из строки подпоследовательности, соответствующие регекспу.
  * - первый subpattern, если в regexp-е есть subpattern;
  * - нулевой subpattern (сматчившуюся часть, иначе);
  * - инача, пустой массив
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
	static constexpr auto name = "alphaTokens";
	static String getName() { return name; }

	/// Проверить типы агрументов функции.
	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
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

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 0;
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
	static constexpr auto name = "splitByChar";
	static String getName() { return name; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeString *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column);

		if (!col)
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName() + ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

		const String & sep_str = col->getData();

		if (sep_str.size() != 1)
			throw Exception("Illegal separator for function " + getName() + ". Must be exactly one byte.");

		sep = sep_str[0];
	}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 1;
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
	static constexpr auto name = "splitByString";
	static String getName() { return name; }

	static void checkArguments(const DataTypes & arguments)
	{
		SplitByCharImpl::checkArguments(arguments);
	}

	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column);

		if (!col)
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of first argument of function " + getName() + ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

		sep = col->getData();
	}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 1;
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

class ExtractAllImpl
{
private:
	Regexps::Pointer re;
	OptimizedRegularExpression::MatchVec matches;
	size_t capture;

	Pos pos;
	Pos end;
public:
	/// Получить имя функции.
	static constexpr auto name = "extractAll";
	static String getName() { return name; }

	/// Проверить типы агрументов функции.
	static void checkArguments( const DataTypes &  arguments )
	{
		SplitByStringImpl::checkArguments(arguments);
	}

	/// Инициализировать по аргументам функции.
	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = typeid_cast<const ColumnConstString *>(&*block.getByPosition(arguments[1]).column);

		if (!col)
			throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
				+ " of first argument of function " + getName() + ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

		re = Regexps::get<false, false>(col->getData());
		capture = re->getNumberOfSubpatterns() > 0 ? 1 : 0;

		matches.resize(capture + 1);
	}

	/// Возвращает позицию аргумента, являющегося столбцом строк
	size_t getStringsArgumentPosition()
	{
		return 0;
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
		if (!pos || pos > end)
			return false;

		if (!re->match(pos, end - pos, matches) || !matches[capture].length)
			return false;

		token_begin = pos + matches[capture].offset;
		token_end = token_begin + matches[capture].length;

		pos += matches[capture].offset + matches[capture].length;

		return true;
	}
};

/// Функция, принимающая строку, и возвращающая массив подстрок, создаваемый некоторым генератором.
template <typename Generator>
class FunctionTokens : public IFunction
{
public:
	static constexpr auto name = Generator::name;
	static IFunction * create(const Context & context) { return new FunctionTokens; }

	/// Получить имя функции.
	String getName() const
	{
		return name;
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
		size_t arrayArgumentPosition = arguments[generator.getStringsArgumentPosition()];

		const ColumnString * col_str = typeid_cast<const ColumnString *>(&*block.getByPosition(arrayArgumentPosition).column);
		const ColumnConstString * col_const_str =
				typeid_cast<const ColumnConstString *>(&*block.getByPosition(arrayArgumentPosition).column);

		ColumnArray * col_res = new ColumnArray(new ColumnString);
		ColumnPtr col_res_holder = col_res;
		ColumnString & res_strings = typeid_cast<ColumnString &>(col_res->getData());
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

			Pos token_begin = nullptr;
			Pos token_end = nullptr;

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
					res_strings_chars[current_dst_strings_offset + token_size] = 0;

					current_dst_strings_offset += token_size + 1;
					res_strings_offsets.push_back(current_dst_strings_offset);
					++j;
				}

				current_dst_offset += j;
				res_offsets.push_back(current_dst_offset);
			}

			block.getByPosition(result).column = col_res_holder;
		}
		else if (col_const_str)
		{
			String src = col_const_str->getData();
			Array dst;

			generator.set(src.data(), src.data() + src.size());
			Pos token_begin = nullptr;
			Pos token_end = nullptr;

			while (generator.get(token_begin, token_end))
				dst.push_back(String(token_begin, token_end - token_begin));

			block.getByPosition(result).column = new ColumnConstArray(col_const_str->size(), dst, new DataTypeArray(new DataTypeString));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arrayArgumentPosition).column->getName()
					+ ", " + block.getByPosition(arrayArgumentPosition).column->getName()
					+ " of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


typedef FunctionTokens<AlphaTokensImpl>	FunctionAlphaTokens;
typedef FunctionTokens<SplitByCharImpl>	FunctionSplitByChar;
typedef FunctionTokens<SplitByStringImpl>	FunctionSplitByString;
typedef FunctionTokens<ExtractAllImpl> 		FunctionExtractAll;

}
