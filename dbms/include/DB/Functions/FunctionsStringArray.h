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
  * arrayStringConcat(arr)
  * arrayStringConcat(arr, delimiter)
  * - склеить массив строк в одну строку через разделитель.
  *
  * alphaTokens(s)			- выделить из строки подпоследовательности [a-zA-Z]+.
  *
  * Функции работы с URL расположены отдельно.
  */


using Pos = const char *;


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

	static size_t getNumberOfArguments() { return 1; }

	/// Проверить типы агрументов функции.
	static void checkArguments(const DataTypes & arguments)
	{
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
		/// Skip garbage
		while (pos < end && !isAlphaASCII(*pos))
			++pos;

		if (pos == end)
			return false;

		token_begin = pos;

		while (pos < end && isAlphaASCII(*pos))
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
	static size_t getNumberOfArguments() { return 2; }

	static void checkArguments(const DataTypes & arguments)
	{
		if (!typeid_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!typeid_cast<const DataTypeString *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = typeid_cast<const ColumnConstString *>(block.getByPosition(arguments[0]).column.get());

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
	static size_t getNumberOfArguments() { return 2; }

	static void checkArguments(const DataTypes & arguments)
	{
		SplitByCharImpl::checkArguments(arguments);
	}

	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = typeid_cast<const ColumnConstString *>(block.getByPosition(arguments[0]).column.get());

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
	Regexps::Pool::Pointer re;
	OptimizedRegularExpression::MatchVec matches;
	size_t capture;

	Pos pos;
	Pos end;
public:
	static constexpr auto name = "extractAll";
	static String getName() { return name; }
	static size_t getNumberOfArguments() { return 2; }

	/// Проверить типы агрументов функции.
	static void checkArguments( const DataTypes &  arguments )
	{
		SplitByStringImpl::checkArguments(arguments);
	}

	/// Инициализировать по аргументам функции.
	void init(Block & block, const ColumnNumbers & arguments)
	{
		const ColumnConstString * col = typeid_cast<const ColumnConstString *>(block.getByPosition(arguments[1]).column.get());

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
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionTokens>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	size_t getNumberOfArguments() const override { return Generator::getNumberOfArguments(); }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		Generator::checkArguments(arguments);

		return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		Generator generator;
		generator.init(block, arguments);
		size_t arrayArgumentPosition = arguments[generator.getStringsArgumentPosition()];

		const ColumnString * col_str = typeid_cast<const ColumnString *>(block.getByPosition(arrayArgumentPosition).column.get());
		const ColumnConstString * col_const_str =
				typeid_cast<const ColumnConstString *>(block.getByPosition(arrayArgumentPosition).column.get());

		auto col_res = std::make_shared<ColumnArray>(std::make_shared<ColumnString>());
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
					memcpySmallAllowReadWriteOverflow15(&res_strings_chars[current_dst_strings_offset], token_begin, token_size);
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

			block.getByPosition(result).column = std::make_shared<ColumnConstArray>(col_const_str->size(), dst, std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arrayArgumentPosition).column->getName()
					+ ", " + block.getByPosition(arrayArgumentPosition).column->getName()
					+ " of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Склеивает массив строк в одну строку через разделитель.
class FunctionArrayStringConcat : public IFunction
{
private:
	void executeInternal(
		const ColumnString::Chars_t & src_chars,
		const ColumnString::Offsets_t & src_string_offsets,
		const ColumnArray::Offsets_t & src_array_offsets,
		const char * delimiter, const size_t delimiter_size,
		ColumnString::Chars_t & dst_chars,
		ColumnString::Offsets_t & dst_string_offsets)
	{
		size_t size = src_array_offsets.size();

		if (!size)
			return;

		/// С небольшим запасом - как будто разделитель идёт и после последней строки массива.
		dst_chars.resize(
			src_chars.size()
			+ delimiter_size * src_string_offsets.size()	/// Разделители после каждой строки...
			+ src_array_offsets.size() 					/// Нулевой байт после каждой склеенной строки
			- src_string_offsets.size());				/// Бывший нулевой байт после каждой строки массива

		/// Будет столько строк, сколько было массивов.
		dst_string_offsets.resize(src_array_offsets.size());

		ColumnArray::Offset_t current_src_array_offset = 0;
		ColumnString::Offset_t current_src_string_offset = 0;

		ColumnString::Offset_t current_dst_string_offset = 0;

		/// Цикл по массивам строк.
		for (size_t i = 0; i < size; ++i)
		{
			/// Цикл по строкам внутри массива. /// NOTE Можно всё сделать за одно копирование, если разделитель имеет размер 1.
			for (auto next_src_array_offset = src_array_offsets[i]; current_src_array_offset < next_src_array_offset; ++current_src_array_offset)
			{
				size_t bytes_to_copy = src_string_offsets[current_src_array_offset] - current_src_string_offset - 1;

				memcpySmallAllowReadWriteOverflow15(
					&dst_chars[current_dst_string_offset], &src_chars[current_src_string_offset], bytes_to_copy);

				current_src_string_offset = src_string_offsets[current_src_array_offset];
				current_dst_string_offset += bytes_to_copy;

				if (current_src_array_offset + 1 != next_src_array_offset)
				{
					memcpy(&dst_chars[current_dst_string_offset], delimiter, delimiter_size);
					current_dst_string_offset += delimiter_size;
				}
			}

			dst_chars[current_dst_string_offset] = 0;
			++current_dst_string_offset;

			dst_string_offsets[i] = current_dst_string_offset;
		}

		dst_chars.resize(dst_string_offsets.back());
	}

public:
	static constexpr auto name = "arrayStringConcat";
	static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayStringConcat>(); }

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1 && arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1 or 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const DataTypeArray * array_type = typeid_cast<const DataTypeArray *>(arguments[0].get());
		if (!array_type || !typeid_cast<const DataTypeString *>(array_type->getNestedType().get()))
			throw Exception("First argument for function " + getName() + " must be array of strings.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments.size() == 2
			&& !typeid_cast<const DataTypeString *>(arguments[1].get()))
			throw Exception("Second argument for function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		String delimiter;
		if (arguments.size() == 2)
		{
			const ColumnConstString * col_delim = typeid_cast<const ColumnConstString *>(block.getByPosition(arguments[1]).column.get());
			if (!col_delim)
				throw Exception("Second argument for function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);

			delimiter = col_delim->getData();
		}

		if (const ColumnConstArray * col_const_arr = typeid_cast<const ColumnConstArray *>(block.getByPosition(arguments[0]).column.get()))
		{
			auto col_res = std::make_shared<ColumnConstString>(col_const_arr->size(), "");
			block.getByPosition(result).column = col_res;

			const Array & src_arr = col_const_arr->getData();
			String & dst_str = col_res->getData();
			for (size_t i = 0, size = src_arr.size(); i < size; ++i)
			{
				if (i != 0)
					dst_str += delimiter;
				dst_str += src_arr[i].get<const String &>();
			}
		}
		else
		{
			const ColumnArray & col_arr = static_cast<const ColumnArray &>(*block.getByPosition(arguments[0]).column);
			const ColumnString & col_string = static_cast<const ColumnString &>(col_arr.getData());

			std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
			block.getByPosition(result).column = col_res;

			executeInternal(
				col_string.getChars(), col_string.getOffsets(), col_arr.getOffsets(),
				delimiter.data(), delimiter.size(),
				col_res->getChars(), col_res->getOffsets());
		}
	}
};


using FunctionAlphaTokens = FunctionTokens<AlphaTokensImpl>	;
using FunctionSplitByChar = FunctionTokens<SplitByCharImpl>	;
using FunctionSplitByString = FunctionTokens<SplitByStringImpl>;
using FunctionExtractAll = FunctionTokens<ExtractAllImpl> 	;

}
