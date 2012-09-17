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
  * split(sep, s)
  * join(sep, arr)
  * join(arr)
  * alphaTokens(s)	- выделить из строки подпоследовательности [a-zA-Z]+.
  * 
  * Функции работы с URL расположены отдельно.
  */


typedef const char * Pos;

/// Получить следующий кусок [a-zA-Z]+
inline Pos nextAlphaToken(Pos pos, Pos end, Pos & token_begin, Pos & token_end)
{
	/// Пропускаем мусор
	while (pos < end && !((*pos >= 'a' && *pos <= 'z') || (*pos >= 'A' && *pos <= 'Z')))
		++pos;

	if (pos == end)
		return NULL;

	token_begin = pos;

	while (pos < end && ((*pos >= 'a' && *pos <= 'z') || (*pos >= 'A' && *pos <= 'Z')))
		++pos;

	token_end = pos;

	return pos;
}


class FunctionAlphaTokens : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "alphaTokens";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 1)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 1.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() + ". Must be String.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(new DataTypeString);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		const ColumnString * col_str = dynamic_cast<const ColumnString *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConstString * col_const_str = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[0]).column);

		ColumnArray * col_res = new ColumnArray(new ColumnString);
		ColumnString & res_strings = dynamic_cast<ColumnString &>(col_res->getData());
		ColumnArray::Offsets_t & res_offsets = col_res->getOffsets();
		ColumnUInt8::Container_t & res_strings_chars = dynamic_cast<ColumnUInt8 &>(res_strings.getData()).getData();
		ColumnString::Offsets_t & res_strings_offsets = res_strings.getOffsets();

		if (col_str)
		{
			const ColumnUInt8::Container_t & src_chars = dynamic_cast<const ColumnUInt8 &>(col_str->getData()).getData();
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
				Pos end = reinterpret_cast<Pos>(&src_chars[current_src_offset]);

				size_t j = 0;
				while (NULL != (pos = nextAlphaToken(pos, end, token_begin, token_end)))
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

			Pos pos = src.data();
			Pos end = src.data() + src.size();
			Pos token_begin = NULL;
			Pos token_end = NULL;

			while (NULL != (pos = nextAlphaToken(pos, end, token_begin, token_end)))
				dst.push_back(String(token_begin, token_end - token_begin));

			block.getByPosition(result).column = new ColumnConstArray(col_const_str->size(), dst);
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
					+ ", " + block.getByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


}
