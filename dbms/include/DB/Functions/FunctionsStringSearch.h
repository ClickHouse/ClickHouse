#pragma once

#include <statdaemons/OptimizedRegularExpression.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции поиска и замены в строках:
  *
  * position(haystack, needle)	- обычный поиск подстроки в строке, возвращает позицию (в байтах) найденной подстроки, начиная с 1, или 0, если подстрока не найдена.
  * positionUTF8(haystack, needle) - то же самое, но позиция вычисляется в кодовых точках, при условии, что строка в кодировке UTF-8.
  * like(haystack, pattern)		- поиск по регулярному выражению LIKE; возвращает 0 или 1.
  * match(haystack, pattern)	- поиск по регулярному выражению re2; возвращает 0 или 1.
  * extract(haystack, pattern)	- вынимает первый subpattern, (или нулевой, если первого нет) согласно регулярному выражению re2;
  * 							  возвращает пустую строку, если не матчится.
  * extract(haystack, pattern, n) - вынимает n-ый subpattern; возвращает пустую строку, если не матчится.
  * replaceOne(haystack, pattern, replacement) - замена шаблона по заданным правилам, только первое вхождение.
  * replaceAll(haystack, pattern, replacement) - замена шаблона по заданным правилам, все вхождения.
  *
  * Внимание! На данный момент, аргементы needle, pattern, n, replacement обязаны быть константами.
  */


struct PositionImpl
{
	static void vector(const std::vector<UInt8> & data, const std::vector<size_t> & offsets,
		const std::string & needle,
		std::vector<UInt64> & res)
	{
		const UInt8 * begin = &data[0];
		const UInt8 * pos = begin;
		const UInt8 * end = pos + data.size();

		/// Текущий индекс в массиве строк.
		size_t i = 0;

		/// Искать будем следующее вхождение сразу во всех строках.
		while (pos < end && NULL != (pos = reinterpret_cast<UInt8 *>(memmem(pos, end - pos, needle.data(), needle.size()))))
		{
			/// Определим, к какому индексу оно относится.
			while (begin + offsets[i] < pos)
			{
				res[i] = 0;
				++i;
			}

			/// Проверяем, что вхождение не переходит через границы строк.
			if (pos + needle.size() < begin + offsets[i])
				res[i] = (i != 0) ? pos - begin - offsets[i - 1] + 1 : (pos - begin + 1);
			else
				res[i] = 0;

			++i;
			pos += needle.size() + 1;
		}

		for (size_t size = offsets.size(); i < size; ++i)
			res[i] = 0;
	}

	static void constant(const std::string & data, const std::string & needle, UInt64 & res)
	{
		res = data.find(needle);
		if (res == std::string::npos)
			res = 0;
		else
			++res;
	}
};


class FunctionPosition : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "position";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ Poco::NumberFormatter::format(arguments.size()) + ", should be 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[0]))
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (!dynamic_cast<const DataTypeString *>(&*arguments[1]))
			throw Exception("Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeUInt64;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		typedef UInt64 ResultType;
		
		const ColumnPtr column = block.getByPosition(arguments[0]).column;
		const ColumnPtr column_needle = block.getByPosition(arguments[1]).column;

		const ColumnConstString * col_needle = dynamic_cast<const ColumnConstString *>(&*column_needle);
		if (!col_needle)
			throw Exception("Second argument of function " + getName() + " must be constant string.", ErrorCodes::ILLEGAL_COLUMN);
		
		if (const ColumnString * col = dynamic_cast<const ColumnString *>(&*column))
		{
			ColumnVector<ResultType> * col_res = new ColumnVector<ResultType>;
			block.getByPosition(result).column = col_res;

			ColumnVector<ResultType>::Container_t & vec_res = col_res->getData();
			vec_res.resize(col->size());
			PositionImpl::vector(dynamic_cast<const ColumnUInt8 &>(col->getData()).getData(), col->getOffsets(), col_needle->getData(), vec_res);
		}
		else if (const ColumnConstString * col = dynamic_cast<const ColumnConstString *>(&*column))
		{
			ResultType res = 0;
			PositionImpl::constant(col->getData(), col_needle->getData(), res);

			ColumnConst<ResultType> * col_res = new ColumnConst<ResultType>(col->size(), res);
			block.getByPosition(result).column = col_res;
		}
		else
		   throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
				+ " of argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};

}
