#pragma once

#include <DB/DataTypes/DataTypeArray.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

/** Функции по работе с массивами:
  *
  * array(с1, с2, ...) - создать массив из констант.
  * arrayElement(arr, i) - получить элемент массива.
  */

class FunctionArray : public IFunction
{
public:
	/// Получить имя функции.
	String getName() const
	{
		return "array";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
	{
		if (arguments.empty())
			throw Exception("Function array requires at least one argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		for (size_t i = 1, size = arguments.size(); i < size; ++i)
			if (arguments[i]->getName() != arguments[0]->getName())
				throw Exception("Arguments for function array must have same type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return new DataTypeArray(arguments[0]);
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Все аргументы должны быть константами.
		for (size_t i = 0, size = arguments.size(); i < size; ++i)
			if (!block.getByPosition(arguments[i]).column->isConst())
				throw Exception("Arguments for function array must be constant.", ErrorCodes::ILLEGAL_COLUMN);;

		Array arr;
		for (size_t i = 0, size = arguments.size(); i < size; ++i)
			arr.push_back((*block.getByPosition(arguments[i]).column)[0]);

		block.getByPosition(result).column = new ColumnConstArray(block.getByPosition(arguments[0]).column->size(), arr);
	}
};


}
