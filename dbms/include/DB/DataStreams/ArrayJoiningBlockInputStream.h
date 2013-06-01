#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>

#include <DB/DataTypes/DataTypeArray.h>

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{


/** Реализует операцию ARRAY JOIN.
  * (Для удобства, эта операция записывается, как функция arrayJoin, применённая к массиву.)
  * Эта операция размножает все строки столько раз, сколько элементов в массиве.
  * Результат функции arrayJoin - столбец единичных значений соответствующих элементов.
  *
  * Например,
  *
  * name    arr
  * ------  ------
  * 'вася'  [1, 2]
  * 'петя'  []
  *
  * преобразуется в
  *
  * name    arrayJoin(arr)
  * ------  --------------
  * 'вася'  1
  * 'вася'  2
  */
class ArrayJoiningBlockInputStream : public IProfilingBlockInputStream
{
public:
	ArrayJoiningBlockInputStream(BlockInputStreamPtr input_, ssize_t array_column_)
		: array_column(array_column_)
	{
		children.push_back(input_);
	}

	ArrayJoiningBlockInputStream(BlockInputStreamPtr input_, const String & array_column_name_)
		: array_column(-1), array_column_name(array_column_name_)
	{
		children.push_back(input_);
	}
	
	String getName() const { return "ArrayJoiningBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "ArrayJoining(" << children.back()->getID() << ", " << array_column << ", " << array_column_name << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		Block block = children.back()->read();

		if (!block)
			return block;

		if (-1 == array_column)
			array_column = block.getPositionByName(array_column_name);

		ColumnPtr array = block.getByPosition(array_column).column;

		if (array->isConst())
			array = dynamic_cast<const IColumnConst &>(*array).convertToFullColumn();

		size_t columns = block.columns();
		for (size_t i = 0; i < columns; ++i)
		{
			ColumnWithNameAndType & current = block.getByPosition(i);

			if (static_cast<ssize_t>(i) == array_column)
			{
				ColumnWithNameAndType result;

				result.column = dynamic_cast<const ColumnArray &>(*array).getDataPtr();
				result.type = dynamic_cast<const DataTypeArray &>(*current.type).getNestedType();
				result.name = "arrayJoin(" + current.name + ")";

				block.erase(i);
				block.insert(i, result);
			}
			else
				current.column = current.column->replicate(dynamic_cast<const ColumnArray &>(*array).getOffsets());
		}

		/// TODO: баг - если все массивы в данном блоке пустые, то вернётся пустой блок, и это будет ошибочно воспринято, как конец потока.

		return block;
	}

private:
	ssize_t array_column;
	String array_column_name;
};

}
