#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Columns/ColumnConst.h>



namespace DB
{


/** Добавляет в блок недостающие столбцы со значениями по-умолчанию.
  * Эти столбцы - материалированные (не константы).
  */
class AddingDefaultBlockInputStream : public IProfilingBlockInputStream
{
public:
	AddingDefaultBlockInputStream(
		BlockInputStreamPtr input_,
		NamesAndTypesListPtr required_columns_)
		: input(input_), required_columns(required_columns_)
	{
		children.push_back(input);
	}

	Block readImpl()
	{
		Block res = input->read();
		if (!res)
			return res;

		for (NamesAndTypesList::const_iterator it = required_columns->begin(); it != required_columns->end(); ++it)
		{
			if (!res.has(it->first))
			{
				ColumnWithNameAndType col;
				col.name = it->first;
				col.type = it->second;
				col.column = dynamic_cast<IColumnConst &>(*it->second->createConstColumn(
					res.rows(), it->second->getDefault())).convertToFullColumn();
				res.insert(col);
			}
		}				
		
		return res;
	}

	String getName() const { return "AddingDefaultBlockInputStream"; }

	BlockInputStreamPtr clone() { return new AddingDefaultBlockInputStream(input, required_columns); }

private:
	BlockInputStreamPtr input;
	NamesAndTypesListPtr required_columns;
};

}
