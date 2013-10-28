#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Columns/ColumnConst.h>



namespace DB
{


/** Добавляет в блок недостающие столбцы со значениями по-умолчанию.
  * Эти столбцы - материалированные (не константы).
  */
class AddingDefaultBlockOutputStream : public IBlockOutputStream
{
public:
	AddingDefaultBlockOutputStream(
		BlockOutputStreamPtr output_,
		NamesAndTypesListPtr required_columns_)
		: output(output_), required_columns(required_columns_)
	{
	}

	String getName() const { return "AddingDefaultBlockOutputStream"; }

	void write(const Block & block) {
		Block res = block;
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
		output->write(res);
	}

private:
	BlockOutputStreamPtr output;
	NamesAndTypesListPtr required_columns;
};


}
