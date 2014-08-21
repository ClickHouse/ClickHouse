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

	void write(const Block & block)
	{
		Block res = block;
		res.addDefaults(required_columns);
		output->write(res);
	}

	void flush() { output->flush(); }

private:
	BlockOutputStreamPtr output;
	NamesAndTypesListPtr required_columns;
};


}
