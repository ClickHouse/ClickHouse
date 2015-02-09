#pragma once

#include <Poco/SharedPtr.h>

#include <DB/DataStreams/IBlockOutputStream.h>
#include <DB/Columns/ColumnConst.h>

#include <DB/Storages/ColumnDefault.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/evaluateMissingDefaults.h>


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
		NamesAndTypesListPtr required_columns_,
		const ColumnDefaults & column_defaults_,
		const Context & context_)
		: output(output_), required_columns(required_columns_),
		  column_defaults(column_defaults_), context(context_)
	{
	}

	AddingDefaultBlockOutputStream(BlockOutputStreamPtr output_, NamesAndTypesListPtr required_columns_, const Context & context_)
		: AddingDefaultBlockOutputStream{output_, required_columns_, ColumnDefaults{}, context_}
	{
	}


	void write(const Block & block) override
	{
		Block res = block;
		evaluateMissingDefaults(res, *required_columns, column_defaults, context);
		res.addDefaults(*required_columns);
		output->write(res);
	}

	void flush() override { output->flush(); }

	void writePrefix() override { output->writePrefix(); }
	void writeSuffix() override { output->writeSuffix(); }

private:
	BlockOutputStreamPtr output;
	NamesAndTypesListPtr required_columns;
	const ColumnDefaults & column_defaults;
	Context context;
};


}
