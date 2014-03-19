#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/AddingDefaultBlockOutputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/PushingToViewsBlockOutputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>

namespace DB
{


InterpreterInsertQuery::InterpreterInsertQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
	ProfileEvents::increment(ProfileEvents::InsertQuery);
}


StoragePtr InterpreterInsertQuery::getTable()
{
	ASTInsertQuery & query = dynamic_cast<ASTInsertQuery &>(*query_ptr);
	
	/// В какую таблицу писать.
	return context.getTable(query.database, query.table);
}

Block InterpreterInsertQuery::getSampleBlock()
{
	ASTInsertQuery & query = dynamic_cast<ASTInsertQuery &>(*query_ptr);
	Block db_sample = getTable()->getSampleBlock();

	/// Если в запросе не указана информация о столбцах
	if (!query.columns)
		return db_sample;

	/// Формируем блок, основываясь на именах столбцов из запроса
	Block res;
	for (ASTs::iterator it = query.columns->children.begin(); it != query.columns->children.end(); ++ it)
	{
		std::string currentName = (*it)->getColumnName();

		/// В таблице нет столбца с таким именем
		if (!db_sample.has(currentName))
			throw Exception("No such column " + currentName + " in table " + query.table, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		ColumnWithNameAndType col;
		col.name = currentName;
		col.type = db_sample.getByName(col.name).type;
		col.column = col.type->createColumn();
		res.insert(col);
	}
	
	return res;
}

void InterpreterInsertQuery::execute(ReadBuffer * remaining_data_istr)
{
	ASTInsertQuery & query = dynamic_cast<ASTInsertQuery &>(*query_ptr);
	StoragePtr table = getTable();

	auto table_lock = table->lockStructure(true);

	BlockInputStreamPtr in;
	NamesAndTypesListPtr required_columns = new NamesAndTypesList (table->getSampleBlock().getColumnsList());
	/// Надо убедиться, что запрос идет в таблицу, которая поддерживает вставку.
	table->write(query_ptr);
	/// Создаем кортеж из нескольких стримов, в которые будем писать данные.

	BlockOutputStreamPtr out = new AddingDefaultBlockOutputStream(new PushingToViewsBlockOutputStream(query.database, query.table, context, query_ptr), required_columns);

	/// TODO: Взять также IStorage::TableStructureReadLock-и для всех затронутых materialized views.
	out->addTableLock(table_lock);

	/// Какой тип запроса: INSERT VALUES | INSERT FORMAT | INSERT SELECT?
	if (!query.select)
	{
		String format = query.format;
		if (format.empty())
			format = "Values";

		/// Данные могут содержаться в распарсенной (query.data) и ещё не распарсенной (remaining_data_istr) части запроса.

		/// Если данных нет.
		bool has_remaining_data = remaining_data_istr && !remaining_data_istr->eof();
			
		if (!query.data && !has_remaining_data)
			throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);

		ConcatReadBuffer::ReadBuffers buffers;
		ReadBuffer buf1(const_cast<char *>(query.data), query.data ? query.end - query.data : 0, 0);

		if (query.data)
			buffers.push_back(&buf1);
		if (has_remaining_data)
			buffers.push_back(remaining_data_istr);

		ConcatReadBuffer istr(buffers);
		Block sample = table->getSampleBlock();

		in = context.getFormatFactory().getInput(format, istr, sample, context.getSettings().max_block_size, context.getDataTypeFactory());
		copyData(*in, *out);
	}
	else
	{
		InterpreterSelectQuery interpreter_select(query.select, context);
		in = interpreter_select.execute();
		in = new MaterializingBlockInputStream(in);

		copyData(*in, *out);
	}
}


BlockOutputStreamPtr InterpreterInsertQuery::execute()
{
	ASTInsertQuery & query = dynamic_cast<ASTInsertQuery &>(*query_ptr);
	StoragePtr table = getTable();

	auto table_lock = table->lockStructure(true);

	NamesAndTypesListPtr required_columns = new NamesAndTypesList(table->getSampleBlock().getColumnsList());

	/// Надо убедиться, что запрос идет в таблицу, которая поддерживает вставку.
	table->write(query_ptr);
	/// Создаем кортеж из нескольких стримов, в которые будем писать данные.
	BlockOutputStreamPtr out = new AddingDefaultBlockOutputStream(new PushingToViewsBlockOutputStream(query.database, query.table, context, query_ptr), required_columns);

	/// TODO: Взять также IStorage::TableStructureReadLock-и для всех затронутых materialized views.
	out->addTableLock(table_lock);

	/// Какой тип запроса: INSERT или INSERT SELECT?
	if (!query.select)
		return out;
	else
	{
		InterpreterSelectQuery interpreter_select(query.select, context);
		BlockInputStreamPtr in = interpreter_select.execute();
		in = new MaterializingBlockInputStream(in);
		copyData(*in, *out);

		return NULL;
	}
}


}
