#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/MaterializingBlockInputStream.h>
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
}


StoragePtr InterpreterInsertQuery::getTable()
{
	ASTInsertQuery & query = dynamic_cast<ASTInsertQuery &>(*query_ptr);
	
	/// В какую таблицу писать.

	String database_name = query.database;
	String table_name = query.table;

	/** Если база данных не указана - используем текущую базу данных.
	  */
	if (database_name.empty())
		database_name = context.current_database;

	if (context.databases->end() == context.databases->find(database_name)
		|| (*context.databases)[database_name].end() == (*context.databases)[database_name].find(table_name))
		throw Exception("Unknown table '" + table_name + "' in database '" + database_name + "'", ErrorCodes::UNKNOWN_TABLE);

	return (*context.databases)[database_name][table_name];
}


void InterpreterInsertQuery::execute(ReadBuffer * remaining_data_istr)
{
	ASTInsertQuery & query = dynamic_cast<ASTInsertQuery &>(*query_ptr);
	StoragePtr table = getTable();
	
	/// TODO - если указаны не все столбцы, то дополнить поток недостающими столбцами со значениями по-умолчанию.

	BlockInputStreamPtr in;
	BlockOutputStreamPtr out = table->write(query_ptr);

	/// Какой тип запроса: INSERT VALUES | INSERT FORMAT | INSERT SELECT?
	if (!query.select)
	{
		String format = query.format;
		if (format.empty())
			format = "Values";

		/// Данные могут содержаться в распарсенной и ещё не распарсенной части запроса.
		ConcatReadBuffer::ReadBuffers buffers;
		ReadBuffer buf1(const_cast<char *>(query.data), query.end - query.data, 0);

		buffers.push_back(&buf1);
		if (remaining_data_istr)
			buffers.push_back(remaining_data_istr);

		ConcatReadBuffer istr(buffers);
		Block sample = table->getSampleBlock();

		in = context.format_factory->getInput(format, istr, sample, context.settings.max_block_size, *context.data_type_factory);
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

	/// TODO - если указаны не все столбцы, то дополнить поток недостающими столбцами со значениями по-умолчанию.
	BlockOutputStreamPtr out = table->write(query_ptr);

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
