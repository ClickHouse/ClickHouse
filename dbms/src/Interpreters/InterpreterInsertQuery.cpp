#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/ProhibitColumnsBlockOutputStream.h>
#include <DB/DataStreams/MaterializingBlockOutputStream.h>
#include <DB/DataStreams/AddingDefaultBlockOutputStream.h>
#include <DB/DataStreams/PushingToViewsBlockOutputStream.h>
#include <DB/DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DB/DataStreams/SquashingBlockOutputStream.h>
#include <DB/DataStreams/CountingBlockOutputStream.h>
#include <DB/DataStreams/NullableAdapterBlockInputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>


namespace ProfileEvents
{
	extern const Event InsertQuery;
}

namespace DB
{

namespace ErrorCodes
{
	extern const int NO_SUCH_COLUMN_IN_TABLE;
}


InterpreterInsertQuery::InterpreterInsertQuery(ASTPtr query_ptr_, Context & context_)
	: query_ptr(query_ptr_), context(context_)
{
	ProfileEvents::increment(ProfileEvents::InsertQuery);
}


StoragePtr InterpreterInsertQuery::getTable()
{
	ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);

	/// В какую таблицу писать.
	return context.getTable(query.database, query.table);
}

Block InterpreterInsertQuery::getSampleBlock()
{
	ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);

	/// Если в запросе не указана информация о столбцах
	if (!query.columns)
		return getTable()->getSampleBlockNonMaterialized();

	Block table_sample = getTable()->getSampleBlock();

	/// Формируем блок, основываясь на именах столбцов из запроса
	Block res;
	for (const auto & identifier : query.columns->children)
	{
		std::string current_name = identifier->getColumnName();

		/// В таблице нет столбца с таким именем
		if (!table_sample.has(current_name))
			throw Exception("No such column " + current_name + " in table " + query.table, ErrorCodes::NO_SUCH_COLUMN_IN_TABLE);

		ColumnWithTypeAndName col;
		col.name = current_name;
		col.type = table_sample.getByName(current_name).type;
		col.column = col.type->createColumn();
		res.insert(std::move(col));
	}

	return res;
}


BlockIO InterpreterInsertQuery::execute()
{
	ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);
	StoragePtr table = getTable();

	auto table_lock = table->lockStructure(true);

	NamesAndTypesListPtr required_columns = std::make_shared<NamesAndTypesList>(table->getColumnsList());

	/// Создаем конвейер из нескольких стримов, в которые будем писать данные.
	BlockOutputStreamPtr out;

	out = std::make_shared<PushingToViewsBlockOutputStream>(query.database, query.table, context, query_ptr);

	out = std::make_shared<MaterializingBlockOutputStream>(out);

	out = std::make_shared<AddingDefaultBlockOutputStream>(out,
		required_columns, table->column_defaults, context, static_cast<bool>(context.getSettingsRef().strict_insert_defaults));

	out = std::make_shared<ProhibitColumnsBlockOutputStream>(out, table->materialized_columns);

	out = std::make_shared<SquashingBlockOutputStream>(out,
		context.getSettingsRef().min_insert_block_size_rows,
		context.getSettingsRef().min_insert_block_size_bytes);

	auto out_wrapper = std::make_shared<CountingBlockOutputStream>(out);
	out_wrapper->setProcessListElement(context.getProcessListElement());
	out = std::move(out_wrapper);

	BlockIO res;
	res.out_sample = getSampleBlock();

	/// Какой тип запроса: INSERT или INSERT SELECT?
	if (!query.select)
	{
		res.out = out;
	}
	else
	{
		InterpreterSelectQuery interpreter_select{query.select, context};
		res.in_sample = interpreter_select.getSampleBlock();

		res.in = interpreter_select.execute().in;
		res.in = std::make_shared<NullableAdapterBlockInputStream>(res.in, res.in_sample, res.out_sample);
		res.in = std::make_shared<NullAndDoCopyBlockInputStream>(res.in, out);
	}

	return res;
}


}
