#include <DB/IO/ConcatReadBuffer.h>

#include <DB/DataStreams/ProhibitColumnsBlockOutputStream.h>
#include <DB/DataStreams/MaterializingBlockOutputStream.h>
#include <DB/DataStreams/AddingDefaultBlockOutputStream.h>
#include <DB/DataStreams/PushingToViewsBlockOutputStream.h>
#include <DB/DataStreams/NullAndDoCopyBlockInputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>

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
		res.insert(col);
	}

	return res;
}


BlockIO InterpreterInsertQuery::execute()
{
	ASTInsertQuery & query = typeid_cast<ASTInsertQuery &>(*query_ptr);
	StoragePtr table = getTable();

	auto table_lock = table->lockStructure(true);

	NamesAndTypesListPtr required_columns = new NamesAndTypesList(table->getColumnsList());

	/// Создаем кортеж из нескольких стримов, в которые будем писать данные.
	BlockOutputStreamPtr out{
		new ProhibitColumnsBlockOutputStream{
			new AddingDefaultBlockOutputStream{
				new MaterializingBlockOutputStream{
					new PushingToViewsBlockOutputStream{query.database, query.table, context, query_ptr}
				},
				required_columns, table->column_defaults, context, static_cast<bool>(context.getSettingsRef().strict_insert_defaults)
			},
			table->materialized_columns
		}
	};

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
		BlockInputStreamPtr in{interpreter_select.execute().in};
		res.in = new NullAndDoCopyBlockInputStream{in, out};
		res.in_sample = interpreter_select.getSampleBlock();
	}

	return res;
}


}
