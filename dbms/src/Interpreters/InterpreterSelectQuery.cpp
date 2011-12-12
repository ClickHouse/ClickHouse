#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/ProjectionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/PartialSortingBlockInputStream.h>
#include <DB/DataStreams/MergeSortingBlockInputStream.h>
#include <DB/DataStreams/AggregatingBlockInputStream.h>
#include <DB/DataStreams/FinalizingAggregatedBlockInputStream.h>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/FormatFactory.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTOrderByElement.h>

#include <DB/Interpreters/Expression.h>
#include <DB/Interpreters/InterpreterSelectQuery.h>


namespace DB
{


InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, Context & context_, size_t max_block_size_)
	: query_ptr(query_ptr_), context(context_), max_block_size(max_block_size_)
{
}


StoragePtr InterpreterSelectQuery::getTable()
{
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);
	
	/// Из какой таблицы читать данные. JOIN-ы не поддерживаются.

	String database_name;
	String table_name;

	/** Если таблица не указана - используем таблицу system.one.
	  * Если база данных не указана - используем текущую базу данных.
	  */
	if (!query.table)
	{
		database_name = "system";
		table_name = "one";
	}
	else if (!query.database)
		database_name = context.current_database;

	if (query.database)
		database_name = dynamic_cast<ASTIdentifier &>(*query.database).name;
	if (query.table)
		table_name = dynamic_cast<ASTIdentifier &>(*query.table).name;

	if (context.databases->end() == context.databases->find(database_name)
		|| (*context.databases)[database_name].end() == (*context.databases)[database_name].find(table_name))
		throw Exception("Unknown table '" + table_name + "' in database '" + database_name + "'", ErrorCodes::UNKNOWN_TABLE);

	return (*context.databases)[database_name][table_name];
}


void InterpreterSelectQuery::setColumns()
{
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);

	context.columns = !query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table)
		? getTable()->getColumnsList()
		: InterpreterSelectQuery(query.table, context, max_block_size).getSampleBlock().getColumnsList();

	if (context.columns.empty())
		throw Exception("There is no available columns", ErrorCodes::THERE_IS_NO_COLUMN);
}


DataTypes InterpreterSelectQuery::getReturnTypes()
{
	setColumns();
	Expression expression(dynamic_cast<ASTSelectQuery &>(*query_ptr).select_expression_list, context);
	return expression.getReturnTypes();
}


Block InterpreterSelectQuery::getSampleBlock()
{
	setColumns();
	Expression expression(dynamic_cast<ASTSelectQuery &>(*query_ptr).select_expression_list, context);
	return expression.getSampleBlock();
}


BlockInputStreamPtr InterpreterSelectQuery::execute()
{
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);

	/// Таблица, откуда читать данные, если не подзапрос.
	StoragePtr table;
	/// Интерпретатор подзапроса, если подзапрос
	SharedPtr<InterpreterSelectQuery> interpreter_subquery;

	/// Добавляем в контекст список доступных столбцов.
	setColumns();
	
	if (!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table))
		table = getTable();
	else
		interpreter_subquery = new InterpreterSelectQuery(query.table, context, max_block_size);
	
	/// Объект, с помощью которого анализируется запрос.
	Poco::SharedPtr<Expression> expression = new Expression(query_ptr, context);
	/// Список столбцов, которых нужно прочитать, чтобы выполнить запрос.
	Names required_columns = expression->getRequiredColumns();

	/// Если не указан ни один столбец из таблицы, то будем читать первый попавшийся (чтобы хотя бы знать число строк).
	if (required_columns.empty())
		required_columns.push_back(context.columns.front().first);

	/// Нужно ли агрегировать.
	bool need_aggregate = expression->hasAggregates() || query.group_expression_list;

	size_t limit_length = 0;
	size_t limit_offset = 0;
	if (query.limit_length)
	{
		limit_length = boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*query.limit_length).value);
		if (query.limit_offset)
			limit_offset = boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*query.limit_offset).value);
	}

	/** Оптимизация - если не указаны WHERE, GROUP, HAVING, ORDER, но указан LIMIT, и limit + offset < max_block_size,
	  *  то в качестве размера блока будем использовать limit + offset (чтобы не читать из таблицы больше, чем запрошено).
	  */
	size_t block_size = max_block_size;
	if (!query.where_expression && !query.group_expression_list && !query.having_expression && !query.order_expression_list
		&& query.limit_length && !need_aggregate && limit_length + limit_offset < block_size)
	{
		block_size = limit_length + limit_offset;
	}

	/// Поток данных.
	BlockInputStreamPtr stream;

	/// Инициализируем изначальный поток данных, на который накладываются преобразования запроса. Таблица или подзапрос?
	if (!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table))
		stream = new AsynchronousBlockInputStream(table->read(required_columns, query_ptr, block_size));
	else
		stream = new AsynchronousBlockInputStream(interpreter_subquery->execute());

	/// Если есть условие WHERE - сначала выполним часть выражения, необходимую для его вычисления
	if (query.where_expression)
	{
		setPartID(query.where_expression, PART_WHERE);
		stream = new AsynchronousBlockInputStream(new ExpressionBlockInputStream(stream, expression, PART_WHERE));
		stream = new AsynchronousBlockInputStream(new FilterBlockInputStream(stream));
	}

	/// Если есть GROUP BY - сначала выполним часть выражения, необходимую для его вычисления
	if (need_aggregate)
	{
		expression->markBeforeAndAfterAggregation(PART_BEFORE_AGGREGATING, PART_AFTER_AGGREGATING);

		if (query.group_expression_list)
			setPartID(query.group_expression_list, PART_GROUP);
		
		stream = new AsynchronousBlockInputStream(new ExpressionBlockInputStream(stream, expression, PART_GROUP | PART_BEFORE_AGGREGATING));
		stream = new AsynchronousBlockInputStream(new AggregatingBlockInputStream(stream, expression));
		stream = new AsynchronousBlockInputStream(new FinalizingAggregatedBlockInputStream(stream));
	}

	/// Если есть условие HAVING - сначала выполним часть выражения, необходимую для его вычисления
	if (query.having_expression)
	{
		setPartID(query.having_expression, PART_HAVING);
		stream = new AsynchronousBlockInputStream(new ExpressionBlockInputStream(stream, expression, PART_HAVING));
		stream = new AsynchronousBlockInputStream(new FilterBlockInputStream(stream));
	}

	/// Выполним оставшуюся часть выражения
	setPartID(query.select_expression_list, PART_SELECT);
	if (query.order_expression_list)
		setPartID(query.order_expression_list, PART_ORDER);
	stream = new AsynchronousBlockInputStream(new ExpressionBlockInputStream(stream, expression, PART_SELECT | PART_ORDER));

	/** Оставим только столбцы, нужные для SELECT и ORDER BY части.
	  * Если нет ORDER BY - то это последняя проекция, и нужно брать только столбцы из SELECT части.
	  */
	stream = new ProjectionBlockInputStream(stream, expression,
		query.order_expression_list ? true : false,
		PART_SELECT | PART_ORDER,
		query.order_expression_list ? NULL : query.select_expression_list);
	
	/// Если есть ORDER BY
	if (query.order_expression_list)
	{
		SortDescription order_descr;
		order_descr.reserve(query.order_expression_list->children.size());
		for (ASTs::iterator it = query.order_expression_list->children.begin();
			it != query.order_expression_list->children.end();
			++it)
		{
			String name = (*it)->children.front()->getColumnName();
			order_descr.push_back(SortColumnDescription(name, dynamic_cast<ASTOrderByElement &>(**it).direction));
		}

		stream = new AsynchronousBlockInputStream(new PartialSortingBlockInputStream(stream, order_descr));
		stream = new AsynchronousBlockInputStream(new MergeSortingBlockInputStream(stream, order_descr));

		/// Оставим только столбцы, нужные для SELECT части
		stream = new ProjectionBlockInputStream(stream, expression, false, PART_SELECT, query.select_expression_list);
	}
	
	/// Если есть LIMIT
	if (query.limit_length)
	{
		stream = new LimitBlockInputStream(stream, limit_length, limit_offset);
	}

	return stream;
}


BlockInputStreamPtr InterpreterSelectQuery::executeAndFormat(WriteBuffer & buf)
{
	FormatFactory format_factory;
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);
	Block sample = getSampleBlock();
	String format_name = query.format ? dynamic_cast<ASTIdentifier &>(*query.format).name : "TabSeparated";

	BlockInputStreamPtr in = execute();
	BlockOutputStreamPtr out = format_factory.getOutput(format_name, buf, sample);
	
	copyData(*in, *out);

	return in;
}


void InterpreterSelectQuery::setPartID(ASTPtr ast, unsigned part_id)
{
	ast->part_id |= part_id;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		setPartID(*it, part_id);
}

}
