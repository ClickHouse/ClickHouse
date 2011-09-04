#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/ProjectionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/PartialSortingBlockInputStream.h>
#include <DB/DataStreams/MergeSortingBlockInputStream.h>

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


DataTypes InterpreterSelectQuery::getReturnTypes()
{
	context.columns = getTable()->getColumns();
	Expression expression(dynamic_cast<ASTSelectQuery &>(*query_ptr).select_expression_list, context);
	return expression.getReturnTypes();
}


BlockInputStreamPtr InterpreterSelectQuery::execute()
{
	ASTSelectQuery & query = dynamic_cast<ASTSelectQuery &>(*query_ptr);

	StoragePtr table = getTable();
	
	/// Какие столбцы читать из этой таблицы

	context.columns = table->getColumns();
	Poco::SharedPtr<Expression> expression = new Expression(query_ptr, context);
	Names required_columns = expression->getRequiredColumns();

	/// Если не указан ни один столбец из таблицы, то будем читать первый попавшийся (чтобы хотя бы знать число строк).
	if (required_columns.empty())
		required_columns.push_back(table->getColumns().begin()->first);

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
		&& query.limit_length && limit_length + limit_offset < block_size)
	{
		block_size = limit_length + limit_offset;
	}

	BlockInputStreamPtr stream = table->read(required_columns, query_ptr, block_size);

	/// Если есть условие WHERE - сначала выполним часть выражения, необходимую для его вычисления
	if (query.where_expression)
	{
		setPartID(query.where_expression, PART_WHERE);
		stream = new ExpressionBlockInputStream(stream, expression, PART_WHERE);
		stream = new FilterBlockInputStream(stream);
	}

	/// Выполним оставшуюся часть выражения
	setPartID(query.select_expression_list, PART_SELECT);
	if (query.order_expression_list)
		setPartID(query.order_expression_list, PART_ORDER);
	stream = new ExpressionBlockInputStream(stream, expression, PART_SELECT | PART_ORDER);
	stream = new ProjectionBlockInputStream(stream, expression, true, PART_SELECT | PART_ORDER);
	
	/// Если есть ORDER BY
	if (query.order_expression_list)
	{
		SortDescription order_descr;
		order_descr.reserve(query.order_expression_list->children.size());
		for (ASTs::iterator it = query.order_expression_list->children.begin();
			it != query.order_expression_list->children.end();
			++it)
		{
			ASTPtr elem = (*it)->children.front();
			ASTIdentifier * id_elem = dynamic_cast<ASTIdentifier *>(&*elem);
			ASTFunction * id_func = dynamic_cast<ASTFunction *>(&*elem);

			String name = id_elem ? id_elem->name : elem->getTreeID();
			if (id_func)
				name += "_0";
			
			order_descr.push_back(SortColumnDescription(name, dynamic_cast<ASTOrderByElement &>(**it).direction));
		}

		stream = new PartialSortingBlockInputStream(stream, order_descr);
		stream = new MergeSortingBlockInputStream(stream, order_descr);
	}
	
	/// Удалим ненужные больше столбцы
	stream = new ProjectionBlockInputStream(stream, expression, false, PART_SELECT);

	/// Если есть LIMIT
	if (query.limit_length)
	{
		stream = new LimitBlockInputStream(stream, limit_length, limit_offset);
	}

	return stream;
}


void InterpreterSelectQuery::setPartID(ASTPtr ast, unsigned part_id)
{
	ast->part_id |= part_id;
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		setPartID(*it, part_id);
}

}
