#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/ProjectionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/PartialSortingBlockInputStream.h>
#include <DB/DataStreams/MergeSortingBlockInputStream.h>
#include <DB/DataStreams/AggregatingBlockInputStream.h>
#include <DB/DataStreams/FinalizingAggregatedBlockInputStream.h>
#include <DB/DataStreams/MergingAggregatedBlockInputStream.h>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DB/DataStreams/NullBlockInputStream.h>
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


InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, Context & context_, QueryProcessingStage::Enum to_stage_)
	: query_ptr(query_ptr_), query(dynamic_cast<ASTSelectQuery &>(*query_ptr)), context(context_), to_stage(to_stage_)
{
}


StoragePtr InterpreterSelectQuery::getTable()
{
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
	context.columns = !query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table)
		? getTable()->getColumnsList()
		: InterpreterSelectQuery(query.table, context).getSampleBlock().getColumnsList();

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


/// Превращает источник в асинхронный, если это указано.
static inline BlockInputStreamPtr maybeAsynchronous(BlockInputStreamPtr in, bool is_async)
{
	return is_async
		? new AsynchronousBlockInputStream(in)
		: in;
}


BlockInputStreamPtr InterpreterSelectQuery::execute()
{
	/// Добавляем в контекст список доступных столбцов.
	setColumns();
	
	/// Объект, с помощью которого анализируется запрос.
	ExpressionPtr expression = new Expression(query_ptr, context);

	/** Потоки данных. При параллельном выполнении запроса, имеем несколько потоков данных.
	  * Если нет GROUP BY, то выполним все операции до ORDER BY и LIMIT параллельно, затем
	  *  если есть ORDER BY, то склеим потоки с помощью UnionBlockInputStream, а затем MergеSortingBlockInputStream,
	  *  если нет, то склеим с помощью UnionBlockInputStream,
	  *  затем применим LIMIT.
	  * Если есть GROUP BY, то выполним все операции до GROUP BY, включительно, параллельно;
	  *  параллельный GROUP BY склеит потоки в один,
	  *  затем выполним остальные операции с одним получившимся потоком.
	  */
	BlockInputStreams streams;

	/** Вынем данные из Storage. from_stage - до какой стадии запрос был выполнен в Storage. */
	QueryProcessingStage::Enum from_stage = executeFetchColumns(streams, expression);

	/** Если данных нет. */
	if (streams.empty())
		return new NullBlockInputStream;

	std::cerr << QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(to_stage) << std::endl;

	if (to_stage > QueryProcessingStage::FetchColumns)
	{
		/// Нужно ли агрегировать.
		bool need_aggregate = expression->hasAggregates() || query.group_expression_list;
		
		if (from_stage < QueryProcessingStage::WithMergeableState)
		{
			executeWhere(streams, expression);

			if (need_aggregate)
				executeAggregation(streams, expression);
		}

		if (from_stage <= QueryProcessingStage::WithMergeableState
			&& to_stage > QueryProcessingStage::WithMergeableState)
		{
			if (need_aggregate)
			{
				/// Если нужно объединить агрегированные результаты с нескольких серверов
				if (from_stage == QueryProcessingStage::WithMergeableState)
					executeMergeAggregated(streams, expression);
			
				executeFinalizeAggregates(streams, expression);
			}

			executeHaving(streams, expression);
			executeOuterExpression(streams, expression);
			executeOrder(streams, expression);
			executeUnion(streams, expression);
			executeLimit(streams, expression);
		}
	}

	executeUnion(streams, expression);
	return streams[0];
}


static void getLimitLengthAndOffset(ASTSelectQuery & query, size_t & length, size_t & offset)
{
	length = 0;
	offset = 0;
	if (query.limit_length)
	{
		length = boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*query.limit_length).value);
		if (query.limit_offset)
			offset = boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*query.limit_offset).value);
	}
}


QueryProcessingStage::Enum InterpreterSelectQuery::executeFetchColumns(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Таблица, откуда читать данные, если не подзапрос.
	StoragePtr table;
	/// Интерпретатор подзапроса, если подзапрос
	SharedPtr<InterpreterSelectQuery> interpreter_subquery;

	/// Добавляем в контекст список доступных столбцов.
	setColumns();

	if (!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table))
		table = getTable();
	else
		interpreter_subquery = new InterpreterSelectQuery(query.table, context);
	
	/// Список столбцов, которых нужно прочитать, чтобы выполнить запрос.
	Names required_columns = expression->getRequiredColumns();

	/// Если не указан ни один столбец из таблицы, то будем читать первый попавшийся (чтобы хотя бы знать число строк).
	if (required_columns.empty())
		required_columns.push_back(getAnyColumn());

	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	/** Оптимизация - если не указаны WHERE, GROUP, HAVING, ORDER, но указан LIMIT, и limit + offset < max_block_size,
	  *  то в качестве размера блока будем использовать limit + offset (чтобы не читать из таблицы больше, чем запрошено).
	  */
	size_t block_size = context.settings.max_block_size;
	if (!query.where_expression && !query.group_expression_list && !query.having_expression && !query.order_expression_list
		&& query.limit_length && !expression->hasAggregates() && limit_length + limit_offset < block_size)
	{
		block_size = limit_length + limit_offset;
	}

	QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
	
	/// Инициализируем изначальные потоки данных, на которые накладываются преобразования запроса. Таблица или подзапрос?
	if (!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table))
 		streams = table->read(required_columns, query_ptr, from_stage, block_size, context.settings.max_threads);
	else
		streams.push_back(maybeAsynchronous(interpreter_subquery->execute(), context.settings.asynchronous));

	return from_stage;
}


void InterpreterSelectQuery::executeWhere(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Если есть условие WHERE - сначала выполним часть выражения, необходимую для его вычисления
	if (query.where_expression)
	{
		setPartID(query.where_expression, PART_WHERE);

		bool is_async = context.settings.asynchronous && streams.size() <= context.settings.max_threads;
		for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		{
			BlockInputStreamPtr & stream = *it;
			stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression, PART_WHERE), is_async);
			stream = maybeAsynchronous(new FilterBlockInputStream(stream), is_async);
		}
	}
}


void InterpreterSelectQuery::executeAggregation(BlockInputStreams & streams, ExpressionPtr & expression)
{
	expression->markBeforeAndAfterAggregation(PART_BEFORE_AGGREGATING, PART_AFTER_AGGREGATING);

	if (query.group_expression_list)
		setPartID(query.group_expression_list, PART_GROUP);

	bool is_async = context.settings.asynchronous && streams.size() <= context.settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression, PART_GROUP | PART_BEFORE_AGGREGATING), is_async);
	}

	BlockInputStreamPtr & stream = streams[0];

	/** Если истчоников слишком много, то склеим их в один (TODO: в max_threads) источников.
	  * (Иначе агрегация в каждом маленьком источнике, а затем объединение состояний, слишком неэффективна.)
	  */
	if (streams.size() > context.settings.max_threads)
	{
		streams[0] = new UnionBlockInputStream(streams, context.settings.max_threads);
		streams.resize(1);
	}

	/// Если источников несколько, то выполняем параллельную агрегацию
	if (streams.size() > 1)
	{
		stream = maybeAsynchronous(new ParallelAggregatingBlockInputStream(streams, expression, context.settings.max_threads), context.settings.asynchronous);
		streams.resize(1);
	}
	else
		stream = maybeAsynchronous(new AggregatingBlockInputStream(stream, expression), context.settings.asynchronous);
}


void InterpreterSelectQuery::executeFinalizeAggregates(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Финализируем агрегатные функции - заменяем их состояния вычислений на готовые значения
	BlockInputStreamPtr & stream = streams[0];
	stream = maybeAsynchronous(new FinalizingAggregatedBlockInputStream(stream), context.settings.asynchronous);
}


void InterpreterSelectQuery::executeMergeAggregated(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Если объединять нечего
	if (streams.size() == 1)
		return;

	/// Склеим несколько источников в один
	streams[0] = new UnionBlockInputStream(streams, context.settings.max_threads);
	streams.resize(1);

	/// Теперь объединим агрегированные блоки
	streams[0] = maybeAsynchronous(new MergingAggregatedBlockInputStream(streams[0], expression), context.settings.asynchronous);
}


void InterpreterSelectQuery::executeHaving(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Если есть условие HAVING - сначала выполним часть выражения, необходимую для его вычисления
	if (query.having_expression)
	{
		setPartID(query.having_expression, PART_HAVING);

		bool is_async = context.settings.asynchronous && streams.size() <= context.settings.max_threads;
		for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		{
			BlockInputStreamPtr & stream = *it;
			stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression, PART_HAVING), is_async);
			stream = maybeAsynchronous(new FilterBlockInputStream(stream), is_async);
		}
	}
}


void InterpreterSelectQuery::executeOuterExpression(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Выполним оставшуюся часть выражения
	setPartID(query.select_expression_list, PART_SELECT);
	if (query.order_expression_list)
		setPartID(query.order_expression_list, PART_ORDER);

	bool is_async = context.settings.asynchronous && streams.size() <= context.settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression, PART_SELECT | PART_ORDER), is_async);

		/** Оставим только столбцы, нужные для SELECT и ORDER BY части.
		  * Если нет ORDER BY - то это последняя проекция, и нужно брать только столбцы из SELECT части.
		  */
		stream = new ProjectionBlockInputStream(stream, expression,
			query.order_expression_list ? true : false,
			PART_SELECT | PART_ORDER,
			query.order_expression_list ? NULL : query.select_expression_list);
	}
}


void InterpreterSelectQuery::executeOrder(BlockInputStreams & streams, ExpressionPtr & expression)
{
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

		bool is_async = context.settings.asynchronous && streams.size() <= context.settings.max_threads;
		for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		{
			BlockInputStreamPtr & stream = *it;
			stream = maybeAsynchronous(new PartialSortingBlockInputStream(stream, order_descr), is_async);
		}

		BlockInputStreamPtr & stream = streams[0];

		/// Если потоков несколько, то объединяем их в один
		if (streams.size() > 1)
		{
			stream = new UnionBlockInputStream(streams, context.settings.max_threads);
			streams.resize(1);
		}

		/// Сливаем сортированные блоки
		stream = maybeAsynchronous(new MergeSortingBlockInputStream(stream, order_descr), context.settings.asynchronous);

		/// Оставим только столбцы, нужные для SELECT части
		stream = new ProjectionBlockInputStream(stream, expression, false, PART_SELECT, query.select_expression_list);
	}
}


void InterpreterSelectQuery::executeUnion(BlockInputStreams & streams, ExpressionPtr & expression)
{
	/// Если до сих пор есть несколько потоков, то объединяем их в один
	if (streams.size() > 1)
	{
		streams[0] = new UnionBlockInputStream(streams, context.settings.max_threads);
		streams.resize(1);
	}
}


void InterpreterSelectQuery::executeLimit(BlockInputStreams & streams, ExpressionPtr & expression)
{
	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	BlockInputStreamPtr & stream = streams[0];

	/// Если есть LIMIT
	if (query.limit_length)
	{
		stream = new LimitBlockInputStream(stream, limit_length, limit_offset);
	}
}


BlockInputStreamPtr InterpreterSelectQuery::executeAndFormat(WriteBuffer & buf)
{
	Block sample = getSampleBlock();
	String format_name = query.format ? dynamic_cast<ASTIdentifier &>(*query.format).name : "TabSeparated";

	BlockInputStreamPtr in = execute();
	BlockOutputStreamPtr out = context.format_factory->getOutput(format_name, buf, sample);
	
	copyData(*in, *out);

	return in;
}


void InterpreterSelectQuery::setPartID(ASTPtr ast, unsigned part_id)
{
	ast->part_id |= part_id;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		setPartID(*it, part_id);
}


String InterpreterSelectQuery::getAnyColumn()
{
	NamesAndTypesList::const_iterator it = context.columns.begin();

	size_t min_size = it->second->isNumeric() ? it->second->getSizeOfField() : 100;
	String res = it->first;
	for (; it != context.columns.end(); ++it)
	{
		size_t current_size = it->second->isNumeric() ? it->second->getSizeOfField() : 100;
		if (current_size < min_size)
		{
			min_size = current_size;
			res = it->first;
		}
	}

	return res;
}


}
