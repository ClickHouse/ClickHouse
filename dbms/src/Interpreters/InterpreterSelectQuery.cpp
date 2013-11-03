#include <DB/DataStreams/ExpressionBlockInputStream.h>
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
#include <DB/DataStreams/SplittingAggregatingBlockInputStream.h>
#include <DB/DataStreams/DistinctBlockInputStream.h>
#include <DB/DataStreams/NullBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTOrderByElement.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Storages/StorageView.h>


namespace DB
{


InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_, QueryProcessingStage::Enum to_stage_,
	size_t subquery_depth_)
	: query_ptr(query_ptr_), query(dynamic_cast<ASTSelectQuery &>(*query_ptr)),
	context(context_), settings(context.getSettings()), to_stage(to_stage_), subquery_depth(subquery_depth_),
	log(&Logger::get("InterpreterSelectQuery"))
{
	if (settings.limits.max_subquery_depth && subquery_depth > settings.limits.max_subquery_depth)
		throw Exception("Too deep subqueries. Maximum: " + toString(settings.limits.max_subquery_depth),
			ErrorCodes::TOO_DEEP_SUBQUERIES);
	
	context.setColumns(!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table)
		? getTable()->getColumnsList()
		: InterpreterSelectQuery(query.table, context).getSampleBlock().getColumnsList());
	
	if (context.getColumns().empty())
		throw Exception("There are no available columns", ErrorCodes::THERE_IS_NO_COLUMN);
	
	query_analyzer = new ExpressionAnalyzer(query_ptr, context, subquery_depth);
}


void InterpreterSelectQuery::getDatabaseAndTableNames(String & database_name, String & table_name)
{
	/** Если таблица не указана - используем таблицу system.one.
	  * Если база данных не указана - используем текущую базу данных.
	  */
	if (!query.table)
	{
		database_name = "system";
		table_name = "one";
	}
	else if (!query.database)
		database_name = context.getCurrentDatabase();

	if (query.database)
		database_name = dynamic_cast<ASTIdentifier &>(*query.database).name;
	if (query.table)
		table_name = dynamic_cast<ASTIdentifier &>(*query.table).name;
}


StoragePtr InterpreterSelectQuery::getTable()
{
	String database_name;
	String table_name;

	getDatabaseAndTableNames(database_name, table_name);
	return context.getTable(database_name, table_name);
}


ASTPtr InterpreterSelectQuery::getCreateQuery()
{
	String database_name;
	String table_name;

	getDatabaseAndTableNames(database_name, table_name);
	return context.getCreateQuery(database_name, table_name);
}


DataTypes InterpreterSelectQuery::getReturnTypes()
{
	DataTypes res;
	NamesAndTypesList columns = query_analyzer->getSelectSampleBlock().getColumnsList();
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		res.push_back(it->second);
	}
	return res;
}


Block InterpreterSelectQuery::getSampleBlock()
{
	Block block = query_analyzer->getSelectSampleBlock();
	/// создадим ненулевые колонки, чтобы SampleBlock можно было
	/// писать (читать) с помощью BlockOut(In)putStream'ов
	for (size_t i = 0; i < block.columns(); ++i)
	{
		ColumnWithNameAndType & col = block.getByPosition(i);
		col.column = col.type->createColumn();
	}
	return block;
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
	QueryProcessingStage::Enum from_stage = executeFetchColumns(streams);

	/** Если данных нет. */
	if (streams.empty())
		return new NullBlockInputStream;

	LOG_TRACE(log, QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(to_stage));

	if (to_stage > QueryProcessingStage::FetchColumns)
	{
		bool has_where      = false;
		bool need_aggregate = false;
		bool has_having     = false;
		bool has_order_by   = false;
		
		ExpressionActionsPtr before_where;
		ExpressionActionsPtr before_aggregation;
		ExpressionActionsPtr before_having;
		ExpressionActionsPtr before_order_and_select;
		ExpressionActionsPtr final_projection;
		
		/// Сначала составим цепочку действий и запомним нужные шаги из нее.
		
		ExpressionActionsChain chain;
		
		need_aggregate = query_analyzer->hasAggregation();
			
		if (from_stage < QueryProcessingStage::WithMergeableState
			&& to_stage >= QueryProcessingStage::WithMergeableState)
		{
			query_analyzer->appendArrayJoin(chain);
			
			if (query_analyzer->appendWhere(chain))
			{
				has_where = true;
				before_where = chain.getLastActions();
				
				/// Если кроме WHERE ничего выполнять не нужно, пометим все исходные столбцы как нужные, чтобы finalize их не выбросил.
				if (!need_aggregate && to_stage == QueryProcessingStage::WithMergeableState)
				{
					Names columns = query_analyzer->getRequiredColumns();
					chain.getLastStep().required_output.insert(chain.getLastStep().required_output.end(),
															columns.begin(), columns.end());
					
					chain.finalize();
				}
				else
				{
					chain.addStep();
				}
			}
			
			if (need_aggregate)
			{
				query_analyzer->appendGroupBy(chain);
				query_analyzer->appendAggregateFunctionsArguments(chain);
				before_aggregation = chain.getLastActions();
				
				chain.finalize();
				
				chain.clear();
			}
		}
		
		if (from_stage <= QueryProcessingStage::WithMergeableState
			&& to_stage > QueryProcessingStage::WithMergeableState)
		{
			if (need_aggregate && query_analyzer->appendHaving(chain))
			{
				has_having = true;
				before_having = chain.getLastActions();
				chain.addStep();
			}
			
			query_analyzer->appendSelect(chain);
			has_order_by = query_analyzer->appendOrderBy(chain);
			before_order_and_select = chain.getLastActions();
			chain.addStep();
			
			query_analyzer->appendProjectResult(chain);
			final_projection = chain.getLastActions();
			chain.finalize();
			
			/// Если предыдущая стадия запроса выполнялась отдельно, нам могли дать лишних столбцов (например, используемых только в секции WHERE).
			/// Уберем их. Они могут существенно мешать, например, при arrayJoin.
			if (from_stage == QueryProcessingStage::WithMergeableState)
				before_order_and_select->prependProjectInput();
			
			/// Перед выполнением HAVING уберем из блока лишние столбцы (в основном, ключи агрегации).
			if (has_having)
				before_having->prependProjectInput();
		}
		
		/// Теперь составим потоки блоков, выполняющие нужные действия.
		
		if (from_stage < QueryProcessingStage::WithMergeableState
			&& to_stage >= QueryProcessingStage::WithMergeableState)
		{
			if (has_where)
				executeWhere(streams, before_where);
			
			if (need_aggregate)
				executeAggregation(streams, before_aggregation);

			/** Оптимизация - при распределённой обработке запроса, на удалённом сервере,
			  *  если не указаны DISTINCT, GROUP, HAVING, ORDER, но указан LIMIT,
			  *  то выполним предварительный LIMIT на удалёном сервере.
			  */
			if (to_stage == QueryProcessingStage::WithMergeableState
				&& !query.distinct && !need_aggregate && !has_having && !has_order_by
				&& query.limit_length)
			{
				executePreLimit(streams);
			}
		}

		if (from_stage <= QueryProcessingStage::WithMergeableState
			&& to_stage > QueryProcessingStage::WithMergeableState)
		{
			if (need_aggregate)
			{
				/// Если нужно объединить агрегированные результаты с нескольких серверов
				if (from_stage == QueryProcessingStage::WithMergeableState)
					executeMergeAggregated(streams);
				
				executeFinalizeAggregates(streams);
			}
			
			if (has_having)
				executeHaving(streams, before_having);
			
			executeOuterExpression(streams, before_order_and_select);
			
			if (has_order_by)
				executeOrder(streams);
			
			executeProjection(streams, final_projection);
			
			/// Сначала выполняем DISTINCT во всех источниках.
			executeDistinct(streams, true);

			/// На этой стадии можно считать минимумы и максимумы, если надо.
			if (settings.extremes)
				for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
					if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(&**it))
						stream->enableExtremes();

			/** Оптимизация - если источников несколько и есть LIMIT, то сначала применим предварительный LIMIT,
			  * ограничивающий число записей в каждом до offset + limit.
			  */
			if (query.limit_length && streams.size() > 1)
				executePreLimit(streams);
			
			bool need_second_distinct_pass = streams.size() > 1;
			
			executeUnion(streams);
			
			/// Если было более одного источника - то нужно выполнить DISTINCT ещё раз после их слияния.
			if (need_second_distinct_pass)
				executeDistinct(streams, false);
			
			/** NOTE: В некоторых случаях, DISTINCT можно было бы применять раньше
			  *  - до сортировки и, возможно, на удалённых серверах.
			  */

			executeLimit(streams);
		}
	}

	executeUnion(streams);

	/// Ограничения на результат, квота на результат, а также колбек для прогресса.
	if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(&*streams[0]))
	{
		stream->setProgressCallback(context.getProgressCallback());
		stream->setProcessListElement(context.getProcessListElement());
		
		/// Ограничения действуют только на конечный результат.
		if (to_stage == QueryProcessingStage::Complete)
		{
			IProfilingBlockInputStream::LocalLimits limits;
			limits.max_rows_to_read = settings.limits.max_result_rows;
			limits.max_bytes_to_read = settings.limits.max_result_bytes;
			limits.read_overflow_mode = settings.limits.result_overflow_mode;

			stream->setLimits(limits);
			stream->setQuota(context.getQuota(), IProfilingBlockInputStream::QUOTA_RESULT);
		}
	}

	return streams[0];
}


static void getLimitLengthAndOffset(ASTSelectQuery & query, size_t & length, size_t & offset)
{
	length = 0;
	offset = 0;
	if (query.limit_length)
	{
		length = safeGet<UInt64>(dynamic_cast<ASTLiteral &>(*query.limit_length).value);
		if (query.limit_offset)
			offset = safeGet<UInt64>(dynamic_cast<ASTLiteral &>(*query.limit_offset).value);
	}
}


QueryProcessingStage::Enum InterpreterSelectQuery::executeFetchColumns(BlockInputStreams & streams)
{
	/// Таблица, откуда читать данные, если не подзапрос.
	StoragePtr table;
	/// Интерпретатор подзапроса, если подзапрос
	SharedPtr<InterpreterSelectQuery> interpreter_subquery;

	if (!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table))
	{
		table = getTable();
		if (table->getName() == "VIEW")
			query.table = dynamic_cast<StorageView *> (table.get())->getInnerQuery();
	}
	else if (dynamic_cast<ASTSelectQuery *>(&*query.table))
		interpreter_subquery = new InterpreterSelectQuery(query.table, context, QueryProcessingStage::Complete, subquery_depth + 1);

	if (query.sample_size && (!table || !table->supportsSampling()))
		throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);
	
	if (query.final && (!table || !table->supportsFinal()))
		throw Exception("Illegal FINAL", ErrorCodes::ILLEGAL_FINAL);
	
	/** При распределённой обработке запроса, в потоках почти не делается вычислений,
	  *  а делается ожидание и получение данных с удалённых серверов.
	  * Если у нас 20 удалённых серверов, а max_threads = 8, то было бы не очень хорошо
	  *  соединяться и опрашивать только по 8 серверов одновременно.
	  * Чтобы одновременно опрашивалось больше удалённых серверов,
	  *  вместо max_threads используется max_distributed_connections.
	  *
	  * Сохраним изначальное значение max_threads в settings_for_storage
	  *  - эти настройки будут переданы на удалённые серверы при распределённой обработке запроса,
	  *  и там должно быть оригинальное значение max_threads, а не увеличенное.
	  */
	Settings settings_for_storage = settings;
	if (table && table->isRemote())
		settings.max_threads = settings.max_distributed_connections;
	
	/// Список столбцов, которых нужно прочитать, чтобы выполнить запрос.
	Names required_columns = query_analyzer->getRequiredColumns();

	/// Ограничение на количество столбцов для чтения.
	if (settings.limits.max_columns_to_read && required_columns.size() > settings.limits.max_columns_to_read)
		throw Exception("Limit for number of columns to read exceeded. "
			"Requested: " + toString(required_columns.size())
			+ ", maximum: " + toString(settings.limits.max_columns_to_read),
			ErrorCodes::TOO_MUCH_COLUMNS);

	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	/** Оптимизация - если не указаны DISTINCT, WHERE, GROUP, HAVING, ORDER, но указан LIMIT, и limit + offset < max_block_size,
	  *  то в качестве размера блока будем использовать limit + offset (чтобы не читать из таблицы больше, чем запрошено),
	  *  а также установим количество потоков в 1 и отменим асинхронное выполнение конвейера запроса.
	  */
	if (!query.distinct && !query.where_expression && !query.group_expression_list && !query.having_expression && !query.order_expression_list
		&& query.limit_length && !query_analyzer->hasAggregation() && limit_length + limit_offset < settings.max_block_size)
	{
		settings.max_block_size = limit_length + limit_offset;
		settings.max_threads = 1;
		settings.asynchronous = false;
	}

	QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;
	
	/// Инициализируем изначальные потоки данных, на которые накладываются преобразования запроса. Таблица или подзапрос?
	if (!query.table || !dynamic_cast<ASTSelectQuery *>(&*query.table))
 		streams = table->read(required_columns, query_ptr, settings_for_storage, from_stage, settings.max_block_size, settings.max_threads);
	else
		streams.push_back(maybeAsynchronous(interpreter_subquery->execute(), settings.asynchronous));

	/** Если истчоников слишком много, то склеим их в max_threads источников.
	  * (Иначе действия в каждом маленьком источнике, а затем объединение состояний, слишком неэффективно.)
	  */
	if (streams.size() > settings.max_threads)
		streams = narrowBlockInputStreams(streams, settings.max_threads);

	/** Установка ограничений и квоты на чтение данных, скорость и время выполнения запроса.
	  * Такие ограничения проверяются на сервере-инициаторе запроса, а не на удалённых серверах.
	  * Потому что сервер-инициатор имеет суммарные данные о выполнении запроса на всех серверах.
	  */
	if (table && to_stage == QueryProcessingStage::Complete)
	{
		IProfilingBlockInputStream::LocalLimits limits;
		limits.max_rows_to_read = settings.limits.max_rows_to_read;
		limits.max_bytes_to_read = settings.limits.max_bytes_to_read;
		limits.read_overflow_mode = settings.limits.read_overflow_mode;
		limits.max_execution_time = settings.limits.max_execution_time;
		limits.timeout_overflow_mode = settings.limits.timeout_overflow_mode;
		limits.min_execution_speed = settings.limits.min_execution_speed;
		limits.timeout_before_checking_execution_speed = settings.limits.timeout_before_checking_execution_speed;

		QuotaForIntervals & quota = context.getQuota();
		
		for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		{
			if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(&**it))
			{
				stream->setLimits(limits);
				stream->setQuota(quota, IProfilingBlockInputStream::QUOTA_READ);
			}
		}
	}

	return from_stage;
}


void InterpreterSelectQuery::executeWhere(BlockInputStreams & streams, ExpressionActionsPtr expression)
{
	bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression), is_async);
		stream = maybeAsynchronous(new FilterBlockInputStream(stream, query.where_expression->getColumnName()), is_async);
	}
}


void InterpreterSelectQuery::executeAggregation(BlockInputStreams & streams, ExpressionActionsPtr expression)
{
	bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression), is_async);
	}

	BlockInputStreamPtr & stream = streams[0];

	Names key_names;
	AggregateDescriptions aggregates;
	query_analyzer->getAggregateInfo(key_names, aggregates);

	/// TODO: Оптимизация для случая, когда есть LIMIT, но нет HAVING и ORDER BY.

	bool separate_totals = to_stage > QueryProcessingStage::WithMergeableState;
	
	/// Если источников несколько, то выполняем параллельную агрегацию
	if (streams.size() > 1)
	{
		stream = maybeAsynchronous(new ParallelAggregatingBlockInputStream(streams, key_names, aggregates, query.group_by_with_totals, separate_totals,
			settings.max_threads, settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode), settings.asynchronous);
		streams.resize(1);

	/*	stream = maybeAsynchronous(
			(key_names.empty()
				? new ParallelAggregatingBlockInputStream(streams, key_names, aggregates, query.group_by_with_totals, separate_totals,
					settings.max_threads, settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode)
				: new SplittingAggregatingBlockInputStream(
					new UnionBlockInputStream(streams, settings.max_threads), key_names, aggregates, settings.max_threads)),
			settings.asynchronous);
		
		streams.resize(1);*/
	}
	else
		stream = maybeAsynchronous(new AggregatingBlockInputStream(stream, key_names, aggregates, query.group_by_with_totals, separate_totals,
			settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode), settings.asynchronous);
}


void InterpreterSelectQuery::executeFinalizeAggregates(BlockInputStreams & streams)
{
	Names key_names;
	AggregateDescriptions aggregates;
	query_analyzer->getAggregateInfo(key_names, aggregates);
	
	/// Финализируем агрегатные функции - заменяем их состояния вычислений на готовые значения
	BlockInputStreamPtr & stream = streams[0];
	stream = maybeAsynchronous(new FinalizingAggregatedBlockInputStream(stream, aggregates), settings.asynchronous);
}


void InterpreterSelectQuery::executeMergeAggregated(BlockInputStreams & streams)
{
	/// Если объединять нечего
	if (streams.size() == 1)
		return;

	/// Склеим несколько источников в один
	streams[0] = new UnionBlockInputStream(streams, settings.max_threads);
	streams.resize(1);

	bool separate_totals = to_stage > QueryProcessingStage::WithMergeableState;

	/// Теперь объединим агрегированные блоки
	Names key_names;
	AggregateDescriptions aggregates;
	query_analyzer->getAggregateInfo(key_names, aggregates);
	streams[0] = maybeAsynchronous(new MergingAggregatedBlockInputStream(streams[0], key_names, aggregates, query.group_by_with_totals, separate_totals), settings.asynchronous);
}


void InterpreterSelectQuery::executeHaving(BlockInputStreams & streams, ExpressionActionsPtr expression)
{
	bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression), is_async);
		stream = maybeAsynchronous(new FilterBlockInputStream(stream, query.having_expression->getColumnName()), is_async);
	}
}


void InterpreterSelectQuery::executeOuterExpression(BlockInputStreams & streams, ExpressionActionsPtr expression)
{
	bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression), is_async);
	}
}


void InterpreterSelectQuery::executeOrder(BlockInputStreams & streams)
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

	/// Если есть LIMIT - можно делать частичную сортировку.
	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);
	size_t limit = limit_length + limit_offset;

	bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		IProfilingBlockInputStream * sorting_stream = new PartialSortingBlockInputStream(stream, order_descr, limit);

		/// Ограничения на сортировку
		IProfilingBlockInputStream::LocalLimits limits;
		limits.max_rows_to_read = settings.limits.max_rows_to_sort;
		limits.max_bytes_to_read = settings.limits.max_bytes_to_sort;
		limits.read_overflow_mode = settings.limits.sort_overflow_mode;
		sorting_stream->setLimits(limits);
			
		stream = maybeAsynchronous(sorting_stream, is_async);
	}

	BlockInputStreamPtr & stream = streams[0];

	/// Если потоков несколько, то объединяем их в один
	if (streams.size() > 1)
	{
		stream = new UnionBlockInputStream(streams, settings.max_threads);
		streams.resize(1);
	}

	/// Сливаем сортированные блоки TODO: таймаут на слияние.
	stream = maybeAsynchronous(new MergeSortingBlockInputStream(stream, order_descr, limit), is_async);
}


void InterpreterSelectQuery::executeProjection(BlockInputStreams & streams, ExpressionActionsPtr expression)
{
	bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
	for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
	{
		BlockInputStreamPtr & stream = *it;
		stream = maybeAsynchronous(new ExpressionBlockInputStream(stream, expression), is_async);
	}
}


void InterpreterSelectQuery::executeDistinct(BlockInputStreams & streams, bool before_order)
{
	if (query.distinct)
	{
		size_t limit_length = 0;
		size_t limit_offset = 0;
		getLimitLengthAndOffset(query, limit_length, limit_offset);

		size_t limit_for_distinct = 0;

		/// Если после этой стадии DISTINCT не будет выполняться ORDER BY, то можно достать не более limit_length + limit_offset различных строк.
		if (!query.order_expression_list || !before_order)
			limit_for_distinct = limit_length + limit_offset;

		bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
		for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		{
			BlockInputStreamPtr & stream = *it;
			stream = maybeAsynchronous(new DistinctBlockInputStream(stream, settings.limits, limit_for_distinct), is_async);
		}
	}
}


void InterpreterSelectQuery::executeUnion(BlockInputStreams & streams)
{
	/// Если до сих пор есть несколько потоков, то объединяем их в один
	if (streams.size() > 1)
	{
		streams[0] = new UnionBlockInputStream(streams, settings.max_threads);
		streams.resize(1);
	}
}


/// Предварительный LIMIT - применяется в каждом источнике, если источников несколько, до их объединения.
void InterpreterSelectQuery::executePreLimit(BlockInputStreams & streams)
{
	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	/// Если есть LIMIT
	if (query.limit_length)
	{
		for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
		{
			BlockInputStreamPtr & stream = *it;
			stream = new LimitBlockInputStream(stream, limit_length + limit_offset, 0);
		}
	}
}


void InterpreterSelectQuery::executeLimit(BlockInputStreams & streams)
{
	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	/// Если есть LIMIT
	if (query.limit_length)
	{
		BlockInputStreamPtr & stream = streams[0];
		stream = new LimitBlockInputStream(stream, limit_length, limit_offset);
	}
}


BlockInputStreamPtr InterpreterSelectQuery::executeAndFormat(WriteBuffer & buf)
{
	Block sample = getSampleBlock();
	String format_name = query.format ? dynamic_cast<ASTIdentifier &>(*query.format).name : context.getDefaultFormat();

	BlockInputStreamPtr in = execute();
	BlockOutputStreamPtr out = context.getFormatFactory().getOutput(format_name, buf, sample);

	copyData(*in, *out);

	return in;
}


}
