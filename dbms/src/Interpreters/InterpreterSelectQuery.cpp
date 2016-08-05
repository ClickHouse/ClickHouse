#include <experimental/optional>

#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/PartialSortingBlockInputStream.h>
#include <DB/DataStreams/MergeSortingBlockInputStream.h>
#include <DB/DataStreams/MergingSortedBlockInputStream.h>
#include <DB/DataStreams/AggregatingBlockInputStream.h>
#include <DB/DataStreams/MergingAggregatedBlockInputStream.h>
#include <DB/DataStreams/MergingAggregatedMemoryEfficientBlockInputStream.h>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DB/DataStreams/DistinctBlockInputStream.h>
#include <DB/DataStreams/NullBlockInputStream.h>
#include <DB/DataStreams/TotalsHavingBlockInputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/CreatingSetsBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/ConcatBlockInputStream.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTOrderByElement.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterSetQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/TableFunctions/ITableFunction.h>
#include <DB/TableFunctions/TableFunctionFactory.h>

#include <DB/Core/Field.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int TOO_DEEP_SUBQUERIES;
	extern const int THERE_IS_NO_COLUMN;
	extern const int UNION_ALL_RESULT_STRUCTURES_MISMATCH;
	extern const int SAMPLING_NOT_SUPPORTED;
	extern const int ILLEGAL_FINAL;
	extern const int ILLEGAL_PREWHERE;
	extern const int TOO_MUCH_COLUMNS;
}


InterpreterSelectQuery::~InterpreterSelectQuery() = default;


void InterpreterSelectQuery::init(BlockInputStreamPtr input, const Names & required_column_names)
{
	ProfileEvents::increment(ProfileEvents::SelectQuery);

	initSettings();

	original_max_threads = settings.max_threads;

	if (settings.limits.max_subquery_depth && subquery_depth > settings.limits.max_subquery_depth)
		throw Exception("Too deep subqueries. Maximum: " + toString(settings.limits.max_subquery_depth),
			ErrorCodes::TOO_DEEP_SUBQUERIES);

	if (is_first_select_inside_union_all)
	{
		/// Создать цепочку запросов SELECT.
		InterpreterSelectQuery * interpreter = this;
		ASTPtr tail = query.next_union_all;

		while (tail)
		{
			ASTPtr head = tail;

			ASTSelectQuery & head_query = static_cast<ASTSelectQuery &>(*head);
			tail = head_query.next_union_all;

			interpreter->next_select_in_union_all.reset(new InterpreterSelectQuery(head, context, to_stage, subquery_depth));
			interpreter = interpreter->next_select_in_union_all.get();
		}
	}

	if (is_first_select_inside_union_all && hasAsterisk())
	{
		basicInit(input);

		// Мы выполняем этот код именно здесь, потому что в противном случае следующего рода запрос бы не срабатывал:
		// SELECT X FROM (SELECT * FROM (SELECT 1 AS X, 2 AS Y) UNION ALL SELECT 3, 4)
		// из-за того, что астериски заменены столбцами только при создании объектов query_analyzer в basicInit().
		renameColumns();

		if (!required_column_names.empty() && (table_column_names.size() != required_column_names.size()))
		{
			rewriteExpressionList(required_column_names);
			/// Теперь имеется устаревшая информация для выполнения запроса. Обновляем эту информацию.
			initQueryAnalyzer();
		}
	}
	else
	{
		renameColumns();
		if (!required_column_names.empty())
			rewriteExpressionList(required_column_names);

		basicInit(input);
	}
}

void InterpreterSelectQuery::basicInit(BlockInputStreamPtr input_)
{
	auto query_table = query.table();

	if (query_table && typeid_cast<ASTSelectQuery *>(query_table.get()))
	{
		if (table_column_names.empty())
		{
			table_column_names = InterpreterSelectQuery::getSampleBlock(query_table, context).getColumnsList();
		}
	}
	else
	{
		if (query_table && typeid_cast<const ASTFunction *>(query_table.get()))
		{
			/// Получить табличную функцию
			TableFunctionPtr table_function_ptr = context.getTableFunctionFactory().get(typeid_cast<const ASTFunction *>(query_table.get())->name, context);
			/// Выполнить ее и запомнить результат
			storage = table_function_ptr->execute(query_table, context);
		}
		else
		{
			String database_name;
			String table_name;

			getDatabaseAndTableNames(database_name, table_name);

			storage = context.getTable(database_name, table_name);
		}

		table_lock = storage->lockStructure(false);
		if (table_column_names.empty())
			table_column_names = storage->getColumnsListNonMaterialized();
	}

	if (table_column_names.empty())
		throw Exception("There are no available columns", ErrorCodes::THERE_IS_NO_COLUMN);

	query_analyzer.reset(new ExpressionAnalyzer(query_ptr, context, storage, table_column_names, subquery_depth, !only_analyze));

	/// Сохраняем в query context новые временные таблицы
	for (auto & it : query_analyzer->getExternalTables())
		if (!context.tryGetExternalTable(it.first))
			context.addExternalTable(it.first, it.second);

	if (input_)
		streams.push_back(input_);

	if (is_first_select_inside_union_all)
	{
		/// Проверяем, что результаты всех запросов SELECT cовместимые.
		Block first = getSampleBlock();
		for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
		{
			Block current = p->getSampleBlock();
			if (!blocksHaveEqualStructure(first, current))
				throw Exception("Result structures mismatch in the SELECT queries of the UNION ALL chain. Found result structure:\n\n" + current.dumpStructure()
				+ "\n\nwhile expecting:\n\n" + first.dumpStructure() + "\n\ninstead",
				ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);
		}
	}
}

void InterpreterSelectQuery::initQueryAnalyzer()
{
	query_analyzer.reset(
		new ExpressionAnalyzer(query_ptr, context, storage, table_column_names, subquery_depth, !only_analyze));

	for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
		p->query_analyzer.reset(
			new ExpressionAnalyzer(p->query_ptr, p->context, p->storage, p->table_column_names, p->subquery_depth, !only_analyze));
}

InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_, QueryProcessingStage::Enum to_stage_,
	size_t subquery_depth_, BlockInputStreamPtr input_)
	: query_ptr(query_ptr_), query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
	context(context_), to_stage(to_stage_), subquery_depth(subquery_depth_),
	is_first_select_inside_union_all(query.isUnionAllHead()),
	log(&Logger::get("InterpreterSelectQuery"))
{
	init(input_);
}

InterpreterSelectQuery::InterpreterSelectQuery(OnlyAnalyzeTag, ASTPtr query_ptr_, const Context & context_)
	: query_ptr(query_ptr_), query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
	context(context_), to_stage(QueryProcessingStage::Complete), subquery_depth(0),
	is_first_select_inside_union_all(false), only_analyze(true),
	log(&Logger::get("InterpreterSelectQuery"))
{
	init({});
}

InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_,
	const Names & required_column_names_,
	QueryProcessingStage::Enum to_stage_, size_t subquery_depth_, BlockInputStreamPtr input_)
	: InterpreterSelectQuery(query_ptr_, context_, required_column_names_, {}, to_stage_, subquery_depth_, input_)
{
}

InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_,
	const Names & required_column_names_,
	const NamesAndTypesList & table_column_names_, QueryProcessingStage::Enum to_stage_, size_t subquery_depth_, BlockInputStreamPtr input_)
	: query_ptr(query_ptr_), query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
	context(context_), to_stage(to_stage_), subquery_depth(subquery_depth_), table_column_names(table_column_names_),
	is_first_select_inside_union_all(query.isUnionAllHead()),
	log(&Logger::get("InterpreterSelectQuery"))
{
	init(input_, required_column_names_);
}

bool InterpreterSelectQuery::hasAsterisk() const
{
	if (query.hasAsterisk())
		return true;

	if (is_first_select_inside_union_all)
	{
		for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
		{
			if (p->query.hasAsterisk())
				return true;
		}
	}

	return false;
}

void InterpreterSelectQuery::renameColumns()
{
	if (is_first_select_inside_union_all)
	{
		for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
			p->query.renameColumns(query);
	}
}

void InterpreterSelectQuery::rewriteExpressionList(const Names & required_column_names)
{
	if (query.distinct)
		return;

	if (is_first_select_inside_union_all)
	{
		for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
		{
			if (p->query.distinct)
				return;
		}
	}

	query.rewriteSelectExpressionList(required_column_names);

	if (is_first_select_inside_union_all)
	{
		for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
			p->query.rewriteSelectExpressionList(required_column_names);
	}
}

void InterpreterSelectQuery::getDatabaseAndTableNames(String & database_name, String & table_name)
{
	auto query_database = query.database();
	auto query_table = query.table();

	/** Если таблица не указана - используем таблицу system.one.
	 *  Если база данных не указана - используем текущую базу данных.
	 */
	if (query_database)
		database_name = typeid_cast<ASTIdentifier &>(*query_database).name;
	if (query_table)
		table_name = typeid_cast<ASTIdentifier &>(*query_table).name;

	if (!query_table)
	{
		database_name = "system";
		table_name = "one";
	}
	else if (!query_database)
	{
		if (context.tryGetTable("", table_name))
			database_name = "";
		else
			database_name = context.getCurrentDatabase();
	}
}


DataTypes InterpreterSelectQuery::getReturnTypes()
{
	DataTypes res;
	NamesAndTypesList columns = query_analyzer->getSelectSampleBlock().getColumnsList();
	for (auto & column : columns)
		res.push_back(column.type);

	return res;
}


Block InterpreterSelectQuery::getSampleBlock()
{
	Block block = query_analyzer->getSelectSampleBlock();
	/// создадим ненулевые колонки, чтобы SampleBlock можно было
	/// писать (читать) с помощью BlockOut(In)putStream'ов
	for (size_t i = 0; i < block.columns(); ++i)
	{
		ColumnWithTypeAndName & col = block.getByPosition(i);
		col.column = col.type->createColumn();
	}
	return block;
}


Block InterpreterSelectQuery::getSampleBlock(ASTPtr query_ptr_, const Context & context_)
{
	return InterpreterSelectQuery(OnlyAnalyzeTag(), query_ptr_, context_).getSampleBlock();
}


BlockIO InterpreterSelectQuery::execute()
{
	(void) executeWithoutUnion();

	if (hasNoData())
	{
		BlockIO res;
		res.in = std::make_shared<NullBlockInputStream>();
		res.in_sample = getSampleBlock();
		return res;
	}

	executeUnion();

	/// Ограничения на результат, квота на результат, а также колбек для прогресса.
	if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(streams[0].get()))
	{
		/// Ограничения действуют только на конечный результат.
		if (to_stage == QueryProcessingStage::Complete)
		{
			IProfilingBlockInputStream::LocalLimits limits;
			limits.mode = IProfilingBlockInputStream::LIMITS_CURRENT;
			limits.max_rows_to_read = settings.limits.max_result_rows;
			limits.max_bytes_to_read = settings.limits.max_result_bytes;
			limits.read_overflow_mode = settings.limits.result_overflow_mode;

			stream->setLimits(limits);
			stream->setQuota(context.getQuota());
		}
	}

	BlockIO res;
	res.in = streams[0];
	res.in_sample = getSampleBlock();

	return res;
}

const BlockInputStreams & InterpreterSelectQuery::executeWithoutUnion()
{
	if (is_first_select_inside_union_all)
	{
		executeSingleQuery();
		for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
		{
			p->executeSingleQuery();
			const auto & others = p->streams;
			streams.insert(streams.end(), others.begin(), others.end());
		}

		transformStreams([&](auto & stream)
		{
			stream = std::make_shared<MaterializingBlockInputStream>(stream);
		});
	}
	else
		executeSingleQuery();

	return streams;
}

void InterpreterSelectQuery::executeSingleQuery()
{
	/** Потоки данных. При параллельном выполнении запроса, имеем несколько потоков данных.
	 *  Если нет GROUP BY, то выполним все операции до ORDER BY и LIMIT параллельно, затем
	 *  если есть ORDER BY, то склеим потоки с помощью UnionBlockInputStream, а затем MergеSortingBlockInputStream,
	 *  если нет, то склеим с помощью UnionBlockInputStream,
	 *  затем применим LIMIT.
	 *  Если есть GROUP BY, то выполним все операции до GROUP BY, включительно, параллельно;
	 *  параллельный GROUP BY склеит потоки в один,
	 *  затем выполним остальные операции с одним получившимся потоком.
	 *  Если запрос является членом цепочки UNION ALL и не содержит GROUP BY, ORDER BY, DISTINCT, или LIMIT,
	 *  то объединение источников данных выполняется не на этом уровне, а на верхнем уровне.
	 */

	union_within_single_query = false;

	/** Вынем данные из Storage. from_stage - до какой стадии запрос был выполнен в Storage. */
	QueryProcessingStage::Enum from_stage = executeFetchColumns();

	LOG_TRACE(log, QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(to_stage));

	if (to_stage > QueryProcessingStage::FetchColumns)
	{
		bool has_join		= false;
		bool has_where      = false;
		bool need_aggregate = false;
		bool has_having     = false;
		bool has_order_by   = false;

		ExpressionActionsPtr before_join;	/// включая JOIN
		ExpressionActionsPtr before_where;
		ExpressionActionsPtr before_aggregation;
		ExpressionActionsPtr before_having;
		ExpressionActionsPtr before_order_and_select;
		ExpressionActionsPtr final_projection;

		/// Столбцы из списка SELECT, до переименования в алиасы.
		Names selected_columns;

		/// Нужно ли выполнять первую часть конвейера - выполняемую на удаленных серверах при распределенной обработке.
		bool first_stage = from_stage < QueryProcessingStage::WithMergeableState
			&& to_stage >= QueryProcessingStage::WithMergeableState;
		/// Нужно ли выполнять вторую часть конвейера - выполняемую на сервере-инициаторе при распределенной обработке.
		bool second_stage = from_stage <= QueryProcessingStage::WithMergeableState
			&& to_stage > QueryProcessingStage::WithMergeableState;

		/** Сначала составим цепочку действий и запомним нужные шаги из нее.
		 *  Независимо от from_stage и to_stage составим полную последовательность действий, чтобы выполнять оптимизации и
		 *  выбрасывать ненужные столбцы с учетом всего запроса. В ненужных частях запроса не будем выполнять подзапросы.
		 */

		{
			ExpressionActionsChain chain;

			need_aggregate = query_analyzer->hasAggregation();

			query_analyzer->appendArrayJoin(chain, !first_stage);

			if (query_analyzer->appendJoin(chain, !first_stage))
			{
				has_join = true;
				before_join = chain.getLastActions();
				chain.addStep();

				const ASTTableJoin & join = static_cast<const ASTTableJoin &>(*query.join()->table_join);
				if (join.kind == ASTTableJoin::Kind::Full || join.kind == ASTTableJoin::Kind::Right)
					stream_with_non_joined_data = before_join->createStreamWithNonJoinedDataIfFullOrRightJoin(settings.max_block_size);
			}

			if (query_analyzer->appendWhere(chain, !first_stage))
			{
				has_where = true;
				before_where = chain.getLastActions();
				chain.addStep();
			}

			if (need_aggregate)
			{
				query_analyzer->appendGroupBy(chain, !first_stage);
				query_analyzer->appendAggregateFunctionsArguments(chain, !first_stage);
				before_aggregation = chain.getLastActions();

				chain.finalize();
				chain.clear();

				if (query_analyzer->appendHaving(chain, !second_stage))
				{
					has_having = true;
					before_having = chain.getLastActions();
					chain.addStep();
				}
			}

			/// Если есть агрегация, выполняем выражения в SELECT и ORDER BY на инициировавшем сервере, иначе - на серверах-источниках.
			query_analyzer->appendSelect(chain, need_aggregate ? !second_stage : !first_stage);
			selected_columns = chain.getLastStep().required_output;
			has_order_by = query_analyzer->appendOrderBy(chain, need_aggregate ? !second_stage : !first_stage);
			before_order_and_select = chain.getLastActions();
			chain.addStep();

			query_analyzer->appendProjectResult(chain, !second_stage);
			final_projection = chain.getLastActions();

			chain.finalize();
			chain.clear();
		}

		/** Если данных нет.
		 *  Эта проверка специально вынесена чуть ниже, чем она могла бы быть (сразу после executeFetchColumns),
		 *  чтобы запрос был проанализирован, и в нём могли бы быть обнаружены ошибки (например, несоответствия типов).
		 *  Иначе мог бы вернуться пустой результат на некорректный запрос.
		 */
		if (hasNoData())
			return;

		/// Перед выполнением WHERE и HAVING уберем из блока лишние столбцы (в основном, ключи агрегации).
		if (has_where)
			before_where->prependProjectInput();
		if (has_having)
			before_having->prependProjectInput();

		/// Теперь составим потоки блоков, выполняющие нужные действия.

		/// Нужно ли агрегировать в отдельную строку строки, не прошедшие max_rows_to_group_by.
		bool aggregate_overflow_row =
			need_aggregate &&
			query.group_by_with_totals &&
			settings.limits.max_rows_to_group_by &&
			settings.limits.group_by_overflow_mode == OverflowMode::ANY &&
			settings.totals_mode != TotalsMode::AFTER_HAVING_EXCLUSIVE;

		/// Нужно ли после агрегации сразу финализировать агрегатные функции.
		bool aggregate_final =
			need_aggregate &&
			to_stage > QueryProcessingStage::WithMergeableState &&
			!query.group_by_with_totals;

		if (first_stage)
		{
			if (has_join)
				for (auto & stream : streams)	/// Применяем ко всем источникам кроме stream_with_non_joined_data.
					stream = std::make_shared<ExpressionBlockInputStream>(stream, before_join);

			if (has_where)
				executeWhere(before_where);

			if (need_aggregate)
				executeAggregation(before_aggregation, aggregate_overflow_row, aggregate_final);
			else
			{
				executeExpression(before_order_and_select);
				executeDistinct(true, selected_columns);
			}

			/** При распределённой обработке запроса,
			  *  если не указаны GROUP, HAVING,
			  *  но есть ORDER или LIMIT,
			  *  то выполним предварительную сортировку и LIMIT на удалёном сервере.
			  */
			if (!second_stage
				&& !need_aggregate && !has_having)
			{
				if (has_order_by)
					executeOrder();

				if (has_order_by && query.limit_length)
					executeDistinct(false, selected_columns);

				if (query.limit_length)
					executePreLimit();
			}
		}

		if (second_stage)
		{
			bool need_second_distinct_pass = query.distinct;

			if (need_aggregate)
			{
				/// Если нужно объединить агрегированные результаты с нескольких серверов
				if (!first_stage)
					executeMergeAggregated(aggregate_overflow_row, aggregate_final);

				if (!aggregate_final)
					executeTotalsAndHaving(has_having, before_having, aggregate_overflow_row);
				else if (has_having)
					executeHaving(before_having);

				executeExpression(before_order_and_select);
				executeDistinct(true, selected_columns);

				need_second_distinct_pass = query.distinct && hasMoreThanOneStream();
			}
			else if (query.group_by_with_totals && !aggregate_final)
			{
				executeTotalsAndHaving(false, nullptr, aggregate_overflow_row);
			}

			if (has_order_by)
			{
				/** Если при распределённой обработке запроса есть ORDER BY,
				  *  но нет агрегации, то на удалённых серверах был сделан ORDER BY
				  *  - поэтому, делаем merge сортированных потоков с удалённых серверов.
				  */
				if (!first_stage && !need_aggregate && !(query.group_by_with_totals && !aggregate_final))
					executeMergeSorted();
				else	/// Иначе просто сортировка.
					executeOrder();
			}

			executeProjection(final_projection);

			/// На этой стадии можно считать минимумы и максимумы, если надо.
			if (settings.extremes)
			{
				transformStreams([&](auto & stream)
				{
					if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
						p_stream->enableExtremes();
				});
			}

			/** Оптимизация - если источников несколько и есть LIMIT, то сначала применим предварительный LIMIT,
				* ограничивающий число записей в каждом до offset + limit.
				*/
			if (query.limit_length && hasMoreThanOneStream() && !query.distinct)
				executePreLimit();

			if (need_second_distinct_pass)
				union_within_single_query = true;

			if (union_within_single_query || stream_with_non_joined_data)
				executeUnion();

			if (streams.size() == 1)
			{
				/// Если было более одного источника - то нужно выполнить DISTINCT ещё раз после их слияния.
				if (need_second_distinct_pass)
					executeDistinct(false, Names());

				executeLimit();
			}
		}
	}

	/** Если данных нет. */
	if (hasNoData())
		return;

	SubqueriesForSets subqueries_for_sets = query_analyzer->getSubqueriesForSets();
	if (!subqueries_for_sets.empty())
		executeSubqueriesInSetsAndJoins(subqueries_for_sets);
}


static void getLimitLengthAndOffset(ASTSelectQuery & query, size_t & length, size_t & offset)
{
	length = 0;
	offset = 0;
	if (query.limit_length)
	{
		length = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_length).value);
		if (query.limit_offset)
			offset = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*query.limit_offset).value);
	}
}

QueryProcessingStage::Enum InterpreterSelectQuery::executeFetchColumns()
{
	if (!hasNoData())
		return QueryProcessingStage::FetchColumns;

	/// Интерпретатор подзапроса, если подзапрос
	std::experimental::optional<InterpreterSelectQuery> interpreter_subquery;

	/// Список столбцов, которых нужно прочитать, чтобы выполнить запрос.
	Names required_columns = query_analyzer->getRequiredColumns();
	/// Действия для вычисления ALIAS, если потребуется.
	ExpressionActionsPtr alias_actions;
	/// Требуются ли ALIAS столбцы для выполнения запроса?
	auto alias_columns_required = false;

	if (storage && !storage->alias_columns.empty())
	{
		for (const auto & column : required_columns)
		{
			const auto default_it = storage->column_defaults.find(column);
			if (default_it != std::end(storage->column_defaults) && default_it->second.type == ColumnDefaultType::Alias)
			{
				alias_columns_required = true;
				break;
			}
		}

		if (alias_columns_required)
		{
			/// Составим выражение для возврата всех запрошенных столбцов, с вычислением требуемых ALIAS столбцов.
			auto required_columns_expr_list = std::make_shared<ASTExpressionList>();

			for (const auto & column : required_columns)
			{
				const auto default_it = storage->column_defaults.find(column);
				if (default_it != std::end(storage->column_defaults) && default_it->second.type == ColumnDefaultType::Alias)
					required_columns_expr_list->children.emplace_back(setAlias(default_it->second.expression->clone(), column));
				else
					required_columns_expr_list->children.emplace_back(std::make_shared<ASTIdentifier>(StringRange(), column));
			}

			alias_actions = ExpressionAnalyzer{required_columns_expr_list, context, storage, table_column_names}.getActions(true);

			/// Множество требуемых столбцов могло быть дополнено в результате добавления действия для вычисления ALIAS.
			required_columns = alias_actions->getRequiredColumns();
		}
	}

	auto query_table = query.table();
	if (query_table && typeid_cast<ASTSelectQuery *>(query_table.get()))
	{
		/** Для подзапроса не действуют ограничения на максимальный размер результата.
		 *  Так как результат поздапроса - ещё не результат всего запроса.
		 */
		Context subquery_context = context;
		Settings subquery_settings = context.getSettings();
		subquery_settings.limits.max_result_rows = 0;
		subquery_settings.limits.max_result_bytes = 0;
		/// Вычисление extremes не имеет смысла и не нужно (если его делать, то в результате всего запроса могут взяться extremes подзапроса).
		subquery_settings.extremes = 0;
		subquery_context.setSettings(subquery_settings);

		interpreter_subquery.emplace(
			query_table, subquery_context, required_columns, QueryProcessingStage::Complete, subquery_depth + 1);

		/// Если во внешнем запросе есть аггрегация, то WITH TOTALS игнорируется в подзапросе.
		if (query_analyzer->hasAggregation())
			interpreter_subquery->ignoreWithTotals();
	}

	if (query.sample_size() && (!storage || !storage->supportsSampling()))
		throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

	if (query.final() && (!storage || !storage->supportsFinal()))
		throw Exception(storage ? "Storage " + storage->getName() + " doesn't support FINAL" : "Illegal FINAL", ErrorCodes::ILLEGAL_FINAL);

	if (query.prewhere_expression && (!storage || !storage->supportsPrewhere()))
		throw Exception(storage ? "Storage " + storage->getName() + " doesn't support PREWHERE" : "Illegal PREWHERE", ErrorCodes::ILLEGAL_PREWHERE);

	/** При распределённой обработке запроса, в потоках почти не делается вычислений,
	 *  а делается ожидание и получение данных с удалённых серверов.
	 *  Если у нас 20 удалённых серверов, а max_threads = 8, то было бы не очень хорошо
	 *  соединяться и опрашивать только по 8 серверов одновременно.
	 *  Чтобы одновременно опрашивалось больше удалённых серверов,
	 *  вместо max_threads используется max_distributed_connections.
	 *
	 *  Сохраним изначальное значение max_threads в settings_for_storage
	 *  - эти настройки будут переданы на удалённые серверы при распределённой обработке запроса,
	 *  и там должно быть оригинальное значение max_threads, а не увеличенное.
	 */
	bool is_remote = false;
	Settings settings_for_storage = settings;
	if (storage && storage->isRemote())
	{
		is_remote = true;
		settings.max_threads = settings.max_distributed_connections;
	}

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
	 *  а также установим количество потоков в 1.
	 */
	if (!query.distinct
		&& !query.prewhere_expression
		&& !query.where_expression
		&& !query.group_expression_list
		&& !query.having_expression
		&& !query.order_expression_list
		&& query.limit_length
		&& !query_analyzer->hasAggregation()
		&& limit_length + limit_offset < settings.max_block_size)
	{
		settings.max_block_size = limit_length + limit_offset;
		settings.max_threads = 1;
	}

	QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

	query_analyzer->makeSetsForIndex();

	/// Инициализируем изначальные потоки данных, на которые накладываются преобразования запроса. Таблица или подзапрос?
	if (!interpreter_subquery)
	{
		size_t max_streams = settings.max_threads;

		if (max_streams == 0)
			throw Exception("Logical error: zero number of streams requested", ErrorCodes::LOGICAL_ERROR);

		/// Если надо - запрашиваем больше источников, чем количество потоков - для более равномерного распределения работы по потокам.
		if (max_streams > 1 && !is_remote)
			max_streams *= settings.max_streams_to_max_threads_ratio;

		ASTPtr actual_query_ptr;
		if (storage->isRemote())
		{
			/// В случае удаленного запроса отправляем только SELECT, который выполнится.
			actual_query_ptr = query.cloneFirstSelect();
		}
		else
			actual_query_ptr = query_ptr;

		streams = storage->read(required_columns, actual_query_ptr,
			context, settings_for_storage, from_stage,
			settings.max_block_size, max_streams);

		if (alias_actions)
			/// Обернем каждый поток, возвращенный из таблицы, с целью вычисления и добавления ALIAS столбцов
			transformStreams([&] (auto & stream)
			{
				stream = std::make_shared<ExpressionBlockInputStream>(stream, alias_actions);
			});

		transformStreams([&](auto & stream)
		{
			stream->addTableLock(table_lock);
		});
	}
	else
	{
		const auto & subquery_streams = interpreter_subquery->executeWithoutUnion();
		streams.insert(streams.end(), subquery_streams.begin(), subquery_streams.end());
	}

	/** Установка ограничений и квоты на чтение данных, скорость и время выполнения запроса.
	 *  Такие ограничения проверяются на сервере-инициаторе запроса, а не на удалённых серверах.
	 *  Потому что сервер-инициатор имеет суммарные данные о выполнении запроса на всех серверах.
	 */
	if (storage && to_stage == QueryProcessingStage::Complete)
	{
		IProfilingBlockInputStream::LocalLimits limits;
		limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
		limits.max_rows_to_read = settings.limits.max_rows_to_read;
		limits.max_bytes_to_read = settings.limits.max_bytes_to_read;
		limits.read_overflow_mode = settings.limits.read_overflow_mode;
		limits.max_execution_time = settings.limits.max_execution_time;
		limits.timeout_overflow_mode = settings.limits.timeout_overflow_mode;
		limits.min_execution_speed = settings.limits.min_execution_speed;
		limits.timeout_before_checking_execution_speed = settings.limits.timeout_before_checking_execution_speed;

		QuotaForIntervals & quota = context.getQuota();

		transformStreams([&](auto & stream)
		{
			if (IProfilingBlockInputStream * p_stream = dynamic_cast<IProfilingBlockInputStream *>(stream.get()))
			{
				p_stream->setLimits(limits);
				p_stream->setQuota(quota);
			}
		});
	}

	return from_stage;
}


void InterpreterSelectQuery::executeWhere(ExpressionActionsPtr expression)
{
	transformStreams([&](auto & stream)
	{
		stream = std::make_shared<FilterBlockInputStream>(stream, expression, query.where_expression->getColumnName());
	});
}


void InterpreterSelectQuery::executeAggregation(ExpressionActionsPtr expression, bool overflow_row, bool final)
{
	transformStreams([&](auto & stream)
	{
		stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
	});

	Names key_names;
	AggregateDescriptions aggregates;
	query_analyzer->getAggregateInfo(key_names, aggregates);

	/** Двухуровневая агрегация полезна в двух случаях:
	  * 1. Делается параллельная агрегация, и результаты надо параллельно мерджить.
	  * 2. Делается агрегация с сохранением временных данных на диск, и их нужно мерджить эффективно по памяти.
	  */
	bool allow_to_use_two_level_group_by = streams.size() > 1 || settings.limits.max_bytes_before_external_group_by != 0;

	Aggregator::Params params(key_names, aggregates,
		overflow_row, settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode,
		settings.compile ? &context.getCompiler() : nullptr, settings.min_count_to_compile,
		allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold : SettingUInt64(0),
		allow_to_use_two_level_group_by ? settings.group_by_two_level_threshold_bytes : SettingUInt64(0),
		settings.limits.max_bytes_before_external_group_by, context.getTemporaryPath());

	/// Если источников несколько, то выполняем параллельную агрегацию
	if (streams.size() > 1)
	{
		streams[0] = std::make_shared<ParallelAggregatingBlockInputStream>(
			streams, stream_with_non_joined_data, params, final,
			settings.max_threads,
			settings.aggregation_memory_efficient_merge_threads
				? settings.aggregation_memory_efficient_merge_threads
				: settings.max_threads);

		stream_with_non_joined_data = nullptr;
		streams.resize(1);
	}
	else
	{
		BlockInputStreams inputs;
		if (!streams.empty())
			inputs.push_back(streams[0]);
		else
			streams.resize(1);

		if (stream_with_non_joined_data)
			inputs.push_back(stream_with_non_joined_data);

		streams[0] = std::make_shared<AggregatingBlockInputStream>(std::make_shared<ConcatBlockInputStream>(inputs), params, final);

		stream_with_non_joined_data = nullptr;
	}
}


void InterpreterSelectQuery::executeMergeAggregated(bool overflow_row, bool final)
{
	Names key_names;
	AggregateDescriptions aggregates;
	query_analyzer->getAggregateInfo(key_names, aggregates);

	/** Есть два режима распределённой агрегации.
	  *
	  * 1. В разных потоках читать из удалённых серверов блоки.
	  * Сохранить все блоки в оперативку. Объединить блоки.
	  * Если агрегация двухуровневая - распараллелить по номерам корзин.
	  *
	  * 2. В одном потоке читать по очереди блоки с разных серверов.
	  * В оперативке хранится только по одному блоку с каждого сервера.
	  * Если агрегация двухуровневая - последовательно объединяем блоки каждого следующего уровня.
	  *
	  * Второй вариант расходует меньше памяти (до 256 раз меньше)
	  *  в случае двухуровневой агрегации, которая используется для больших результатов после GROUP BY,
	  *  но при этом может работать медленнее.
	  */

	Aggregator::Params params(key_names, aggregates, overflow_row);

	if (!settings.distributed_aggregation_memory_efficient)
	{
		/// Склеим несколько источников в один, распараллеливая работу.
		executeUnion();

		/// Теперь объединим агрегированные блоки
		streams[0] = std::make_shared<MergingAggregatedBlockInputStream>(streams[0], params, final, original_max_threads);
	}
	else
	{
		streams[0] = std::make_shared<MergingAggregatedMemoryEfficientBlockInputStream>(streams, params, final,
			settings.max_threads,
			settings.aggregation_memory_efficient_merge_threads
				? size_t(settings.aggregation_memory_efficient_merge_threads)
				: original_max_threads);

		streams.resize(1);
	}
}


void InterpreterSelectQuery::executeHaving(ExpressionActionsPtr expression)
{
	transformStreams([&](auto & stream)
	{
		stream = std::make_shared<FilterBlockInputStream>(stream, expression, query.having_expression->getColumnName());
	});
}


void InterpreterSelectQuery::executeTotalsAndHaving(bool has_having, ExpressionActionsPtr expression, bool overflow_row)
{
	executeUnion();

	streams[0] = std::make_shared<TotalsHavingBlockInputStream>(
		streams[0], overflow_row, expression,
		has_having ? query.having_expression->getColumnName() : "", settings.totals_mode, settings.totals_auto_threshold);
}


void InterpreterSelectQuery::executeExpression(ExpressionActionsPtr expression)
{
	transformStreams([&](auto & stream)
	{
		stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
	});
}


static SortDescription getSortDescription(ASTSelectQuery & query)
{
	SortDescription order_descr;
	order_descr.reserve(query.order_expression_list->children.size());
	for (const auto & elem : query.order_expression_list->children)
	{
		String name = elem->children.front()->getColumnName();
		const ASTOrderByElement & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

		order_descr.emplace_back(name, order_by_elem.direction, order_by_elem.collator);
	}

	return order_descr;
}

static size_t getLimitForSorting(ASTSelectQuery & query)
{
	/// Если есть LIMIT и нет DISTINCT - можно делать частичную сортировку.
	size_t limit = 0;
	if (!query.distinct)
	{
		size_t limit_length = 0;
		size_t limit_offset = 0;
		getLimitLengthAndOffset(query, limit_length, limit_offset);
		limit = limit_length + limit_offset;
	}

	return limit;
}


void InterpreterSelectQuery::executeOrder()
{
	SortDescription order_descr = getSortDescription(query);
	size_t limit = getLimitForSorting(query);

	transformStreams([&](auto & stream)
	{
		auto sorting_stream = std::make_shared<PartialSortingBlockInputStream>(stream, order_descr, limit);

		/// Ограничения на сортировку
		IProfilingBlockInputStream::LocalLimits limits;
		limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
		limits.max_rows_to_read = settings.limits.max_rows_to_sort;
		limits.max_bytes_to_read = settings.limits.max_bytes_to_sort;
		limits.read_overflow_mode = settings.limits.sort_overflow_mode;
		sorting_stream->setLimits(limits);

		stream = sorting_stream;
	});

	/// Если потоков несколько, то объединяем их в один
	executeUnion();

	/// Сливаем сортированные блоки.
	streams[0] = std::make_shared<MergeSortingBlockInputStream>(
		streams[0], order_descr, settings.max_block_size, limit,
		settings.limits.max_bytes_before_external_sort, context.getTemporaryPath());
}


void InterpreterSelectQuery::executeMergeSorted()
{
	SortDescription order_descr = getSortDescription(query);
	size_t limit = getLimitForSorting(query);

	/// Если потоков несколько, то объединяем их в один
	if (hasMoreThanOneStream())
	{
		/** MergingSortedBlockInputStream читает источники последовательно.
		  * Чтобы данные на удалённых серверах готовились параллельно, оборачиваем в AsynchronousBlockInputStream.
		  */
		transformStreams([&](auto & stream)
		{
			stream = std::make_shared<AsynchronousBlockInputStream>(stream);
		});

		/// Сливаем сортированные источники в один сортированный источник.
		streams[0] = std::make_shared<MergingSortedBlockInputStream>(streams, order_descr, settings.max_block_size, limit);
		streams.resize(1);
	}
}


void InterpreterSelectQuery::executeProjection(ExpressionActionsPtr expression)
{
	transformStreams([&](auto & stream)
	{
		stream = std::make_shared<ExpressionBlockInputStream>(stream, expression);
	});
}


void InterpreterSelectQuery::executeDistinct(bool before_order, Names columns)
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

		transformStreams([&](auto & stream)
		{
			stream = std::make_shared<DistinctBlockInputStream>(stream, settings.limits, limit_for_distinct, columns);
		});

		if (hasMoreThanOneStream())
			union_within_single_query = true;
	}
}


void InterpreterSelectQuery::executeUnion()
{
	/// Если до сих пор есть несколько потоков, то объединяем их в один
	if (hasMoreThanOneStream())
	{
		streams[0] = std::make_shared<UnionBlockInputStream<>>(streams, stream_with_non_joined_data, settings.max_threads);
		stream_with_non_joined_data = nullptr;
		streams.resize(1);
		union_within_single_query = false;
	}
	else if (stream_with_non_joined_data)
	{
		streams.push_back(stream_with_non_joined_data);
		stream_with_non_joined_data = nullptr;
		union_within_single_query = false;
	}
}


/// Предварительный LIMIT - применяется в каждом источнике, если источников несколько, до их объединения.
void InterpreterSelectQuery::executePreLimit()
{
	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	/// Если есть LIMIT
	if (query.limit_length)
	{
		transformStreams([&](auto & stream)
		{
			stream = std::make_shared<LimitBlockInputStream>(stream, limit_length + limit_offset, 0);
		});

		if (hasMoreThanOneStream())
			union_within_single_query = true;
	}
}


void InterpreterSelectQuery::executeLimit()
{
	size_t limit_length = 0;
	size_t limit_offset = 0;
	getLimitLengthAndOffset(query, limit_length, limit_offset);

	/// Если есть LIMIT
	if (query.limit_length)
	{
		/** Редкий случай:
		  *  если нет WITH TOTALS и есть подзапрос в FROM, и там на одном из уровней есть WITH TOTALS,
		  *  то при использовании LIMIT-а следует читать данные до конца, а не отменять выполнение запроса раньше,
		  *  потому что при отмене выполнения запроса, мы не получим данные для totals с удалённого сервера.
		  *
		  * Ещё случай:
		  *  если есть WITH TOTALS и нет ORDER BY, то читать данные до конца,
		  *  иначе TOTALS посчитается по неполным данным.
		  */
		bool always_read_till_end = false;

		if (query.group_by_with_totals && !query.order_expression_list)
		{
			always_read_till_end = true;
		}

		auto query_table = query.table();
		if (!query.group_by_with_totals && query_table && typeid_cast<const ASTSelectQuery *>(query_table.get()))
		{
			const ASTSelectQuery * subquery = static_cast<const ASTSelectQuery *>(query_table.get());

			while (subquery->table())
			{
				if (subquery->group_by_with_totals)
				{
					/** NOTE Можно ещё проверять, что таблица в подзапросе - распределённая, и что она смотрит только на один шард.
					  * В остальных случаях totals будет вычислен на сервере-инициаторе запроса, и читать данные до конца не обязательно.
					  */

					always_read_till_end = true;
					break;
				}

				auto subquery_table = subquery->table();
				if (typeid_cast<const ASTSelectQuery *>(subquery_table.get()))
					subquery = static_cast<const ASTSelectQuery *>(subquery_table.get());
				else
					break;
			}
		}

		transformStreams([&](auto & stream)
		{
			stream = std::make_shared<LimitBlockInputStream>(stream, limit_length, limit_offset, always_read_till_end);
		});
	}
}


void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(SubqueriesForSets & subqueries_for_sets)
{
	/// Если запрос не распределённый, то удалим создание временных таблиц из подзапросов (предназначавшихся для отправки на удалённые серверы).
	if (!(storage && storage->isRemote()))
		for (auto & elem : subqueries_for_sets)
			elem.second.table.reset();

	executeUnion();
	streams[0] = std::make_shared<CreatingSetsBlockInputStream>(streams[0], subqueries_for_sets, settings.limits);
}


void InterpreterSelectQuery::ignoreWithTotals()
{
	query.group_by_with_totals = false;
}


void InterpreterSelectQuery::initSettings()
{
	if (query.settings)
		InterpreterSetQuery(query.settings, context).executeForCurrentContext();

	settings = context.getSettings();
}

}
