#include <DB/DataStreams/ExpressionBlockInputStream.h>
#include <DB/DataStreams/FilterBlockInputStream.h>
#include <DB/DataStreams/LimitBlockInputStream.h>
#include <DB/DataStreams/PartialSortingBlockInputStream.h>
#include <DB/DataStreams/MergeSortingBlockInputStream.h>
#include <DB/DataStreams/AggregatingBlockInputStream.h>
#include <DB/DataStreams/MergingAggregatedBlockInputStream.h>
#include <DB/DataStreams/AsynchronousBlockInputStream.h>
#include <DB/DataStreams/UnionBlockInputStream.h>
#include <DB/DataStreams/ParallelAggregatingBlockInputStream.h>
#include <DB/DataStreams/SplittingAggregatingBlockInputStream.h>
#include <DB/DataStreams/DistinctBlockInputStream.h>
#include <DB/DataStreams/NullBlockInputStream.h>
#include <DB/DataStreams/TotalsHavingBlockInputStream.h>
#include <DB/DataStreams/narrowBlockInputStreams.h>
#include <DB/DataStreams/copyData.h>
#include <DB/DataStreams/CreatingSetsBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>

#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTOrderByElement.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Storages/StorageView.h>
#include <DB/TableFunctions/ITableFunction.h>
#include <DB/TableFunctions/TableFunctionFactory.h>

#include <DB/Core/Field.h>

namespace DB
{

void InterpreterSelectQuery::init(BlockInputStreamPtr input_, const NamesAndTypesList & table_column_names)
{
        ProfileEvents::increment(ProfileEvents::SelectQuery);

        if (settings.limits.max_subquery_depth && subquery_depth > settings.limits.max_subquery_depth)
                throw Exception("Too deep subqueries. Maximum: " + toString(settings.limits.max_subquery_depth),
                        ErrorCodes::TOO_DEEP_SUBQUERIES);

        if (query.table && typeid_cast<ASTSelectQuery *>(&*query.table))
        {
                if (table_column_names.empty())
                        context.setColumns(InterpreterSelectQuery(query.table, context, to_stage, subquery_depth, nullptr, false).getSampleBlock().getColumnsList());
        }
        else
        {
                if (query.table && typeid_cast<const ASTFunction *>(&*query.table))
                {
                        /// Получить табличную функцию
                        TableFunctionPtr table_function_ptr = context.getTableFunctionFactory().get(typeid_cast<const ASTFunction *>(&*query.table)->name, context);
                        /// Выполнить ее и запомнить результат
                        storage = table_function_ptr->execute(query.table, context);
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
                        context.setColumns(storage->getColumnsListNonMaterialized());
        }

        if (!table_column_names.empty())
                context.setColumns(table_column_names);

        if (context.getColumns().empty())
                throw Exception("There are no available columns", ErrorCodes::THERE_IS_NO_COLUMN);

        query_analyzer = new ExpressionAnalyzer(query_ptr, context, storage, subquery_depth, true);

        /// Сохраняем в query context новые временные таблицы
        for (auto & it : query_analyzer->getExternalTables())
                if (!context.tryGetExternalTable(it.first))
                        context.addExternalTable(it.first, it.second);

        if (input_)
                streams.push_back(input_);
        
        if (isFirstSelectInsideUnionAll())
        {
                // Создаем цепочку запросов SELECT и проверяем, что результаты всех запросов SELECT cовместимые.
                // NOTE Мы можем безопасно применить static_cast вместо typeid_cast, 
                // потому что знаем, что в цепочке UNION ALL имеются только деревья типа SELECT.
                InterpreterSelectQuery * interpreter = this;
                Block first = interpreter->getSampleBlock();
                for (ASTPtr tree = query.next_union_all; !tree.isNull(); tree = (static_cast<ASTSelectQuery &>(*tree)).next_union_all)
                {
                        interpreter->next_select_in_union_all.reset(new InterpreterSelectQuery(tree, context, to_stage, subquery_depth, nullptr, false));
                        interpreter = interpreter->next_select_in_union_all.get();
                        Block current = interpreter->getSampleBlock();
                        if (!blocksHaveEqualStructure(first, current))
                                throw Exception("Result structures mismatch in the SELECT queries of the UNION ALL chain", 
                                ErrorCodes::UNION_ALL_RESULT_STRUCTURES_MISMATCH);
                }
        }
}

InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_, QueryProcessingStage::Enum to_stage_,
        size_t subquery_depth_, BlockInputStreamPtr input_, bool is_union_all_head_)
        : query_ptr(query_ptr_), query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
        context(context_), settings(context.getSettings()), to_stage(to_stage_), subquery_depth(subquery_depth_),
        is_union_all_head(is_union_all_head_),
        log(&Logger::get("InterpreterSelectQuery"))
{
        init(input_);
}

InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_,
        const Names & required_column_names_,
        QueryProcessingStage::Enum to_stage_, size_t subquery_depth_, BlockInputStreamPtr input_)
        : query_ptr(query_ptr_), query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
        context(context_), settings(context.getSettings()), to_stage(to_stage_), subquery_depth(subquery_depth_),
        is_union_all_head(true),
        log(&Logger::get("InterpreterSelectQuery"))
{
        /** Оставляем в запросе в секции SELECT только нужные столбцы.
          * Но если используется DISTINCT, то все столбцы считаются нужными, так как иначе DISTINCT работал бы по-другому.
          */
        if (!query.distinct)
                query.rewriteSelectExpressionList(required_column_names_);

        init(input_);
}

InterpreterSelectQuery::InterpreterSelectQuery(ASTPtr query_ptr_, const Context & context_,
        const Names & required_column_names_,
        const NamesAndTypesList & table_column_names, QueryProcessingStage::Enum to_stage_, size_t subquery_depth_, BlockInputStreamPtr input_)
        : query_ptr(query_ptr_), query(typeid_cast<ASTSelectQuery &>(*query_ptr)),
        context(context_), settings(context.getSettings()), to_stage(to_stage_), subquery_depth(subquery_depth_),
        is_union_all_head(true),
        log(&Logger::get("InterpreterSelectQuery"))
{
        /** Оставляем в запросе в секции SELECT только нужные столбцы.
          * Но если используется DISTINCT, то все столбцы считаются нужными, так как иначе DISTINCT работал бы по-другому.
          */
        if (!query.distinct)
                query.rewriteSelectExpressionList(required_column_names_);

        init(input_, table_column_names);
}

bool InterpreterSelectQuery::isFirstSelectInsideUnionAll() const
{
        return is_union_all_head && !query.next_union_all.isNull();
}

void InterpreterSelectQuery::getDatabaseAndTableNames(String & database_name, String & table_name)
{
        /** Если таблица не указана - используем таблицу system.one.
          * Если база данных не указана - используем текущую базу данных.
          */
        if (query.database)
                database_name = typeid_cast<ASTIdentifier &>(*query.database).name;
        if (query.table)
                table_name = typeid_cast<ASTIdentifier &>(*query.table).name;

        if (!query.table)
        {
                database_name = "system";
                table_name = "one";
        }
        else if (!query.database)
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
        for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
        {
                res.push_back(it->type);
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
        if (isFirstSelectInsideUnionAll())
        {
                executeSingleQuery(false);       
                for (auto p = next_select_in_union_all.get(); p != nullptr; p = p->next_select_in_union_all.get())
                {
                        p->executeSingleQuery(false);
                        const auto & others = p->streams;
                        streams.insert(streams.end(), others.begin(), others.end());
                }
                
                if (streams.empty())
                        return new NullBlockInputStream;
                
                for (auto & stream : streams)
                        stream = new MaterializingBlockInputStream(stream);
                
                executeUnion(streams);
        }
        else
        {
                executeSingleQuery();
                if (streams.empty())
                        return new NullBlockInputStream;
        }
        
        /// Ограничения на результат, квота на результат, а также колбек для прогресса.
        if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(&*streams[0]))
        {
                stream->setProgressCallback(context.getProgressCallback());
                stream->setProcessListElement(context.getProcessListElement());

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
        
        return streams[0];
}


void InterpreterSelectQuery::executeSingleQuery(bool should_perform_union_hint)
{
        /** Потоки данных. При параллельном выполнении запроса, имеем несколько потоков данных.
          * Если нет GROUP BY, то выполним все операции до ORDER BY и LIMIT параллельно, затем
          *  если есть ORDER BY, то склеим потоки с помощью UnionBlockInputStream, а затем MergеSortingBlockInputStream,
          *  если нет, то склеим с помощью UnionBlockInputStream,
          *  затем применим LIMIT.
          * Если есть GROUP BY, то выполним все операции до GROUP BY, включительно, параллельно;
          *  параллельный GROUP BY склеит потоки в один,
          *  затем выполним остальные операции с одним получившимся потоком.
          * Если запрос является членом цепочки UNION ALL и не содержит GROUP BY, ORDER BY, DISTINCT, или LIMIT, 
          * то объединение источников данных выполняется не на этом уровне, а на верхнем уровне.
          */

        bool do_execute_union = should_perform_union_hint;
        
        /** Вынем данные из Storage. from_stage - до какой стадии запрос был выполнен в Storage. */
        QueryProcessingStage::Enum from_stage = executeFetchColumns(streams);

        LOG_TRACE(log, QueryProcessingStage::toString(from_stage) << " -> " << QueryProcessingStage::toString(to_stage));

        if (to_stage > QueryProcessingStage::FetchColumns)
        {
                bool has_where      = false;
                bool need_aggregate = false;
                bool has_having     = false;
                bool has_order_by   = false;
                
                ExpressionActionsPtr array_join;
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
                  * Независимо от from_stage и to_stage составим полную последовательность действий, чтобы выполнять оптимизации и
                  *  выбрасывать ненужные столбцы с учетом всего запроса. В ненужных частях запроса не будем выполнять подзапросы.
                  */

                {
                        ExpressionActionsChain chain;

                        need_aggregate = query_analyzer->hasAggregation();

                        query_analyzer->appendArrayJoin(chain, !first_stage);
                        query_analyzer->appendJoin(chain, !first_stage);

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
                  * Эта проверка специально вынесена чуть ниже, чем она могла бы быть (сразу после executeFetchColumns),
                  *  чтобы запрос был проанализирован, и в нём могли бы быть обнаружены ошибки (например, несоответствия типов).
                  * Иначе мог бы вернуться пустой результат на некорректный запрос.
                  */
                if (streams.empty())
                        return;

                /// Перед выполнением HAVING уберем из блока лишние столбцы (в основном, ключи агрегации).
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

                        
                if (need_aggregate || has_order_by)
                        do_execute_union = true;
                
                if (first_stage)
                {
                        if (has_where)
                                executeWhere(streams, before_where);

                        if (need_aggregate)
                                executeAggregation(streams, before_aggregation, aggregate_overflow_row, aggregate_final);
                        else
                        {
                                executeExpression(streams, before_order_and_select);
                                executeDistinct(streams, true, selected_columns);
                        }

                        /** Оптимизация - при распределённой обработке запроса,
                          *  если не указаны GROUP, HAVING, ORDER, но указан LIMIT,
                          *  то выполним предварительный LIMIT на удалёном сервере.
                          */
                        if (!second_stage
                                && !need_aggregate && !has_having && !has_order_by
                                && query.limit_length)
                        {
                                executePreLimit(streams);
                                do_execute_union = true;
                        }
                }
                
                if (second_stage)
                {
                        bool need_second_distinct_pass = true;

                        if (need_aggregate)
                        {
                                /// Если нужно объединить агрегированные результаты с нескольких серверов
                                if (!first_stage)
                                        executeMergeAggregated(streams, aggregate_overflow_row, aggregate_final);

                                if (!aggregate_final)
                                        executeTotalsAndHaving(streams, has_having, before_having, aggregate_overflow_row);
                                else if (has_having)
                                        executeHaving(streams, before_having);

                                executeExpression(streams, before_order_and_select);
                                executeDistinct(streams, true, selected_columns);

                                need_second_distinct_pass = streams.size() > 1;
                        }
                        else if (query.group_by_with_totals && !aggregate_final)
                        {
                                executeTotalsAndHaving(streams, false, nullptr, aggregate_overflow_row);
                        }

                        if (has_order_by)
                                executeOrder(streams);

                        executeProjection(streams, final_projection);

                        /// На этой стадии можно считать минимумы и максимумы, если надо.
                        if (settings.extremes)
                                for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
                                        if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(&**it))
                                                stream->enableExtremes();

                        /** Оптимизация - если источников несколько и есть LIMIT, то сначала применим предварительный LIMIT,
                          * ограничивающий число записей в каждом до offset + limit.
                          */
                        if (query.limit_length && streams.size() > 1 && !query.distinct)
                        {
                                executePreLimit(streams);
                                do_execute_union = true;
                        }
                        
                        if (need_second_distinct_pass)
                                do_execute_union = true;
                        
                        if (do_execute_union)
                                executeUnion(streams);
                        
                        /// Если было более одного источника - то нужно выполнить DISTINCT ещё раз после их слияния.
                        if (need_second_distinct_pass)
                                executeDistinct(streams, false, Names());

                        executeLimit(streams);
                }
        }

        /** Если данных нет. */
        if (streams.empty())
                return;

        if (do_execute_union)
                executeUnion(streams);

        SubqueriesForSets subqueries_for_sets = query_analyzer->getSubqueriesForSets();
        if (!subqueries_for_sets.empty())
                executeSubqueriesInSetsAndJoins(streams, subqueries_for_sets);
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

QueryProcessingStage::Enum InterpreterSelectQuery::executeFetchColumns(BlockInputStreams & streams)
{
        if (!streams.empty())
                return QueryProcessingStage::FetchColumns;

        /// Интерпретатор подзапроса, если подзапрос
        SharedPtr<InterpreterSelectQuery> interpreter_subquery;

        /// Список столбцов, которых нужно прочитать, чтобы выполнить запрос.
        Names required_columns = query_analyzer->getRequiredColumns();

        if (query.table && typeid_cast<ASTSelectQuery *>(&*query.table))
        {
                /** Для подзапроса не действуют ограничения на максимальный размер результата.
                  * Так как результат поздапроса - ещё не результат всего запроса.
                  */
                Context subquery_context = context;
                Settings subquery_settings = context.getSettings();
                subquery_settings.limits.max_result_rows = 0;
                subquery_settings.limits.max_result_bytes = 0;
                /// Вычисление extremes не имеет смысла и не нужно (если его делать, то в результате всего запроса могут взяться extremes подзапроса).
                subquery_settings.extremes = 0;
                subquery_context.setSettings(subquery_settings);

                interpreter_subquery = new InterpreterSelectQuery(
                        query.table, subquery_context, required_columns, QueryProcessingStage::Complete, subquery_depth + 1);
        }

        /// если в настройках установлен default_sample != 1, то все запросы выполняем с сэмплингом
        /// если таблица не поддерживает сэмплинг получим исключение
        /// поэтому запросы типа SHOW TABLES работать с включенном default_sample не будут
        if (!query.sample_size && settings.default_sample != 1)
                query.sample_size = new ASTLiteral(StringRange(), Float64(settings.default_sample));

        if (query.sample_size && (!storage || !storage->supportsSampling()))
                throw Exception("Illegal SAMPLE: table doesn't support sampling", ErrorCodes::SAMPLING_NOT_SUPPORTED);

        if (query.final && (!storage || !storage->supportsFinal()))
                throw Exception(storage ? "Storage " + storage->getName() + " doesn't support FINAL" : "Illegal FINAL", ErrorCodes::ILLEGAL_FINAL);

        if (query.prewhere_expression && (!storage || !storage->supportsPrewhere()))
                throw Exception(storage ? "Storage " + storage->getName() + " doesn't support PREWHERE" : "Illegal PREWHERE", ErrorCodes::ILLEGAL_PREWHERE);

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
        if (storage && storage->isRemote())
                settings.max_threads = settings.max_distributed_connections;

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
        if (!query.distinct && !query.prewhere_expression && !query.where_expression && !query.group_expression_list && !query.having_expression && !query.order_expression_list
                && query.limit_length && !query_analyzer->hasAggregation() && limit_length + limit_offset < settings.max_block_size)
        {
                settings.max_block_size = limit_length + limit_offset;
                settings.max_threads = 1;
                settings.asynchronous = false;
        }

        QueryProcessingStage::Enum from_stage = QueryProcessingStage::FetchColumns;

        query_analyzer->makeSetsForIndex();

        /// Инициализируем изначальные потоки данных, на которые накладываются преобразования запроса. Таблица или подзапрос?
        if (!interpreter_subquery)
        {
                /** При распределённой обработке запроса, на все удалённые серверы отправляются временные таблицы,
                  *  полученные из глобальных подзапросов - GLOBAL IN/JOIN.
                  */
                if (storage && storage->isRemote())
                        storage->storeExternalTables(query_analyzer->getExternalTables());

		streams = storage->read(required_columns, query_ptr,
					context, settings_for_storage, from_stage,
					settings.max_block_size, settings.max_threads);

                for (auto & stream : streams)
                        stream->addTableLock(table_lock);
        }
        else
        {
                streams.push_back(maybeAsynchronous(interpreter_subquery->execute(), settings.asynchronous));
        }

        /** Если истчоников слишком много, то склеим их в max_threads источников.
          * (Иначе действия в каждом маленьком источнике, а затем объединение состояний, слишком неэффективно.)
          */
        if (streams.size() > settings.max_threads)
                streams = narrowBlockInputStreams(streams, settings.max_threads);

        /** Установка ограничений и квоты на чтение данных, скорость и время выполнения запроса.
          * Такие ограничения проверяются на сервере-инициаторе запроса, а не на удалённых серверах.
          * Потому что сервер-инициатор имеет суммарные данные о выполнении запроса на всех серверах.
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

                for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
                {
                        if (IProfilingBlockInputStream * stream = dynamic_cast<IProfilingBlockInputStream *>(&**it))
                        {
                                stream->setLimits(limits);
                                stream->setQuota(quota);
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


void InterpreterSelectQuery::executeAggregation(BlockInputStreams & streams, ExpressionActionsPtr expression, bool overflow_row, bool final)
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

        bool separate_totals = to_stage > QueryProcessingStage::WithMergeableState;

        /// Если источников несколько, то выполняем параллельную агрегацию
        if (streams.size() > 1)
        {
                if (!settings.use_splitting_aggregator || key_names.empty())
                {
                        stream = maybeAsynchronous(
                                new ParallelAggregatingBlockInputStream(streams, key_names, aggregates, overflow_row, final,
                                settings.max_threads, settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode), settings.asynchronous);
                }
                else
                {
                        if (overflow_row)
                                throw Exception("Splitting aggregator cannot handle queries like this yet. "
                                                                "Please change use_splitting_aggregator, remove WITH TOTALS, "
                                                                "change group_by_overflow_mode or set totals_mode to AFTER_HAVING_EXCLUSIVE.",
                                                                ErrorCodes::NOT_IMPLEMENTED);
                        stream = maybeAsynchronous(
                                new SplittingAggregatingBlockInputStream(
                                        new UnionBlockInputStream(streams, settings.max_threads),
                                        key_names, aggregates, settings.max_threads, query.group_by_with_totals, separate_totals, final,
                                        settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode), settings.asynchronous);
                }

                streams.resize(1);
        }
        else
                stream = maybeAsynchronous(new AggregatingBlockInputStream(stream, key_names, aggregates, overflow_row, final,
                        settings.limits.max_rows_to_group_by, settings.limits.group_by_overflow_mode), settings.asynchronous);
}


void InterpreterSelectQuery::executeMergeAggregated(BlockInputStreams & streams, bool overflow_row, bool final)
{
        /// Если объединять нечего
        if (streams.size() == 1)
                return;

        /// Склеим несколько источников в один
        streams[0] = new UnionBlockInputStream(streams, settings.max_threads);
        streams.resize(1);

        /// Теперь объединим агрегированные блоки
        Names key_names;
        AggregateDescriptions aggregates;
        query_analyzer->getAggregateInfo(key_names, aggregates);
        streams[0] = maybeAsynchronous(new MergingAggregatedBlockInputStream(streams[0], key_names, aggregates, overflow_row, final), settings.asynchronous);
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


void InterpreterSelectQuery::executeTotalsAndHaving(BlockInputStreams & streams, bool has_having,
        ExpressionActionsPtr expression, bool overflow_row)
{
        if (streams.size() > 1)
        {
                streams[0] = new UnionBlockInputStream(streams, settings.max_threads);
                streams.resize(1);
        }

        Names key_names;
        AggregateDescriptions aggregates;
        query_analyzer->getAggregateInfo(key_names, aggregates);
        streams[0] = maybeAsynchronous(new TotalsHavingBlockInputStream(
                        streams[0], key_names, aggregates, overflow_row, expression,
                        has_having ? query.having_expression->getColumnName() : "", settings.totals_mode, settings.totals_auto_threshold),
                settings.asynchronous);
}


void InterpreterSelectQuery::executeExpression(BlockInputStreams & streams, ExpressionActionsPtr expression)
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
                order_descr.push_back(SortColumnDescription(name, typeid_cast<ASTOrderByElement &>(**it).direction));
        }

        /// Если есть LIMIT и нет DISTINCT - можно делать частичную сортировку.
        size_t limit = 0;
        if (!query.distinct)
        {
                size_t limit_length = 0;
                size_t limit_offset = 0;
                getLimitLengthAndOffset(query, limit_length, limit_offset);
                limit = limit_length + limit_offset;
        }

        bool is_async = settings.asynchronous && streams.size() <= settings.max_threads;
        for (BlockInputStreams::iterator it = streams.begin(); it != streams.end(); ++it)
        {
                BlockInputStreamPtr & stream = *it;
                IProfilingBlockInputStream * sorting_stream = new PartialSortingBlockInputStream(stream, order_descr, limit);

                /// Ограничения на сортировку
                IProfilingBlockInputStream::LocalLimits limits;
                limits.mode = IProfilingBlockInputStream::LIMITS_TOTAL;
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


void InterpreterSelectQuery::executeDistinct(BlockInputStreams & streams, bool before_order, Names columns)
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
                        stream = maybeAsynchronous(new DistinctBlockInputStream(
                                stream, settings.limits, limit_for_distinct, columns), is_async);
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


void InterpreterSelectQuery::executeSubqueriesInSetsAndJoins(BlockInputStreams & streams, SubqueriesForSets & subqueries_for_sets)
{
        /// Если запрос не распределённый, то удалим создание временных таблиц из подзапросов (предназначавшихся для отправки на удалённые серверы).
        if (!(storage && storage->isRemote()))
                for (auto & elem : subqueries_for_sets)
                        elem.second.table.reset();

        streams[0] = new CreatingSetsBlockInputStream(streams[0], subqueries_for_sets, settings.limits);
}


BlockInputStreamPtr InterpreterSelectQuery::executeAndFormat(WriteBuffer & buf)
{
        Block sample = getSampleBlock();
        String format_name = query.format ? typeid_cast<ASTIdentifier &>(*query.format).name : context.getDefaultFormat();

        BlockInputStreamPtr in = execute();
        BlockOutputStreamPtr out = context.getFormatFactory().getOutput(format_name, buf, sample);

        copyData(*in, *out);

        return in;
}


}
