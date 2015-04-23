#pragma once

#include <DB/Core/QueryProcessingStage.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/DataStreams/IBlockInputStream.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/TableFunctions/ITableFunction.h>

namespace DB
{

/** Интерпретирует запрос SELECT. Возвращает поток блоков с результатами выполнения запроса до стадии to_stage.
  */
class InterpreterSelectQuery
{
public:
	/** to_stage
	 * - стадия, до которой нужно выполнить запрос. По-умолчанию - до конца.
	 *   Можно выполнить до промежуточного состояния агрегации, которые объединяются с разных серверов при распределённой обработке запроса.
	 *
	 * subquery_depth
	 * - для контроля ограничений на глубину вложенности подзапросов. Для подзапросов передаётся значение, увеличенное на единицу.
	 *
	 * input
	 * - если задан - читать не из таблицы, указанной в запросе, а из готового источника.
	 *
	 * required_column_names
	 * - удалить из запроса все столбцы кроме указанных - используется для удаления ненужных столбцов из подзапросов.
	 *
	 * table_column_names
	 * - поместить в контекст в качестве известных столбцов только указанные столбцы, а не все столбцы таблицы.
	 *   Используется, например, совместно с указанием input.
	 */

	InterpreterSelectQuery(
		ASTPtr query_ptr_,
		const Context & context_,
		QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
		size_t subquery_depth_ = 0,
		BlockInputStreamPtr input = nullptr,
		bool is_union_all_head_ = true);

	InterpreterSelectQuery(
		ASTPtr query_ptr_,
		const Context & context_,
		const Names & required_column_names,
		QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
		size_t subquery_depth_ = 0,
		BlockInputStreamPtr input = nullptr);

	InterpreterSelectQuery(
		ASTPtr query_ptr_,
		const Context & context_,
		const Names & required_column_names,
		const NamesAndTypesList & table_column_names,
		QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
		size_t subquery_depth_ = 0,
		BlockInputStreamPtr input = nullptr);

	/** Выполнить запрос, возможно являющиийся цепочкой UNION ALL.
	 *  Получить поток блоков для чтения
	 */
	BlockInputStreamPtr execute();

	/** Выполнить запрос без объединения потоков, если это возможно.
	 */
	const BlockInputStreams & executeWithoutUnion();

	/** Выполнить запрос, записать результат в нужном формате в buf.
	 * BlockInputStreamPtr возвращается, чтобы можно было потом получить информацию о плане выполнения запроса.
	 */
	BlockInputStreamPtr executeAndFormat(WriteBuffer & buf);

	DataTypes getReturnTypes();
	Block getSampleBlock();

private:
	void init(BlockInputStreamPtr input, const Names & required_column_names = Names(), const NamesAndTypesList & table_column_names = NamesAndTypesList());
	void basicInit(BlockInputStreamPtr input, const NamesAndTypesList & table_column_names);
	void initQueryAnalyzer();

	/// Выполнить один запрос SELECT из цепочки UNION ALL.
	void executeSingleQuery();

	/** Оставить в каждом запросе цепочки UNION ALL только нужные столбцы секции SELECT.
	 *  Однако, если используется хоть один DISTINCT в цепочке, то все столбцы считаются нужными,
	 *  так как иначе DISTINCT работал бы по-другому.
	 */
	void rewriteExpressionList(const Names & required_column_names);

	/// Содержит ли запрос хотя бы один астериск?
	bool hasAsterisk() const;

	// Переименовать столбцы каждого запроса цепочки UNION ALL в такие же имена, как в первом запросе.
	void renameColumns();

	/** Из какой таблицы читать. JOIN-ы не поддерживаются.
	 */
	void getDatabaseAndTableNames(String & database_name, String & table_name);

	/** Выбрать из списка столбцов какой-нибудь, лучше - минимального размера.
	 */
	String getAnyColumn();

	/// Разные стадии выполнения запроса.

	/// Вынимает данные из таблицы. Возвращает стадию, до которой запрос был обработан в Storage.
	QueryProcessingStage::Enum executeFetchColumns(BlockInputStreams & streams);

	void executeWhere(                   BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeAggregation(             BlockInputStreams & streams, ExpressionActionsPtr expression, bool overflow_row, bool final);
	void executeMergeAggregated(         BlockInputStreams & streams, bool overflow_row, bool final);
	void executeTotalsAndHaving(         BlockInputStreams & streams, bool has_having, ExpressionActionsPtr expression, bool overflow_row);
	void executeHaving(                  BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeExpression(              BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeOrder(                   BlockInputStreams & streams);
	void executeMergeSorted(             BlockInputStreams & streams);
	void executePreLimit(                BlockInputStreams & streams);
	void executeUnion(                   BlockInputStreams & streams);
	void executeLimit(                   BlockInputStreams & streams);
	void executeProjection(              BlockInputStreams & streams, ExpressionActionsPtr expression);
	void executeDistinct(                BlockInputStreams & streams, bool before_order, Names columns);
	void executeSubqueriesInSetsAndJoins(BlockInputStreams & streams, SubqueriesForSets & subqueries_for_sets);

	void ignoreWithTotals() { query.group_by_with_totals = false; }

	ASTPtr query_ptr;
	ASTSelectQuery & query;
	Context context;
	Settings settings;
	size_t original_max_threads; /// В settings настройка max_threads может быть изменена. В original_max_threads сохраняется изначальное значение.
	QueryProcessingStage::Enum to_stage;
	size_t subquery_depth;
	std::unique_ptr<ExpressionAnalyzer> query_analyzer;
	BlockInputStreams streams;

	/// Являемся ли мы первым запросом SELECT цепочки UNION ALL?
	bool is_first_select_inside_union_all;

	/// Следующий запрос SELECT в цепочке UNION ALL.
	std::unique_ptr<InterpreterSelectQuery> next_select_in_union_all;

	/// Таблица, откуда читать данные, если не подзапрос.
	StoragePtr storage;
	IStorage::TableStructureReadLockPtr table_lock;

	/// Выполнить объединение потоков внутри запроса SELECT?
	bool union_within_single_query = false;

	Logger * log;
};

}
