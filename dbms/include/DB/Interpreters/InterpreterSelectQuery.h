#pragma once

#include <DB/Core/QueryProcessingStage.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/DataStreams/IBlockInputStream.h>

namespace DB
{

class ExpressionAnalyzer;
class ASTSelectQuery;
class SubqueryForSet;


/** Интерпретирует запрос SELECT. Возвращает поток блоков с результатами выполнения запроса до стадии to_stage.
  */
class InterpreterSelectQuery : public IInterpreter
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
		BlockInputStreamPtr input = nullptr);

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

	~InterpreterSelectQuery();

	/** Выполнить запрос, возможно являющиийся цепочкой UNION ALL.
	 *  Получить поток блоков для чтения
	 */
	BlockIO execute() override;

	/** Выполнить запрос без объединения потоков, если это возможно.
	 */
	const BlockInputStreams & executeWithoutUnion();

	DataTypes getReturnTypes();
	Block getSampleBlock();

	static Block getSampleBlock(
		ASTPtr query_ptr_,
		const Context & context_,
		QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
		size_t subquery_depth_ = 0);

private:
	/**
	 * ignore_union_all_tail
	 * - Оптимизация, если объект создаётся только, чтобы вызвать getSampleBlock(): учитываем только первый SELECT цепочки UNION ALL, потом что
	 *   первый SELECT достаточен для определения нужных столбцов.
	 */
	InterpreterSelectQuery(
		ASTPtr query_ptr_,
		const Context & context_,
		bool ignore_union_all_tail,
		QueryProcessingStage::Enum to_stage_ = QueryProcessingStage::Complete,
		size_t subquery_depth_ = 0,
		BlockInputStreamPtr input = nullptr);

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

	/** Из какой таблицы читать. При JOIN, возвращается "левая" таблицы.
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
	void executeSubqueriesInSetsAndJoins(BlockInputStreams & streams, std::unordered_map<String, SubqueryForSet> & subqueries_for_sets);

	void ignoreWithTotals();

	/** Если в запросе SELECT есть секция SETTINGS, то применить настройки из неё.
	  * Затем достать настройки из context и поместить их в settings.
	  *
	  * Секция SETTINGS - настройки для конкретного запроса.
	  * Обычно настройки могут быть переданы другими способами, не внутри запроса.
	  * Но использование такой секции оправдано, если нужно задать настройки для одного подзапроса.
	  */
	void initSettings();

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
