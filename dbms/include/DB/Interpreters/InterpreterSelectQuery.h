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
struct SubqueryForSet;


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
	 * - список доступных столбцов таблицы.
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
		const NamesAndTypesList & table_column_names_,
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
		const Context & context_);

private:
	/**
	 * - Оптимизация, если объект создаётся только, чтобы вызвать getSampleBlock(): учитываем только первый SELECT цепочки UNION ALL, потому что
	 *   первый SELECT достаточен для определения нужных столбцов.
	 */
	struct OnlyAnalyzeTag {};
	InterpreterSelectQuery(
		OnlyAnalyzeTag,
		ASTPtr query_ptr_,
		const Context & context_);

	void init(BlockInputStreamPtr input, const Names & required_column_names = Names{});
	void basicInit(BlockInputStreamPtr input);
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

	/** Из какой таблицы читать. При JOIN, возвращается "левая" таблица.
	 */
	void getDatabaseAndTableNames(String & database_name, String & table_name);

	/** Выбрать из списка столбцов какой-нибудь, лучше - минимального размера.
	 */
	String getAnyColumn();

	/// Разные стадии выполнения запроса.

	/// Вынимает данные из таблицы. Возвращает стадию, до которой запрос был обработан в Storage.
	QueryProcessingStage::Enum executeFetchColumns();

	void executeWhere(ExpressionActionsPtr expression);
	void executeAggregation(ExpressionActionsPtr expression, bool overflow_row, bool final);
	void executeMergeAggregated(bool overflow_row, bool final);
	void executeTotalsAndHaving(bool has_having, ExpressionActionsPtr expression, bool overflow_row);
	void executeHaving(ExpressionActionsPtr expression);
	void executeExpression(ExpressionActionsPtr expression);
	void executeOrder();
	void executeMergeSorted();
	void executePreLimit();
	void executeUnion();
	void executeLimit();
	void executeProjection(ExpressionActionsPtr expression);
	void executeDistinct(bool before_order, Names columns);
	void executeSubqueriesInSetsAndJoins(std::unordered_map<String, SubqueryForSet> & subqueries_for_sets);

	template <typename Transform>
	void transformStreams(Transform && transform)
	{
		for (auto & stream : streams)
			transform(stream);

		if (stream_with_non_joined_data)
			transform(stream_with_non_joined_data);
	}

	bool hasNoData() const
	{
		return streams.empty() && !stream_with_non_joined_data;
	}

	bool hasMoreThanOneStream() const
	{
		return streams.size() + (stream_with_non_joined_data ? 1 : 0) > 1;
	}


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
	NamesAndTypesList table_column_names;

	/** Потоки данных.
	  * Исходные потоки данных получаются в функции executeFetchColumns.
	  * Затем они преобразуются (оборачиваются в другие потоки) с помощью функций execute*,
	  *  чтобы получить целый конвейер выполнения запроса.
	  */
	BlockInputStreams streams;

	/** При выполнении FULL или RIGHT JOIN, здесь будет поток данных, из которого можно прочитать "неприсоединённые" строки.
	  * Он имеет особое значение, так как чтение из него должно осуществляться после чтения из основных потоков.
	  * Он подклеивается к основным потокам в UnionBlockInputStream или ParallelAggregatingBlockInputStream.
	  */
	BlockInputStreamPtr stream_with_non_joined_data;

	/// Являемся ли мы первым запросом SELECT цепочки UNION ALL?
	bool is_first_select_inside_union_all;

	/// Объект создан только для анализа запроса.
	bool only_analyze = false;

	/// Следующий запрос SELECT в цепочке UNION ALL, если есть.
	std::unique_ptr<InterpreterSelectQuery> next_select_in_union_all;

	/// Таблица, откуда читать данные, если не подзапрос.
	StoragePtr storage;
	IStorage::TableStructureReadLockPtr table_lock;

	/// Выполнить объединение потоков внутри запроса SELECT?
	bool union_within_single_query = false;

	Logger * log;
};

}
