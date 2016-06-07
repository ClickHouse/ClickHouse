#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/AggregateDescription.h>
#include <DB/Interpreters/Set.h>
#include <DB/Interpreters/Join.h>


namespace DB
{


class ExpressionActions;
struct ExpressionActionsChain;


/** Информация о том, что делать при выполнении подзапроса в секции [GLOBAL] IN/JOIN.
  */
struct SubqueryForSet
{
	/// Источник - получен с помощью InterpreterSelectQuery подзапроса.
	BlockInputStreamPtr source;
	Block source_sample;

	/// Если задано - создать из результата Set.
	SetPtr set;

	/// Если задано - создать из результата Join.
	JoinPtr join;

	/// Если задано - положить результат в таблицу.
	/// Это - временная таблица для передачи на удалённые серверы при распределённой обработке запроса.
	StoragePtr table;
};

/// ID подзапроса -> что с ним делать.
using SubqueriesForSets = std::unordered_map<String, SubqueryForSet>;


/** Превращает выражение из синтаксического дерева в последовательность действий для его выполнения.
  *
  * NOTE: если ast - запрос SELECT из таблицы, структура этой таблицы не должна меняться во все время жизни ExpressionAnalyzer-а.
  */
class ExpressionAnalyzer : private boost::noncopyable
{
private:
	using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
	ExpressionAnalyzer(
		const ASTPtr & ast_,
		const Context & context_,
		StoragePtr storage_,
		const NamesAndTypesList & columns_,
		size_t subquery_depth_ = 0,
		bool do_global_ = false)
		:
		ast(ast_), context(context_), settings(context.getSettings()),
		subquery_depth(subquery_depth_), columns(columns_),
		storage(storage_ ? storage_ : getTable()),
		do_global(do_global_)
	{
		init();
	}

	/// Есть ли в выражении агрегатные функции или секция GROUP BY или HAVING.
	bool hasAggregation() const { return has_aggregation; }

	/// Получить список ключей агрегирования и описаний агрегатных функций, если в запросе есть GROUP BY.
	void getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const;

	/** Получить набор столбцов, которых достаточно прочитать из таблицы для вычисления выражения.
	  * Не учитываются столбцы, добавляемые из другой таблицы путём JOIN-а.
	  */
	Names getRequiredColumns();

	/** Эти методы позволяют собрать цепочку преобразований над блоком, получающую значения в нужных секциях запроса.
	  *
	  * Пример использования:
	  *   ExpressionActionsChain chain;
	  *   analyzer.appendWhere(chain);
	  *   chain.addStep();
	  *   analyzer.appendSelect(chain);
	  *   analyzer.appendOrderBy(chain);
	  *   chain.finalize();
	  *
	  * Если указано only_types = true, не выполняет подзапросы в соответствующих частях запроса. Полученные таким
	  *  образом действия не следует выполнять, они нужны только чтобы получить список столбцов с их типами.
	  */

	/// До агрегации:
	bool appendArrayJoin(ExpressionActionsChain & chain, bool only_types);
	bool appendJoin(ExpressionActionsChain & chain, bool only_types);
	bool appendWhere(ExpressionActionsChain & chain, bool only_types);
	bool appendGroupBy(ExpressionActionsChain & chain, bool only_types);
	void appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types);

	/// После агрегации:
	bool appendHaving(ExpressionActionsChain & chain, bool only_types);
	void appendSelect(ExpressionActionsChain & chain, bool only_types);
	bool appendOrderBy(ExpressionActionsChain & chain, bool only_types);
	/// Удаляет все столбцы кроме выбираемых SELECT, упорядочивает оставшиеся столбцы и переименовывает их в алиасы.
	void appendProjectResult(ExpressionActionsChain & chain, bool only_types) const;

	/// Если ast не запрос SELECT, просто получает все действия для вычисления выражения.
	/// Если project_result, в выходном блоке останутся только вычисленные значения в нужном порядке, переименованные в алиасы.
	/// Иначе, из блока будут удаляться только временные столбцы.
	ExpressionActionsPtr getActions(bool project_result);

	/// Действия, которые можно сделать над пустым блоком: добавление констант и применение функций, зависящих только от констант.
	/// Не выполняет подзапросы.
	ExpressionActionsPtr getConstActions();

	/** Множества, для создания которых нужно будет выполнить подзапрос.
	  * Только множества, нужные для выполнения действий, возвращенных из уже вызванных append* или getActions.
	  * То есть, нужно вызвать getSetsWithSubqueries после всех вызовов append* или getActions
	  *  и создать все возвращенные множества перед выполнением действий.
	  */
	SubqueriesForSets getSubqueriesForSets() { return subqueries_for_sets; }

	/** Таблицы, которые надо будет отправить на удалённые серверы при распределённой обработке запроса.
	  */
	const Tables & getExternalTables() const { return external_tables; }

	/// Если ast - запрос SELECT, получает имена (алиасы) и типы столбцов из секции SELECT.
	Block getSelectSampleBlock();

	/// Создаем какие сможем Set из секции IN для использования индекса по ним.
	void makeSetsForIndex();

private:
	ASTPtr ast;
	ASTSelectQuery * select_query;
	const Context & context;
	Settings settings;
	size_t subquery_depth;

	/// Столбцы, которые упоминаются в выражении, но не были заданы в конструкторе.
	NameSet unknown_required_columns;

	/** Исходные столбцы.
	  * Сначала сюда помещаются все доступные столбцы таблицы. Затем (при разборе запроса) удаляются неиспользуемые столбцы.
	  */
	NamesAndTypesList columns;

	/// Столбцы после ARRAY JOIN, JOIN и/или агрегации.
	NamesAndTypesList aggregated_columns;

	/// Таблица, из которой делается запрос.
	const StoragePtr storage;

	bool has_aggregation = false;
	NamesAndTypesList aggregation_keys;
	AggregateDescriptions aggregate_descriptions;

	SubqueriesForSets subqueries_for_sets;

	/// NOTE: Пока поддерживается только один JOIN на запрос.

	/** Запрос вида SELECT expr(x) AS k FROM t1 ANY LEFT JOIN (SELECT expr(x) AS k FROM t2) USING k
	  * Соединение делается по столбцу k.
	  * Во время JOIN-а,
	  *  - в "правой" таблице, он будет доступен по алиасу k, так как было выполнено действие Project для подзапроса.
	  *  - в "левой" таблице, он будет доступен по имени expr(x), так как ещё не было выполнено действие Project.
	  * Надо запомнить оба этих варианта.
	  */
	Names join_key_names_left;
	Names join_key_names_right;

	NamesAndTypesList columns_added_by_join;

	typedef std::unordered_map<String, ASTPtr> Aliases;
	Aliases aliases;

	typedef std::set<const IAST *> SetOfASTs;
	typedef std::map<ASTPtr, ASTPtr> MapOfASTs;

	/// Какой столбец нужно по-ARRAY-JOIN-ить, чтобы получить указанный.
	/// Например, для SELECT s.v ... ARRAY JOIN a AS s сюда попадет "s.v" -> "a.v".
	NameToNameMap array_join_result_to_source;

	/// Для секции ARRAY JOIN отображение из алиаса в полное имя столбца.
	/// Например, для ARRAY JOIN [1,2] AS b сюда попадет "b" -> "array(1,2)".
	NameToNameMap array_join_alias_to_name;

	/// Обратное отображение для array_join_alias_to_name.
	NameToNameMap array_join_name_to_alias;

	/// Нужно ли подготавливать к выполнению глобальные подзапросы при анализировании запроса.
	bool do_global;

	/// Все новые временные таблицы, полученные при выполнении подзапросов GLOBAL IN/JOIN.
	Tables external_tables;
	size_t external_table_id = 1;


	void init();

	static NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols);
	NamesAndTypesList::iterator findColumn(const String & name) { return findColumn(name, columns); }

	/** Из списка всех доступных столбцов таблицы (columns) удалить все ненужные.
	  * Заодно, сформировать множество неизвестных столбцов (unknown_required_columns),
	  * а также столбцов, добавленных JOIN-ом (columns_added_by_join).
	  */
	void collectUsedColumns();

	/** Найти столбцы, получаемые путём JOIN-а.
	  */
	void collectJoinedColumns(NameSet & joined_columns, NamesAndTypesList & joined_columns_name_type);

	/** Создать словарь алиасов.
	  */
	void addASTAliases(ASTPtr & ast, int ignore_levels = 0);

	/** Для узлов-звёздочек - раскрыть их в список всех столбцов.
	  * Для узлов-литералов - подставить алиасы.
	  */
	void normalizeTree();
	void normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias);

	///	Eliminates injective function calls and constant expressions from group by statement
	void optimizeGroupBy();

	/// Удалить из ORDER BY повторяющиеся элементы.
	void optimizeOrderBy();

	/// Превратить перечисление значений или подзапрос в ASTSet. node - функция in или notIn.
	void makeSet(ASTFunction * node, const Block & sample_block);

	/// Добавляет список ALIAS столбцов из таблицы
	void addAliasColumns();

	/// Замена скалярных подзапросов на значения-константы.
	void executeScalarSubqueries();
	void executeScalarSubqueriesImpl(ASTPtr & ast);

	/// Находит глобальные подзапросы в секциях GLOBAL IN/JOIN. Заполняет external_tables.
	void initGlobalSubqueriesAndExternalTables();
	void initGlobalSubqueries(ASTPtr & ast);

	/// Находит в запросе использования внешних таблиц (в виде идентификаторов таблиц). Заполняет external_tables.
	void findExternalTables(ASTPtr & ast);

	/** Инициализировать InterpreterSelectQuery для подзапроса в секции GLOBAL IN/JOIN,
	  * создать временную таблицу типа Memory и запомнить это в словаре external_tables.
	  */
	void addExternalStorage(ASTPtr & subquery_or_table_name);

	void getArrayJoinedColumns();
	void getArrayJoinedColumnsImpl(ASTPtr ast);
	void addMultipleArrayJoinAction(ExpressionActionsPtr & actions) const;

	void addJoinAction(ExpressionActionsPtr & actions, bool only_types) const;

	struct ScopeStack;
	void getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack);

	void getRootActions(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActionsPtr & actions);

	void getActionsBeforeAggregation(ASTPtr ast, ExpressionActionsPtr & actions, bool no_subqueries);

	/** Добавить ключи агрегации в aggregation_keys, агрегатные функции в aggregate_descriptions,
	  * Создать набор столбцов aggregated_columns, получаемых после агрегации, если она есть,
	  *  или после всех действий, которые обычно выполняются до агрегации.
	  * Установить has_aggregation = true, если есть GROUP BY или хотя бы одна агрегатная функция.
	  */
	void analyzeAggregation();
	void getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions);
	void assertNoAggregates(const ASTPtr & ast, const char * description);

	/** Получить множество нужных столбцов для чтения из таблицы.
	  * При этом, столбцы, указанные в ignored_names, считаются ненужными. И параметр ignored_names может модифицироваться.
	  * Множество столбцов available_joined_columns - столбцы, доступные из JOIN-а, они не нужны для чтения из основной таблицы.
	  * Положить в required_joined_columns множество столбцов, доступных из JOIN-а и востребованных.
	  */
	void getRequiredColumnsImpl(ASTPtr ast,
		NameSet & required_columns, NameSet & ignored_names,
		const NameSet & available_joined_columns, NameSet & required_joined_columns);

	/// Получить таблицу, из которой идет запрос
	StoragePtr getTable();

	/// columns - столбцы, присутствующие до начала преобразований.
	void initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const;

	void assertSelect() const;
	void assertAggregation() const;

	/** Создать Set из явного перечисления значений в запросе.
	  * Если create_ordered_set = true - создать структуру данных, подходящую для использования индекса.
	  */
	void makeExplicitSet(ASTFunction * node, const Block & sample_block, bool create_ordered_set);
	void makeSetsForIndexImpl(ASTPtr & node, const Block & sample_block);
};

}
