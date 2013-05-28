#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Aggregator.h>
#include <DB/Interpreters/ExpressionActions.h>


namespace DB
{

/** Превращает выражение из синтаксического дерева в последовательность действий для его выполнения.
  */
class ExpressionAnalyzer : private boost::noncopyable
{
public:
	ExpressionAnalyzer(const ASTPtr & ast_, const Context & context_)
		: ast(ast_), context(context_), settings(context.getSettings()), columns(context.getColumns()), storage(getTable())
	{
		init();
	}

	/// columns - список известных столбцов (которых можно достать из таблицы).
	ExpressionAnalyzer(const ASTPtr & ast_, const Context & context_, const NamesAndTypesList & columns_)
		: ast(ast_), context(context_), settings(context.getSettings()), columns(columns_), storage(getTable())
	{
		init();
	}
	
	/// Есть ли в выражении агрегатные функции или секция GROUP BY или HAVING.
	bool hasAggregation() { return has_aggregation; }
	
	/// Получить список ключей агрегирования и описаний агрегатных функций, если в запросе есть GROUP BY.
	void getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates);

	
	/** Эти методы позволяют собрать цепочку преобразований над блоком, получающую значения в нужных секциях запроса.
	  *
	  * Пример использования:
	  *   ExpressionActionsChain chain;
	  *   analyzer.appendWhere(chain);
	  *   chain.addStep();
	  *   analyzer.appendSelect(chain);
	  *   analyzer.appendOrderBy(chain);
	  *   chain.finalize();
	  */
	
	/// До агрегации:
	bool appendWhere(ExpressionActionsChain & chain);
	bool appendGroupBy(ExpressionActionsChain & chain);
	void appendAggregateFunctionsArguments(ExpressionActionsChain & chain);
	
	/// После агрегации:
	bool appendHaving(ExpressionActionsChain & chain);
	void appendSelect(ExpressionActionsChain & chain);
	bool appendOrderBy(ExpressionActionsChain & chain);
	/// Удаляет все столбцы кроме выбираемых SELECT, упорядочивает оставшиеся столбцы и переименовывает их в алиасы.
	void appendProjectResult(ExpressionActionsChain & chain);
	
	/// Если ast не запрос SELECT, просто получает все действия для вычисления выражения.
	ExpressionActionsPtr getActions();
	
	/// Действия, которые можно сделать над пустым блоком: добавление констант и применение функций, зависящих только от констант.
	/// Не выполняет подзапросы.
	ExpressionActionsPtr getConstActions();
	
	/// Получить информацию об операции arrayJoin, если она есть.
	/// Если есть - в column_name будет записано имя столбца, находящегося внутри arrayJoin.
	/// TODO: 
	//bool getArrayJoinInfo(String & column_name);

private:
	ASTPtr ast;
	ASTSelectQuery * select_query;
	const Context & context;
	Settings settings;
	
	/// Известные столбцы.
	NamesAndTypesList columns;
	
	/// Таблица, из которой делается запрос. Используется для sign-rewrite'а
	const StoragePtr storage;
	/// Имя поля Sign в таблице. Непусто, если нужно осуществлять sign-rewrite
	String sign_column_name;
	
	bool has_aggregation;
	NamesAndTypesList aggregation_keys;
	AggregateDescriptions aggregate_descriptions;
	NamesAndTypesList aggregated_columns; /// Если нет агрегации, совпадает с columns.
	
	typedef std::map<String, ASTPtr> Aliases;
	Aliases aliases;
	
	typedef std::set<ASTPtr> SetOfASTs;
	typedef std::map<ASTPtr, ASTPtr> MapOfASTs;

	void init();

	NamesAndTypesList::iterator findColumn(const String & name);

	/** Создать словарь алиасов.
	  */
	void createAliasesDict(ASTPtr & ast);
		
	/** Для узлов-звёздочек - раскрыть их в список всех столбцов.
	  * Для узлов-литералов - подставить алиасы.
	  * Для агрегатных функций - если нужно, сделать sign rewrite.
	  */
	void normalizeTree();
	void normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts);
	
	/// Превратить перечисление значений или подзапрос в ASTSet. ast - функция in или notIn.
	void makeSet(ASTPtr ast);
	
	void getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActions & actions);
	
	void getActionsBeforeAggregationImpl(ASTPtr ast, ExpressionActions * actions, Names * result_columns);
	
	/// Добавить агрегатные функции в aggregate_descriptions.
	void getAggregatesImpl(ASTPtr ast, ExpressionActions & actions);
	
	/// Получить таблицу, из которой идет запрос
	StoragePtr getTable();
	
	/// Получить имя столбца Sign
	String getSignColumnName();
	
	/// Проверить нужно ли переписывать агрегатные функции для учета Sign
	bool needSignRewrite();
	
	/// Попробовать переписать агрегатную функцию для учета Sign
	void considerSignRewrite(ASTPtr & ast);
	
	ASTPtr createSignColumn();
	
	/// Заменить count() на sum(Sign)
	ASTPtr rewriteCount(const ASTFunction * node);
	/// Заменить sum(x) на sum(x * Sign)
	ASTPtr rewriteSum(const ASTFunction * node);
	/// Заменить avg(x) на sum(Sign * x) / sum(Sign)
	ASTPtr rewriteAvg(const ASTFunction * node);
	
	void initChain(ExpressionActionsChain & chain, NamesAndTypesList & columns);
	
	void assertSelect();
	void assertAggregation();
};

}
