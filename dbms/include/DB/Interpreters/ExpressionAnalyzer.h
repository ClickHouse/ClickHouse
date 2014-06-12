#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/Aggregator.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Interpreters/Set.h>
#include <DB/Interpreters/Join.h>


namespace DB
{

/** Превращает выражение из синтаксического дерева в последовательность действий для его выполнения.
  *
  * NOTE: если ast - запрос SELECT из таблицы, структура этой таблицы не должна меняться во все время жизни ExpressionAnalyzer-а.
  */
class ExpressionAnalyzer : private boost::noncopyable
{
public:
	ExpressionAnalyzer(const ASTPtr & ast_, const Context & context_, size_t subquery_depth_ = 0, bool do_global_ = false)
		: ast(ast_), context(context_), settings(context.getSettings()),
		subquery_depth(subquery_depth_), columns(context.getColumns()), storage(getTable()), do_global(do_global_)
	{
		init();
	}

	ExpressionAnalyzer(const ASTPtr & ast_, const Context & context_, StoragePtr storage_, size_t subquery_depth_ = 0, bool do_global_ = false)
		: ast(ast_), context(context_), settings(context.getSettings()),
		subquery_depth(subquery_depth_), columns(context.getColumns()), storage(storage_ ? storage_ : getTable()), do_global(do_global_)
	{
		init();
	}

	/// columns - список известных столбцов (которых можно достать из таблицы).
	ExpressionAnalyzer(const ASTPtr & ast_, const Context & context_, const NamesAndTypesList & columns_, size_t subquery_depth_ = 0, bool do_global_ = false)
		: ast(ast_), context(context_), settings(context.getSettings()),
		subquery_depth(subquery_depth_), columns(columns_), storage(getTable()), do_global(do_global_)
	{
		init();
	}

	/// Есть ли в выражении агрегатные функции или секция GROUP BY или HAVING.
	bool hasAggregation() { return has_aggregation; }

	/// Получить список ключей агрегирования и описаний агрегатных функций, если в запросе есть GROUP BY.
	void getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates);

	/// Получить набор столбцов, которые достаточно прочесть для вычисления выражения.
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
	  * Если указано only_types=true, не выполняет подзапросы в соответствующих частях запроса. Полученные таким
	  *  образом действия не следует выполнять, они нужны только чтобы получить список столбцов с их типами.
	  */

	/// До агрегации:
	bool appendArrayJoin(ExpressionActionsChain & chain, bool only_types);
	bool appendWhere(ExpressionActionsChain & chain, bool only_types);
	bool appendGroupBy(ExpressionActionsChain & chain, bool only_types);
	void appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types);

	/// После агрегации:
	bool appendHaving(ExpressionActionsChain & chain, bool only_types);
	void appendSelect(ExpressionActionsChain & chain, bool only_types);
	bool appendOrderBy(ExpressionActionsChain & chain, bool only_types);
	/// Удаляет все столбцы кроме выбираемых SELECT, упорядочивает оставшиеся столбцы и переименовывает их в алиасы.
	void appendProjectResult(ExpressionActionsChain & chain, bool only_types);

	/// Если ast не запрос SELECT, просто получает все действия для вычисления выражения.
	/// Если project_result, в выходном блоке останутся только вычисленные значения в нужном порядке, переименованные в алиасы.
	/// Иначе, из блока будут удаляться только временные столбцы.
	ExpressionActionsPtr getActions(bool project_result);

	/// Действия, которые можно сделать над пустым блоком: добавление констант и применение функций, зависящих только от констант.
	/// Не выполняет подзапросы.
	ExpressionActionsPtr getConstActions();

	/** Множества, для создания которых нужно будет выполнить подзапрос.
	  * Только множества, нужные для выполнения действий, возвращенных из уже вызванных append* или getActions.
	  * То есть, нужно вызвать getSubquerySets после всех вызовов append* или getActions и создать все возвращенные множества перед выполнением действий.
	  */
	Sets getSetsWithSubqueries();
	Joins getJoinsWithSubqueries();

	/// Если ast - запрос SELECT, получает имена (алиасы) и типы столбцов из секции SELECT.
	Block getSelectSampleBlock();

	/// Все новые временные таблицы, полученные при выполнении подзапросов GLOBAL IN.
	Tables external_tables;
	std::map<String, BlockInputStreamPtr> external_data;
	size_t external_table_id;

	/// Создаем какие сможем Set из секции In для использования индекса по ним
	void makeSetsForIndex();
private:
	typedef std::set<String> NamesSet;

	ASTPtr ast;
	ASTSelectQuery * select_query;
	const Context & context;
	Settings settings;
	size_t subquery_depth;

	/// Столбцы, которые упоминаются в выражении, но не были заданы в конструкторе.
	NameSet unknown_required_columns;

	/// Исходные столбцы.
	NamesAndTypesList columns;
	/// Столбцы после ARRAY JOIN и/или агрегации.
	NamesAndTypesList aggregated_columns;

	/// Таблица, из которой делается запрос. Используется для sign-rewrite'а
	const StoragePtr storage;
	/// Имя поля Sign в таблице. Непусто, если нужно осуществлять sign-rewrite
	String sign_column_name;

	bool has_aggregation;
	NamesAndTypesList aggregation_keys;
	AggregateDescriptions aggregate_descriptions;

	std::unordered_map<String, SetPtr> sets_with_subqueries;

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

	/// Вычислять ли результат глобальных селектов при анализировании запроса.
	bool do_global;

	/** Для getActionsImpl.
	  * Стек из ExpressionActions, соответствующих вложенным лямбда-выражениям.
	  * Новое действие нужно добавлять на самый высокий возможный уровень.
	  * Например, в выражении "select arrayMap(x -> x + column1 * column2, array1)"
	  *  вычисление произведения нужно делать вне лямбда-выражения (оно не зависит от x), а вычисление суммы - внутри (зависит от x).
	  */
	struct ScopeStack
	{
		struct Level
		{
			ExpressionActionsPtr actions;
			NameSet new_columns;
		};

		typedef std::vector<Level> Levels;

		Levels stack;
		Settings settings;

		ScopeStack(const ExpressionActions & actions, const Settings & settings_)
			: settings(settings_)
		{
			stack.push_back(Level());
			stack.back().actions = new ExpressionActions(actions);
			const NamesAndTypesList & input_columns = actions.getSampleBlock().getColumnsList();
			for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
				stack.back().new_columns.insert(it->first);
		}

		void pushLevel(const NamesAndTypesList & input_columns)
		{
			stack.push_back(Level());
			Level & prev = stack[stack.size() - 2];

			ColumnsWithNameAndType prev_columns = prev.actions->getSampleBlock().getColumns();

			ColumnsWithNameAndType all_columns;
			NameSet new_names;

			for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
			{
				all_columns.push_back(ColumnWithNameAndType(nullptr, it->second, it->first));
				new_names.insert(it->first);
				stack.back().new_columns.insert(it->first);
			}

			for (ColumnsWithNameAndType::const_iterator it = prev_columns.begin(); it != prev_columns.end(); ++it)
			{
				if (!new_names.count(it->name))
					all_columns.push_back(*it);
			}

			stack.back().actions = new ExpressionActions(all_columns, settings);
		}

		size_t getColumnLevel(const std::string & name)
		{
			for (int i = static_cast<int>(stack.size()) - 1; i >= 0; --i)
			{
				if (stack[i].new_columns.count(name))
					return i;
			}

			throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
		}

		void addAction(const ExpressionAction & action, const Names & additional_required_columns = Names())
		{
			size_t level = 0;
			for (size_t i = 0; i < additional_required_columns.size(); ++i)
				level = std::max(level, getColumnLevel(additional_required_columns[i]));
			Names required = action.getNeededColumns();
			for (size_t i = 0; i < required.size(); ++i)
				level = std::max(level, getColumnLevel(required[i]));

			Names added;
			stack[level].actions->add(action, added);

			stack[level].new_columns.insert(added.begin(), added.end());

			for (size_t i = 0; i < added.size(); ++i)
			{
				const ColumnWithNameAndType & col = stack[level].actions->getSampleBlock().getByName(added[i]);
				for (size_t j = level + 1; j < stack.size(); ++j)
					stack[j].actions->addInput(col);
			}
		}

		ExpressionActionsPtr popLevel()
		{
			ExpressionActionsPtr res = stack.back().actions;
			stack.pop_back();
			return res;
		}

		const Block & getSampleBlock()
		{
			return stack.back().actions->getSampleBlock();
		}
	};

	void init();

	NamesAndTypesList::iterator findColumn(const String & name, NamesAndTypesList & cols);
	NamesAndTypesList::iterator findColumn(const String & name) { return findColumn(name, columns); }

	void removeUnusedColumns();

	/** Создать словарь алиасов.
	  */
	void createAliasesDict(ASTPtr & ast, int ignore_levels = 0);

	/** Для узлов-звёздочек - раскрыть их в список всех столбцов.
	  * Для узлов-литералов - подставить алиасы.
	  * Для агрегатных функций - если нужно, сделать sign rewrite.
	  */
	void normalizeTree();
	void normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, bool in_sign_rewritten);

	/// Обходит запрос и сохраняет найденные глобальные функции (например GLOBAL IN)
	void findGlobalFunctions(ASTPtr & ast, std::vector<ASTPtr> & global_nodes);
	void findExternalTables(ASTPtr & ast);

	/// Превратить перечисление значений или подзапрос в ASTSet. node - функция in или notIn.
	void makeSet(ASTFunction * node, const Block & sample_block);
	/// Запустить подзапрос в секции GLOBAL IN, создать временную таблицу типа memory и запомнить эту пару в переменной external_tables
	void addExternalStorage(ASTFunction * node);

	void getArrayJoinedColumns();
	void getArrayJoinedColumnsImpl(ASTPtr ast);
	void addMultipleArrayJoinAction(ExpressionActions & actions);

	void getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack);

	void getRootActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActions & actions);

	void getActionsBeforeAggregationImpl(ASTPtr ast, ExpressionActions & actions, bool no_subqueries);

	/// Добавить агрегатные функции в aggregate_descriptions.
	/// Установить has_aggregation=true, если есть хоть одна агрегатная функция.
	void getAggregatesImpl(ASTPtr ast, ExpressionActions & actions);

	void getRequiredColumnsImpl(ASTPtr ast, NamesSet & required_columns, NamesSet & ignored_names);

	/// Получить таблицу, из которой идет запрос
	StoragePtr getTable();

	/// Получить имя столбца Sign
	String getSignColumnName();

	/// Проверить нужно ли переписывать агрегатные функции для учета Sign
	bool needSignRewrite();

	/// Попробовать переписать агрегатную функцию для учета Sign
	bool considerSignRewrite(ASTPtr & ast);

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
	void assertArrayJoin();

	void makeExplicitSet(ASTFunction * node, const Block & sample_block, bool create_ordered_set);
	void makeSetsForIndexRecursively(ASTPtr & node, const Block & sample_block);
};

}
