#include <Poco/Util/Application.h>

#include <DB/DataTypes/FieldToDataType.h>

#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTAsterisk.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTSubquery.h>
#include <DB/Parsers/ASTSet.h>
#include <DB/Parsers/ASTOrderByElement.h>

#include <DB/DataTypes/DataTypeSet.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/DataTypes/DataTypeExpression.h>
#include <DB/DataTypes/DataTypeNested.h>

#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnExpression.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>
#include <DB/Interpreters/ExpressionActions.h>
#include <DB/Interpreters/InJoinSubqueriesPreprocessor.h>
#include <DB/Interpreters/LogicalExpressionsOptimizer.h>
#include <DB/Interpreters/ExternalDictionaries.h>

#include <DB/AggregateFunctions/AggregateFunctionFactory.h>

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/StorageMemory.h>
#include <DB/Storages/StorageSet.h>
#include <DB/Storages/StorageJoin.h>

#include <DB/DataStreams/LazyBlockInputStream.h>
#include <DB/DataStreams/copyData.h>

#include <DB/Dictionaries/IDictionary.h>

#include <DB/Common/typeid_cast.h>

#include <DB/Parsers/formatAST.h>

#include <DB/Functions/FunctionFactory.h>

#include <ext/range.hpp>
#include <DB/DataTypes/DataTypeFactory.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int MULTIPLE_EXPRESSIONS_FOR_ALIAS;
	extern const int UNKNOWN_IDENTIFIER;
	extern const int CYCLIC_ALIASES;
	extern const int INCORRECT_RESULT_OF_SCALAR_SUBQUERY;
	extern const int TOO_MUCH_ROWS;
	extern const int NOT_FOUND_COLUMN_IN_BLOCK;
	extern const int INCORRECT_ELEMENT_OF_SET;
	extern const int ALIAS_REQUIRED;
	extern const int EMPTY_NESTED_TABLE;
	extern const int NOT_AN_AGGREGATE;
	extern const int UNEXPECTED_EXPRESSION;
	extern const int PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS;
	extern const int DUPLICATE_COLUMN;
	extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
	extern const int ILLEGAL_AGGREGATION;
}


/** Calls to these functions in the GROUP BY statement would be
  * replaced by their immediate argument.
  */
const std::unordered_set<String> injective_function_names
{
	"negate",
	"bitNot",
	"reverse",
	"reverseUTF8",
	"toString",
	"toFixedString",
	"toStringCutToZero",
	"IPv4NumToString",
	"IPv4StringToNum",
	"hex",
	"unhex",
	"bitmaskToList",
	"bitmaskToArray",
	"tuple",
	"regionToName",
	"concatAssumeInjective",
};

const std::unordered_set<String> possibly_injective_function_names
{
	"dictGetString",
	"dictGetUInt8",
	"dictGetUInt16",
	"dictGetUInt32",
	"dictGetUInt64",
	"dictGetInt8",
	"dictGetInt16",
	"dictGetInt32",
	"dictGetInt64",
	"dictGetFloat32",
	"dictGetFloat64",
	"dictGetDate",
	"dictGetDateTime"
};

namespace
{

bool functionIsInOperator(const String & name)
{
	return name == "in" || name == "notIn";
}

bool functionIsInOrGlobalInOperator(const String & name)
{
	return name == "in" || name == "notIn" || name == "globalIn" || name == "globalNotIn";
}

}

void ExpressionAnalyzer::init()
{
	select_query = typeid_cast<ASTSelectQuery *>(ast.get());

	/// В зависимости от профиля пользователя проверить наличие прав на выполнение
	/// распределённых подзапросов внутри секций IN или JOIN и обработать эти подзапросы.
	InJoinSubqueriesPreprocessor<>(select_query, context, storage).perform();

	/// Оптимизирует логические выражения.
	LogicalExpressionsOptimizer(select_query, settings).perform();

	/// Создаёт словарь aliases: alias -> ASTPtr
	addASTAliases(ast);

	/// Common subexpression elimination. Rewrite rules.
	normalizeTree();

	/// ALIAS столбцы не должны подставляться вместо ASTAsterisk, добавим их теперь, после normalizeTree.
	addAliasColumns();

	/// Выполнение скалярных подзапросов - замена их на значения-константы.
	executeScalarSubqueries();

	/// GROUP BY injective function elimination.
	optimizeGroupBy();

	/// Удалить из ORDER BY повторяющиеся элементы.
	optimizeOrderBy();

	/// array_join_alias_to_name, array_join_result_to_source.
	getArrayJoinedColumns();

	/// Удалить ненужное из списка columns. Создать unknown_required_columns. Сформировать columns_added_by_join.
	collectUsedColumns();

	/// external_tables, subqueries_for_sets для глобальных подзапросов.
	/// Заменяет глобальные подзапросы на сгенерированные имена временных таблиц, которые будут отправлены на удалённые серверы.
	initGlobalSubqueriesAndExternalTables();

	/// has_aggregation, aggregation_keys, aggregate_descriptions, aggregated_columns.
	/// Этот анализ надо провести после обработки глобальных подзапросов, потому что в противном случае,
	/// если агрегатная функция содержит глобальный подзапрос, то метод analyzeAggregation сохранит
	/// в aggregate_descriptions информацию о параметрах этой агрегатной функции, среди которых окажется
	/// глобальный подзапрос. Затем при вызове метода initGlobalSubqueriesAndExternalTables, этот
	/// глобальный подзапрос будет заменён на временную таблицу, в результате чего aggregate_descriptions
	/// будет содержать устаревшую информацию, что приведёт к ошибке при выполнении запроса.
	analyzeAggregation();
}


void ExpressionAnalyzer::analyzeAggregation()
{
	/** Найдем ключи агрегации (aggregation_keys), информацию об агрегатных функциях (aggregate_descriptions),
	 *  а также набор столбцов, получаемых после агрегации, если она есть,
	 *  или после всех действий, которые обычно выполняются до агрегации (aggregated_columns).
	 *
	 * Всё, что ниже (составление временных ExpressionActions) - только в целях анализа запроса (вывода типов).
	 */

	if (select_query && (select_query->group_expression_list || select_query->having_expression))
		has_aggregation = true;

	ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(columns, settings);

	if (select_query && select_query->array_join_expression_list)
	{
		getRootActions(select_query->array_join_expression_list, true, false, temp_actions);
		addMultipleArrayJoinAction(temp_actions);
	}

	if (select_query && select_query->join)
	{
		auto join = typeid_cast<ASTJoin &>(*select_query->join);
		if (join.using_expr_list)
			getRootActions(join.using_expr_list, true, false, temp_actions);

		addJoinAction(temp_actions, true);
	}

	getAggregates(ast, temp_actions);

	if (has_aggregation)
	{
		assertSelect();

		/// Найдем ключи агрегации.
		if (select_query->group_expression_list)
		{
			NameSet unique_keys;
			auto & group_asts = select_query->group_expression_list->children;
			for (size_t i = 0; i < group_asts.size(); ++i)
			{
				getRootActions(group_asts[i], true, false, temp_actions);

				const auto & column_name = group_asts[i]->getColumnName();
				const auto & block = temp_actions->getSampleBlock();

				if (!block.has(column_name))
					throw Exception("Unknown identifier (in GROUP BY): " + column_name, ErrorCodes::UNKNOWN_IDENTIFIER);

				const auto & col = block.getByName(column_name);

				/// constant expressions have non-null column pointer at this stage
				if (const auto is_constexpr = col.column)
				{
					/// but don't remove last key column if no aggregate functions, otherwise aggregation will not work
					if (!aggregate_descriptions.empty() || group_asts.size() > 1)
					{
						if (i < group_asts.size() - 1)
							group_asts[i] = std::move(group_asts.back());

						group_asts.pop_back();
						i -= 1;

						continue;
					}
				}

				NameAndTypePair key{column_name, col.type};

				/// Ключи агрегации уникализируются.
				if (!unique_keys.count(key.name))
				{
					unique_keys.insert(key.name);
					aggregation_keys.push_back(key);

					/// key is no longer needed, therefore we can save a little by moving it
					aggregated_columns.push_back(std::move(key));
				}
			}

			if (group_asts.empty())
			{
				select_query->group_expression_list = nullptr;
				has_aggregation = select_query->having_expression || aggregate_descriptions.size();
			}
		}

		for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
		{
			AggregateDescription & desc = aggregate_descriptions[i];
			aggregated_columns.emplace_back(desc.column_name, desc.function->getReturnType());
		}
	}
	else
	{
		aggregated_columns = temp_actions->getSampleBlock().getColumnsList();
	}
}


void ExpressionAnalyzer::initGlobalSubqueriesAndExternalTables()
{
	/// Добавляет уже существующие внешние таблицы (не подзапросы) в словарь external_tables.
	findExternalTables(ast);

	/// Преобразует GLOBAL-подзапросы во внешние таблицы; кладёт их в словарь external_tables: name -> StoragePtr.
	initGlobalSubqueries(ast);
}


void ExpressionAnalyzer::initGlobalSubqueries(ASTPtr & ast)
{
	/// Рекурсивные вызовы. Не опускаемся в подзапросы.

	for (auto & child : ast->children)
		if (!typeid_cast<ASTSelectQuery *>(child.get()))
			initGlobalSubqueries(child);

	/// Действия, выполняемые снизу вверх.

	if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
	{
		/// Для GLOBAL IN.
		if (do_global && (node->name == "globalIn" || node->name == "globalNotIn"))
			addExternalStorage(node->arguments->children.at(1));
	}
	else if (ASTJoin * node = typeid_cast<ASTJoin *>(ast.get()))
	{
		/// Для GLOBAL JOIN.
		if (do_global && node->locality == ASTJoin::Global)
			addExternalStorage(node->table);
	}
}


void ExpressionAnalyzer::findExternalTables(ASTPtr & ast)
{
	/// Обход снизу. Намеренно опускаемся в подзапросы.
	for (auto & child : ast->children)
		findExternalTables(child);

	/// Если идентификатор типа таблица
	StoragePtr external_storage;

	if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
		if (node->kind == ASTIdentifier::Table)
			if ((external_storage = context.tryGetExternalTable(node->name)))
				external_tables[node->name] = external_storage;
}


static SharedPtr<InterpreterSelectQuery> interpretSubquery(
	ASTPtr & subquery_or_table_name, const Context & context, size_t subquery_depth, const Names & required_columns);


void ExpressionAnalyzer::addExternalStorage(ASTPtr & subquery_or_table_name)
{
	/// При нераспределённых запросах, создание временных таблиц не имеет смысла.
	if (!(storage && storage->isRemote()))
		return;

	if (const ASTIdentifier * table = typeid_cast<const ASTIdentifier *>(subquery_or_table_name.get()))
	{
		/// Если это уже внешняя таблица, ничего заполять не нужно. Просто запоминаем ее наличие.
		if (external_tables.end() != external_tables.find(table->name))
			return;
	}

	/// Сгенерируем имя для внешней таблицы.
	String external_table_name = "_data" + toString(external_table_id);
	while (external_tables.count(external_table_name))
	{
		++external_table_id;
		external_table_name = "_data" + toString(external_table_id);
	}

	SharedPtr<InterpreterSelectQuery> interpreter = interpretSubquery(subquery_or_table_name, context, subquery_depth, {});

	Block sample = interpreter->getSampleBlock();
	NamesAndTypesListPtr columns = new NamesAndTypesList(sample.getColumnsList());

	StoragePtr external_storage = StorageMemory::create(external_table_name, columns);

	/** Есть два способа выполнения распределённых GLOBAL-подзапросов.
	  *
	  * Способ push:
	  * Данные подзапроса отправляются на все удалённые серверы, где они затем используются.
	  * Для этого способа, данные отправляются в виде "внешних таблиц" и будут доступны на каждом удалённом сервере по имени типа _data1.
	  * Заменяем в запросе подзапрос на это имя.
	  *
	  * Способ pull:
	  * Удалённые серверы скачивают данные подзапроса с сервера-инициатора запроса.
	  * Для этого способа, заменяем подзапрос на другой подзапрос вида (SELECT * FROM remote('host:port', _query_QUERY_ID, _data1))
	  * Этот подзапрос, по факту, говорит - "надо скачать данные оттуда".
	  *
	  * Способ pull имеет преимущество, потому что в нём удалённый сервер может решить, что ему не нужны данные и не скачивать их в таких случаях.
	  */

	if (settings.global_subqueries_method == GlobalSubqueriesMethod::PUSH)
	{
		/** Заменяем подзапрос на имя временной таблицы.
		  * Именно в таком виде, запрос отправится на удалённый сервер.
		  * На удалённый сервер отправится эта временная таблица, и на его стороне,
		  *  вместо выполнения подзапроса, надо будет просто из неё прочитать.
		  */

		subquery_or_table_name = new ASTIdentifier(StringRange(), external_table_name, ASTIdentifier::Table);
	}
	else if (settings.global_subqueries_method == GlobalSubqueriesMethod::PULL)
	{
		String host_port = getFQDNOrHostName() + ":" + toString(context.getTCPPort());
		String database = "_query_" + context.getCurrentQueryId();

		auto subquery = new ASTSubquery;
		subquery_or_table_name = subquery;

		auto select = new ASTSelectQuery;
		subquery->children.push_back(select);

		auto exp_list = new ASTExpressionList;
		select->select_expression_list = exp_list;
		select->children.push_back(select->select_expression_list);

		Names column_names = external_storage->getColumnNamesList();
		for (const auto & name : column_names)
			exp_list->children.push_back(new ASTIdentifier({}, name));

		auto table_func = new ASTFunction;
		select->table = table_func;
		select->children.push_back(select->table);

		table_func->name = "remote";
		auto args = new ASTExpressionList;
		table_func->arguments = args;
		table_func->children.push_back(table_func->arguments);

		auto address_lit = new ASTLiteral({}, host_port);
		args->children.push_back(address_lit);

		auto database_lit = new ASTLiteral({}, database);
		args->children.push_back(database_lit);

		auto table_lit = new ASTLiteral({}, external_table_name);
		args->children.push_back(table_lit);
	}
	else
		throw Exception("Unknown global subqueries execution method", ErrorCodes::UNKNOWN_GLOBAL_SUBQUERIES_METHOD);

	external_tables[external_table_name] = external_storage;
	subqueries_for_sets[external_table_name].source = interpreter->execute().in;
	subqueries_for_sets[external_table_name].source_sample = interpreter->getSampleBlock();
	subqueries_for_sets[external_table_name].table = external_storage;

	/** NOTE Если было написано IN tmp_table - существующая временная (но не внешняя) таблица,
	  *  то здесь будет создана новая временная таблица (например, _data1),
	  *  и данные будут затем в неё скопированы.
	  * Может быть, этого можно избежать.
	  */
}


NamesAndTypesList::iterator ExpressionAnalyzer::findColumn(const String & name, NamesAndTypesList & cols)
{
	return std::find_if(cols.begin(), cols.end(),
		[&](const NamesAndTypesList::value_type & val) { return val.name == name; });
}


/// ignore_levels - алиасы в скольки верхних уровнях поддерева нужно игнорировать.
/// Например, при ignore_levels=1 ast не может быть занесен в словарь, но его дети могут.
void ExpressionAnalyzer::addASTAliases(ASTPtr & ast, int ignore_levels)
{
	ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get());

	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	for (auto & child : ast->children)
	{
		int new_ignore_levels = std::max(0, ignore_levels - 1);

		/// Алиасы верхнего уровня в секции ARRAY JOIN имеют особый смысл, их добавлять не будем
		///  (пропустим сам expression list и его детей).
		if (select && child == select->array_join_expression_list)
			new_ignore_levels = 2;

		if (!typeid_cast<ASTSelectQuery *>(child.get()))
			addASTAliases(child, new_ignore_levels);
	}

	if (ignore_levels > 0)
		return;

	String alias = ast->tryGetAlias();
	if (!alias.empty())
	{
		if (aliases.count(alias) && ast->getTreeID() != aliases[alias]->getTreeID())
			throw Exception("Different expressions with the same alias " + alias, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

		aliases[alias] = ast;
	}
}


StoragePtr ExpressionAnalyzer::getTable()
{
	if (const ASTSelectQuery * select = typeid_cast<const ASTSelectQuery *>(ast.get()))
	{
		if (select->table && !typeid_cast<const ASTSelectQuery *>(select->table.get()) && !typeid_cast<const ASTFunction *>(select->table.get()))
		{
			String database = select->database
				? typeid_cast<const ASTIdentifier &>(*select->database).name
				: "";
			const String & table = typeid_cast<const ASTIdentifier &>(*select->table).name;
			return context.tryGetTable(database, table);
		}
	}

	return StoragePtr();
}


void ExpressionAnalyzer::normalizeTree()
{
	SetOfASTs tmp_set;
	MapOfASTs tmp_map;
	normalizeTreeImpl(ast, tmp_map, tmp_set, "");
}


/// finished_asts - уже обработанные вершины (и на что они заменены)
/// current_asts - вершины в текущем стеке вызовов этого метода
/// current_alias - алиас, повешенный на предка ast (самого глубокого из предков с алиасами)
void ExpressionAnalyzer::normalizeTreeImpl(
	ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias)
{
	if (finished_asts.count(ast))
	{
		ast = finished_asts[ast];
		return;
	}

	ASTPtr initial_ast = ast;
	current_asts.insert(initial_ast);

	String my_alias = ast->tryGetAlias();
	if (!my_alias.empty())
		current_alias = my_alias;

	/// rewrite правила, которые действуют при обходе сверху-вниз.
	bool replaced = false;

	ASTFunction * func_node = typeid_cast<ASTFunction *>(ast.get());
	if (func_node)
	{
		/** Нет ли в таблице столбца, название которого полностью совпадает с записью функции?
		  * Например, в таблице есть столбец "domain(URL)", и мы запросили domain(URL).
		  */
		String function_string = func_node->getColumnName();
		NamesAndTypesList::const_iterator it = findColumn(function_string);
		if (columns.end() != it)
		{
			ASTIdentifier * ast_id = new ASTIdentifier(func_node->range, function_string);
			ast = ast_id;
			current_asts.insert(ast);
			replaced = true;
		}

		/// Может быть указано IN t, где t - таблица, что равносильно IN (SELECT * FROM t).
		if (functionIsInOrGlobalInOperator(func_node->name))
			if (ASTIdentifier * right = typeid_cast<ASTIdentifier *>(func_node->arguments->children.at(1).get()))
				right->kind = ASTIdentifier::Table;

		/// А ещё, в качестве исключения, будем понимать count(*) как count(), а не count(список всех столбцов).
		if (func_node->name == "count" && func_node->arguments->children.size() == 1
			&& typeid_cast<const ASTAsterisk *>(func_node->arguments->children[0].get()))
		{
			func_node->arguments->children.clear();
		}
	}
	else if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			/// Если это алиас, но не родительский алиас (чтобы работали конструкции вроде "SELECT column+1 AS column").
			Aliases::const_iterator jt = aliases.find(node->name);
			if (jt != aliases.end() && current_alias != node->name)
			{
				/// Заменим его на соответствующий узел дерева.
				if (current_asts.count(jt->second))
					throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);
				if (!my_alias.empty() && my_alias != jt->second->getAliasOrColumnName())
				{
					/// В конструкции вроде "a AS b", где a - алиас, нужно перевесить алиас b на результат подстановки алиаса a.
					ast = jt->second->clone();
					ast->setAlias(my_alias);
				}
				else
				{
					ast = jt->second;
				}

				replaced = true;
			}
		}
	}
	else if (ASTExpressionList * node = typeid_cast<ASTExpressionList *>(ast.get()))
	{
		/// Заменим * на список столбцов.
		ASTs & asts = node->children;
		for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
		{
			if (ASTAsterisk * asterisk = typeid_cast<ASTAsterisk *>(asts[i].get()))
			{
				ASTs all_columns;
				for (const auto & column_name_type : columns)
					all_columns.emplace_back(new ASTIdentifier(asterisk->range, column_name_type.name));

				asts.erase(asts.begin() + i);
				asts.insert(asts.begin() + i, all_columns.begin(), all_columns.end());
			}
		}
	}
	else if (ASTJoin * node = typeid_cast<ASTJoin *>(ast.get()))
	{
		/// может быть указано JOIN t, где t - таблица, что равносильно JOIN (SELECT * FROM t).
		if (ASTIdentifier * right = typeid_cast<ASTIdentifier *>(node->table.get()))
			right->kind = ASTIdentifier::Table;
	}

	/// Если заменили корень поддерева вызовемся для нового корня снова - на случай, если алиас заменился на алиас.
	if (replaced)
	{
		normalizeTreeImpl(ast, finished_asts, current_asts, current_alias);
		current_asts.erase(initial_ast);
		current_asts.erase(ast);
		finished_asts[initial_ast] = ast;
		return;
	}

	/// Рекурсивные вызовы. Не опускаемся в подзапросы.
	/// Также не опускаемся в левый аргумент лямбда-выражений, чтобы не заменять формальные параметры
	///  по алиасам в выражениях вида 123 AS x, arrayMap(x -> 1, [2]).

	if (func_node && func_node->name == "lambda")
	{
		/// Пропускаем первый аргумент. Также предполагаем, что у функции lambda не может быть parameters.
		for (size_t i = 1, size = func_node->arguments->children.size(); i < size; ++i)
		{
			auto & child = func_node->arguments->children[i];

			if (typeid_cast<const ASTSelectQuery *>(child.get()))
				continue;

			normalizeTreeImpl(child, finished_asts, current_asts, current_alias);
		}
	}
	else
	{
		for (auto & child : ast->children)
		{
			if (typeid_cast<const ASTSelectQuery *>(child.get()))
				continue;

			normalizeTreeImpl(child, finished_asts, current_asts, current_alias);
		}
	}

	/// Если секция WHERE или HAVING состоит из одного алиаса, ссылку нужно заменить не только в children, но и в where_expression и having_expression.
	if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get()))
	{
		if (select->prewhere_expression)
			normalizeTreeImpl(select->prewhere_expression, finished_asts, current_asts, current_alias);
		if (select->where_expression)
			normalizeTreeImpl(select->where_expression, finished_asts, current_asts, current_alias);
		if (select->having_expression)
			normalizeTreeImpl(select->having_expression, finished_asts, current_asts, current_alias);
	}

	/// Действия, выполняемые снизу вверх.

	if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
	{
		if (node->kind == ASTFunction::TABLE_FUNCTION)
		{
		}
		else if (node->name == "lambda")
		{
			node->kind = ASTFunction::LAMBDA_EXPRESSION;
		}
		else if (context.getAggregateFunctionFactory().isAggregateFunctionName(node->name))
		{
			node->kind = ASTFunction::AGGREGATE_FUNCTION;
		}
		else if (node->name == "arrayJoin")
		{
			node->kind = ASTFunction::ARRAY_JOIN;
		}
		else
		{
			node->kind = ASTFunction::FUNCTION;
		}

		if (node->parameters && node->kind != ASTFunction::AGGREGATE_FUNCTION)
			throw Exception("The only parametric functions (functions with two separate parenthesis pairs) are aggregate functions"
				", and '" + node->name + "' is not an aggregate function.", ErrorCodes::FUNCTION_CANNOT_HAVE_PARAMETERS);
	}

	current_asts.erase(initial_ast);
	current_asts.erase(ast);
	finished_asts[initial_ast] = ast;
}


void ExpressionAnalyzer::addAliasColumns()
{
	if (!select_query)
		return;

	if (!storage)
		return;

	columns.insert(std::end(columns), std::begin(storage->alias_columns), std::end(storage->alias_columns));
}


void ExpressionAnalyzer::executeScalarSubqueries()
{
	if (!select_query)
		executeScalarSubqueriesImpl(ast);
	else
	{
		for (auto & child : ast->children)
		{
			/// Не опускаемся в FROM, JOIN, UNION.
			if (child.get() != select_query->table.get()
				&& child.get() != select_query->join.get()
				&& child.get() != select_query->next_union_all.get())
			{
				executeScalarSubqueriesImpl(child);
			}
		}
	}
}


static ASTPtr addTypeConversion(ASTLiteral * ast_, const String & type_name)
{
	auto ast = std::unique_ptr<ASTLiteral>(ast_);
	ASTFunction * func = new ASTFunction(ast->range);
	ASTPtr res = func;
	func->alias = ast->alias;
	ast->alias.clear();
	func->kind = ASTFunction::FUNCTION;
	func->name = "CAST";
	ASTExpressionList * exp_list = new ASTExpressionList(ast->range);
	func->arguments = exp_list;
	func->children.push_back(func->arguments);
	exp_list->children.emplace_back(ast.release());
	exp_list->children.emplace_back(new ASTLiteral{{}, type_name});
	return res;
}


void ExpressionAnalyzer::executeScalarSubqueriesImpl(ASTPtr & ast)
{
	/** Заменяем подзапросы, возвращающие ровно одну строку
	  * ("скалярные" подзапросы) на соответствующие константы.
	  *
	  * Если подзапрос возвращает более одного столбца, то он заменяется на кортеж констант.
	  *
	  * Особенности:
	  *
	  * Замена происходит во время анализа запроса, а не во время основной стадии выполнения.
	  * Это значит, что не будет работать индикатор прогресса во время выполнения этих запросов,
	  *  а также такие запросы нельзя будет прервать.
	  *
	  * Зато результат запросов может быть использован для индекса в таблице.
	  *
	  * Скалярные подзапросы выполняются на сервере-инициаторе запроса.
	  * На удалённые серверы запрос отправляется с уже подставленными константами.
	  */

	if (ASTSubquery * subquery = typeid_cast<ASTSubquery *>(ast.get()))
	{
		Context subquery_context = context;
		Settings subquery_settings = context.getSettings();
		subquery_settings.limits.max_result_rows = 1;
		subquery_settings.extremes = 0;
		subquery_context.setSettings(subquery_settings);

		ASTPtr query = subquery->children.at(0);
		BlockIO res = InterpreterSelectQuery(query, subquery_context, QueryProcessingStage::Complete, subquery_depth + 1).execute();

		Block block;
		try
		{
			block = res.in->read();

			if (!block)
				throw Exception("Scalar subquery returned empty result", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);

			if (block.rows() != 1 || res.in->read())
				throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
		}
		catch (const Exception & e)
		{
			if (e.code() == ErrorCodes::TOO_MUCH_ROWS)
				throw Exception("Scalar subquery returned more than one row", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);
			else
				throw;
		}

		size_t columns = block.columns();
		if (columns == 1)
		{
			ASTLiteral * lit = new ASTLiteral(ast->range, (*block.getByPosition(0).column)[0]);
			lit->alias = subquery->alias;
			ast = addTypeConversion(lit, block.getByPosition(0).type->getName());
		}
		else
		{
			ASTFunction * tuple = new ASTFunction(ast->range);
			tuple->alias = subquery->alias;
			ast = tuple;
			tuple->kind = ASTFunction::FUNCTION;
			tuple->name = "tuple";
			ASTExpressionList * exp_list = new ASTExpressionList(ast->range);
			tuple->arguments = exp_list;
			tuple->children.push_back(tuple->arguments);

			exp_list->children.resize(columns);
			for (size_t i = 0; i < columns; ++i)
			{
				exp_list->children[i] = addTypeConversion(
					new ASTLiteral(ast->range, (*block.getByPosition(i).column)[0]),
					block.getByPosition(i).type->getName());
			}
		}
	}
	else
	{
		/** Не опускаемся в подзапросы в аргументах IN.
		  * Но если аргумент - не подзапрос, то глубже внутри него могут быть подзапросы, и в них надо опускаться.
		  */
		ASTFunction * func = typeid_cast<ASTFunction *>(ast.get());

		if (func && func->kind == ASTFunction::FUNCTION
			&& functionIsInOrGlobalInOperator(func->name))
		{
			for (auto & child : ast->children)
			{
				if (child.get() != func->arguments)
					executeScalarSubqueriesImpl(child);
				else
					for (size_t i = 0, size = func->arguments->children.size(); i < size; ++i)
						if (i != 1 || !typeid_cast<ASTSubquery *>(func->arguments->children[i].get()))
							executeScalarSubqueriesImpl(func->arguments->children[i]);
			}
		}
		else
			for (auto & child : ast->children)
				executeScalarSubqueriesImpl(child);
	}
}


void ExpressionAnalyzer::optimizeGroupBy()
{
	if (!(select_query && select_query->group_expression_list))
		return;

	const auto is_literal = [] (const ASTPtr& ast) {
		return typeid_cast<const ASTLiteral*>(ast.get());
	};

	auto & group_exprs = select_query->group_expression_list->children;

	/// removes expression at index idx by making it last one and calling .pop_back()
	const auto remove_expr_at_index = [&group_exprs] (const size_t idx)
	{
		if (idx < group_exprs.size() - 1)
			std::swap(group_exprs[idx], group_exprs.back());

		group_exprs.pop_back();
	};

	/// iterate over each GROUP BY expression, eliminate injective function calls and literals
	for (size_t i = 0; i < group_exprs.size();)
	{
		if (const auto function = typeid_cast<ASTFunction *>(group_exprs[i].get()))
		{
			/// assert function is injective
			if (possibly_injective_function_names.count(function->name))
			{
				/// do not handle semantic errors here
				if (function->arguments->children.size() < 2)
				{
					++i;
					continue;
				}

				const auto & dict_name = typeid_cast<const ASTLiteral &>(*function->arguments->children[0])
					.value.safeGet<String>();

				const auto & dict_ptr = context.getExternalDictionaries().getDictionary(dict_name);

				const auto & attr_name = typeid_cast<const ASTLiteral &>(*function->arguments->children[1])
					.value.safeGet<String>();

				if (!dict_ptr->isInjective(attr_name))
				{
					++i;
					continue;
				}
			}
			else if (!injective_function_names.count(function->name))
			{
				++i;
				continue;
			}

			/// copy shared pointer to args in order to ensure lifetime
			auto args_ast = function->arguments;

			/** remove function call and take a step back to ensure
			  * next iteration does not skip not yet processed data
			  */
			remove_expr_at_index(i);

			/// copy non-literal arguments
			std::remove_copy_if(
				std::begin(args_ast->children), std::end(args_ast->children),
				std::back_inserter(group_exprs), is_literal
			);
		}
		else if (is_literal(group_exprs[i]))
		{
			remove_expr_at_index(i);
		}
		else
		{
			/// if neither a function nor literal - advance to next expression
			++i;
		}
	}

	if (group_exprs.empty())
	{
		/** Нельзя полностью убирать GROUP BY. Потому что если при этом даже агрегатных функций не было, то получится, что не будет агрегации.
		  * Вместо этого оставим GROUP BY const.
		  * Далее см. удаление констант в методе analyzeAggregation.
		  */

		/// Нужно вставить константу, которая не является именем столбца таблицы. Такой случай редкий, но бывает.
		UInt64 unused_column = 0;
		String unused_column_name = toString(unused_column);

		while (columns.end() != std::find_if(columns.begin(), columns.end(),
			[&unused_column_name](const NameAndTypePair & name_type) { return name_type.name == unused_column_name; }))
		{
			++unused_column;
			unused_column_name = toString(unused_column);
		}

		select_query->group_expression_list = new ASTExpressionList;
		select_query->group_expression_list->children.push_back(new ASTLiteral(StringRange(), UInt64(unused_column)));
	}
}


void ExpressionAnalyzer::optimizeOrderBy()
{
	if (!(select_query && select_query->order_expression_list))
		return;

	/// Уникализируем условия сортировки.
	using NameAndLocale = std::pair<std::string, std::string>;
	std::set<NameAndLocale> elems_set;

	ASTs & elems = select_query->order_expression_list->children;
	ASTs unique_elems;
	unique_elems.reserve(elems.size());

	for (const auto & elem : elems)
	{
		String name = elem->children.front()->getColumnName();
		const ASTOrderByElement & order_by_elem = typeid_cast<const ASTOrderByElement &>(*elem);

		if (elems_set.emplace(
			std::piecewise_construct,
			std::forward_as_tuple(name),
			std::forward_as_tuple(order_by_elem.collator ? order_by_elem.collator->getLocale() : std::string())).second)
		{
			unique_elems.emplace_back(elem);
		}
	}

	if (unique_elems.size() < elems.size())
		elems = unique_elems;
}


void ExpressionAnalyzer::makeSetsForIndex()
{
	if (storage && ast && storage->supportsIndexForIn())
		makeSetsForIndexImpl(ast, storage->getSampleBlock());
}

void ExpressionAnalyzer::makeSetsForIndexImpl(ASTPtr & node, const Block & sample_block)
{
	for (auto & child : node->children)
		makeSetsForIndexImpl(child, sample_block);

	ASTFunction * func = typeid_cast<ASTFunction *>(node.get());
	if (func && func->kind == ASTFunction::FUNCTION && functionIsInOperator(func->name))
	{
		IAST & args = *func->arguments;
		ASTPtr & arg = args.children.at(1);

		if (!typeid_cast<ASTSet *>(arg.get()) && !typeid_cast<ASTSubquery *>(arg.get()) && !typeid_cast<ASTIdentifier *>(arg.get()))
		{
			try
			{
				makeExplicitSet(func, sample_block, true);
			}
			catch (const DB::Exception & e)
			{
				/// в sample_block нет колонок, которые добаляет getActions
				if (e.code() != ErrorCodes::NOT_FOUND_COLUMN_IN_BLOCK)
					throw;
			}
		}
	}
}


static SharedPtr<InterpreterSelectQuery> interpretSubquery(
	ASTPtr & subquery_or_table_name, const Context & context, size_t subquery_depth, const Names & required_columns)
{
	/// Подзапрос или имя таблицы. Имя таблицы аналогично подзапросу SELECT * FROM t.
	const ASTSubquery * subquery = typeid_cast<const ASTSubquery *>(subquery_or_table_name.get());
	const ASTIdentifier * table = typeid_cast<const ASTIdentifier *>(subquery_or_table_name.get());

	if (!subquery && !table)
		throw Exception("IN/JOIN supports only SELECT subqueries.", ErrorCodes::BAD_ARGUMENTS);

	/** Для подзапроса в секции IN/JOIN не действуют ограничения на максимальный размер результата.
	  * Так как результат этого поздапроса - ещё не результат всего запроса.
	  * Вместо этого работают ограничения
	  *  max_rows_in_set, max_bytes_in_set, set_overflow_mode,
	  *  max_rows_in_join, max_bytes_in_join, join_overflow_mode,
	  *  которые проверяются отдельно (в объектах Set, Join).
	  */
	Context subquery_context = context;
	Settings subquery_settings = context.getSettings();
	subquery_settings.limits.max_result_rows = 0;
	subquery_settings.limits.max_result_bytes = 0;
	/// Вычисление extremes не имеет смысла и не нужно (если его делать, то в результате всего запроса могут взяться extremes подзапроса).
	subquery_settings.extremes = 0;
	subquery_context.setSettings(subquery_settings);

	ASTPtr query;
	if (table)
	{
		/// create ASTSelectQuery for "SELECT * FROM table" as if written by hand
		const auto select_query = new ASTSelectQuery;
		query = select_query;

		const auto select_expression_list = new ASTExpressionList;
		select_query->select_expression_list = select_expression_list;
		select_query->children.emplace_back(select_query->select_expression_list);

		/// get columns list for target table
		const auto & storage = context.getTable("", table->name);
		const auto & columns = storage->getColumnsListNonMaterialized();
		select_expression_list->children.reserve(columns.size());

		/// manually substitute column names in place of asterisk
		for (const auto & column : columns)
			select_expression_list->children.emplace_back(new ASTIdentifier{
				StringRange{}, column.name
			});

		select_query->table = subquery_or_table_name;
		select_query->children.emplace_back(select_query->table);
	}
	else
	{
		query = subquery->children.at(0);

		/** В подзапросе могут быть указаны столбцы с одинаковыми именами. Например, SELECT x, x FROM t
		  * Это плохо, потому что результат такого запроса нельзя сохранить в таблицу, потому что в таблице не может быть одноимённых столбцов.
		  * Сохранение в таблицу требуется для GLOBAL-подзапросов.
		  *
		  * Чтобы избежать такой ситуации, будем переименовывать одинаковые столбцы.
		  */

		std::set<std::string> all_column_names;
		std::set<std::string> assigned_column_names;

		if (ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(query.get()))
		{
			for (auto & expr : select->select_expression_list->children)
				all_column_names.insert(expr->getAliasOrColumnName());

			for (auto & expr : select->select_expression_list->children)
			{
				auto name = expr->getAliasOrColumnName();

				if (!assigned_column_names.insert(name).second)
				{
					size_t i = 1;
					while (all_column_names.end() != all_column_names.find(name + "_" + toString(i)))
						++i;

					name = name + "_" + toString(i);
					expr = expr->clone();	/// Отменяет склейку одинаковых выражений в дереве.
					expr->setAlias(name);

					all_column_names.insert(name);
					assigned_column_names.insert(name);
				}
			}
		}
	}

	if (required_columns.empty())
		return new InterpreterSelectQuery(query, subquery_context, QueryProcessingStage::Complete, subquery_depth + 1);
	else
		return new InterpreterSelectQuery(query, subquery_context, required_columns, QueryProcessingStage::Complete, subquery_depth + 1);
}


void ExpressionAnalyzer::makeSet(ASTFunction * node, const Block & sample_block)
{
	/** Нужно преобразовать правый аргумент в множество.
	  * Это может быть имя таблицы, значение, перечисление значений или подзапрос.
	  * Перечисление значений парсится как функция tuple.
	  */
	IAST & args = *node->arguments;
	ASTPtr & arg = args.children.at(1);

	/// Уже преобразовали.
	if (typeid_cast<ASTSet *>(arg.get()))
		return;

	/// Если подзапрос или имя таблицы для SELECT.
	ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(arg.get());
	if (typeid_cast<ASTSubquery *>(arg.get()) || identifier)
	{
		/// Получаем поток блоков для подзапроса. Создаём Set и кладём на место подзапроса.
		String set_id = arg->getColumnName();
		ASTSet * ast_set = new ASTSet(set_id);
		ASTPtr ast_set_ptr = ast_set;

		/// Особый случай - если справа оператора IN указано имя таблицы, при чём, таблица имеет тип Set (заранее подготовленное множество).
		/// TODO В этом синтаксисе не поддерживается указание имени БД.
		if (identifier)
		{
			StoragePtr table = context.tryGetTable("", identifier->name);

			if (table)
			{
				StorageSet * storage_set = typeid_cast<StorageSet *>(table.get());

				if (storage_set)
				{
					SetPtr & set = storage_set->getSet();
					ast_set->set = set;
					arg = ast_set_ptr;
					return;
				}
			}
		}

		SubqueryForSet & subquery_for_set = subqueries_for_sets[set_id];

		/// Если уже создали Set с таким же подзапросом/таблицей.
		if (subquery_for_set.set)
		{
			ast_set->set = subquery_for_set.set;
			arg = ast_set_ptr;
			return;
		}

		ast_set->set = std::make_shared<Set>(settings.limits);

		/** Для GLOBAL IN-ов происходит следующее:
		  * - в функции addExternalStorage подзапрос IN (SELECT ...) заменяется на IN _data1,
		  *   в объекте subquery_for_set выставляется этот подзапрос в качестве source и временная таблица _data1 в качестве table.
		  * - в этой функции видно выражение IN _data1.
		  */
		if (!subquery_for_set.source)
		{
			auto interpreter = interpretSubquery(arg, context, subquery_depth, {});
			subquery_for_set.source = new LazyBlockInputStream([interpreter]() mutable { return interpreter->execute().in; });
			subquery_for_set.source_sample = interpreter->getSampleBlock();

			/** Зачем используется LazyBlockInputStream?
			  *
			  * Дело в том, что при обработке запроса вида
			  *  SELECT ... FROM remote_test WHERE column GLOBAL IN (subquery),
			  *  если распределённая таблица remote_test содержит в качестве одного из серверов localhost,
			  *  то запрос будет ещё раз интерпретирован локально (а не отправлен по TCP, как в случае удалённого сервера).
			  *
			  * Конвейер выполнения запроса будет такой:
			  * CreatingSets
			  *  выполнение подзапроса subquery, заполнение временной таблицы _data1 (1)
			  *  CreatingSets
			  *   чтение из таблицы _data1, создание множества (2)
			  *   чтение из таблицы, подчинённой remote_test.
			  *
			  * (Вторая часть конвейера под CreatingSets - это повторная интерпретация запроса внутри StorageDistributed,
			  *  запрос отличается тем, что имя БД и таблицы заменены на подчинённые, а также подзапрос заменён на _data1.)
			  *
			  * Но при создании конвейера, при создании источника (2), будет обнаружено, что таблица _data1 пустая
			  *  (потому что запрос ещё не начал выполняться), и будет возвращён в качестве источника пустой источник.
			  * И затем, при выполнении запроса, на шаге (2), будет создано пустое множество.
			  *
			  * Поэтому, мы делаем инициализацию шага (2) ленивой
			  *  - чтобы она произошла только после выполнения шага (1), на котором нужная таблица будет заполнена.
			  *
			  * Замечание: это решение не очень хорошее, надо подумать лучше.
			  */
		}

		subquery_for_set.set = ast_set->set;
		arg = ast_set_ptr;
	}
	else
	{
		/// Явное перечисление значений в скобках.
		makeExplicitSet(node, sample_block, false);
	}
}

/// Случай явного перечисления значений.
void ExpressionAnalyzer::makeExplicitSet(ASTFunction * node, const Block & sample_block, bool create_ordered_set)
{
	IAST & args = *node->arguments;

	if (args.children.size() != 2)
		throw Exception("Wrong number of arguments passed to function in", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

	ASTPtr & arg = args.children.at(1);

	DataTypes set_element_types;
	ASTPtr & left_arg = args.children.at(0);

	ASTFunction * left_arg_tuple = typeid_cast<ASTFunction *>(left_arg.get());

	if (left_arg_tuple && left_arg_tuple->name == "tuple")
	{
		for (const auto & arg : left_arg_tuple->arguments->children)
		{
			const auto & data_type = sample_block.getByName(arg->getColumnName()).type;

			/// @note prevent crash in query: SELECT (1, [1]) in (1, 1)
			if (const auto array = typeid_cast<const DataTypeArray * >(data_type.get()))
				throw Exception("Incorrect element of tuple: " + array->getName(), ErrorCodes::INCORRECT_ELEMENT_OF_SET);

			set_element_types.push_back(data_type);
		}
	}
	else
	{
		DataTypePtr left_type = sample_block.getByName(left_arg->getColumnName()).type;
		if (DataTypeArray * array_type = typeid_cast<DataTypeArray *>(left_type.get()))
			set_element_types.push_back(array_type->getNestedType());
		else
			set_element_types.push_back(left_type);
	}

	/// Отличим случай x in (1, 2) от случая x in 1 (он же x in (1)).
	bool single_value = false;
	ASTPtr elements_ast = arg;

	if (ASTFunction * set_func = typeid_cast<ASTFunction *>(arg.get()))
	{
		if (set_func->name == "tuple")
		{
			if (set_func->arguments->children.empty())
			{
				/// Пустое множество.
				elements_ast = set_func->arguments;
			}
			else
			{
				/// Отличим случай (x, y) in ((1, 2), (3, 4)) от случая (x, y) in (1, 2).
				ASTFunction * any_element = typeid_cast<ASTFunction *>(set_func->arguments->children.at(0).get());
				if (set_element_types.size() >= 2 && (!any_element || any_element->name != "tuple"))
					single_value = true;
				else
					elements_ast = set_func->arguments;
			}
		}
		else
		{
			if (set_element_types.size() >= 2)
				throw Exception("Incorrect type of 2nd argument for function " + node->name
					+ ". Must be subquery or set of " + toString(set_element_types.size()) + "-element tuples.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

			single_value = true;
		}
	}
	else if (typeid_cast<ASTLiteral *>(arg.get()))
	{
		single_value = true;
	}
	else
	{
		throw Exception("Incorrect type of 2nd argument for function " + node->name + ". Must be subquery or set of values.",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
	}

	if (single_value)
	{
		ASTPtr exp_list = new ASTExpressionList;
		exp_list->children.push_back(elements_ast);
		elements_ast = exp_list;
	}

	ASTSet * ast_set = new ASTSet(arg->getColumnName());
	ASTPtr ast_set_ptr = ast_set;
	ast_set->set = std::make_shared<Set>(settings.limits);
	ast_set->is_explicit = true;
	ast_set->set->createFromAST(set_element_types, elements_ast, context, create_ordered_set);
	arg = ast_set_ptr;
}


static String getUniqueName(const Block & block, const String & prefix)
{
	int i = 1;
	while (block.has(prefix + toString(i)))
		++i;
	return prefix + toString(i);
}


/** Для getActionsImpl.
  * Стек из ExpressionActions, соответствующих вложенным лямбда-выражениям.
  * Новое действие нужно добавлять на самый высокий возможный уровень.
  * Например, в выражении "select arrayMap(x -> x + column1 * column2, array1)"
  *  вычисление произведения нужно делать вне лямбда-выражения (оно не зависит от x), а вычисление суммы - внутри (зависит от x).
  */
struct ExpressionAnalyzer::ScopeStack
{
	struct Level
	{
		ExpressionActionsPtr actions;
		NameSet new_columns;
	};

	typedef std::vector<Level> Levels;

	Levels stack;
	Settings settings;

	ScopeStack(const ExpressionActionsPtr & actions, const Settings & settings_)
		: settings(settings_)
	{
		stack.emplace_back();
		stack.back().actions = actions;

		const Block & sample_block = actions->getSampleBlock();
		for (size_t i = 0, size = sample_block.columns(); i < size; ++i)
			stack.back().new_columns.insert(sample_block.unsafeGetByPosition(i).name);
	}

	void pushLevel(const NamesAndTypesList & input_columns)
	{
		stack.emplace_back();
		Level & prev = stack[stack.size() - 2];

		ColumnsWithTypeAndName all_columns;
		NameSet new_names;

		for (NamesAndTypesList::const_iterator it = input_columns.begin(); it != input_columns.end(); ++it)
		{
			all_columns.emplace_back(nullptr, it->type, it->name);
			new_names.insert(it->name);
			stack.back().new_columns.insert(it->name);
		}

		const Block & prev_sample_block = prev.actions->getSampleBlock();
		for (size_t i = 0, size = prev_sample_block.columns(); i < size; ++i)
		{
			const ColumnWithTypeAndName & col = prev_sample_block.unsafeGetByPosition(i);
			if (!new_names.count(col.name))
				all_columns.push_back(col);
		}

		stack.back().actions = std::make_shared<ExpressionActions>(all_columns, settings);
	}

	size_t getColumnLevel(const std::string & name)
	{
		for (int i = static_cast<int>(stack.size()) - 1; i >= 0; --i)
			if (stack[i].new_columns.count(name))
				return i;

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
			const ColumnWithTypeAndName & col = stack[level].actions->getSampleBlock().getByName(added[i]);
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

	const Block & getSampleBlock() const
	{
		return stack.back().actions->getSampleBlock();
	}
};


void ExpressionAnalyzer::getRootActions(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActionsPtr & actions)
{
	ScopeStack scopes(actions, settings);
	getActionsImpl(ast, no_subqueries, only_consts, scopes);
	actions = scopes.popLevel();
}


void ExpressionAnalyzer::getArrayJoinedColumns()
{
	if (select_query && select_query->array_join_expression_list)
	{
		ASTs & array_join_asts = select_query->array_join_expression_list->children;
		for (const auto & ast : array_join_asts)
		{
			const String nested_table_name = ast->getColumnName();
			const String nested_table_alias = ast->getAliasOrColumnName();

			if (nested_table_alias == nested_table_name && !typeid_cast<const ASTIdentifier *>(ast.get()))
				throw Exception("No alias for non-trivial value in ARRAY JOIN: " + nested_table_name, ErrorCodes::ALIAS_REQUIRED);

			if (array_join_alias_to_name.count(nested_table_alias) || aliases.count(nested_table_alias))
				throw Exception("Duplicate alias " + nested_table_alias, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);

			array_join_alias_to_name[nested_table_alias] = nested_table_name;
			array_join_name_to_alias[nested_table_name] = nested_table_alias;
		}

		ASTs & query_asts = select_query->children;
		for (const auto & ast : query_asts)
		{
			/// Не опускаемся в подзапросы и UNION.
			if (typeid_cast<const ASTSelectQuery *>(ast.get()))
				continue;

			if (ast != select_query->array_join_expression_list)
				getArrayJoinedColumnsImpl(ast);
		}

		/// Если результат ARRAY JOIN не используется, придется все равно по-ARRAY-JOIN-ить какой-нибудь столбец,
		/// чтобы получить правильное количество строк.
		if (array_join_result_to_source.empty())
		{
			ASTPtr expr = select_query->array_join_expression_list->children.at(0);
			String source_name = expr->getColumnName();
			String result_name = expr->getAliasOrColumnName();

			/// Это массив.
			if (!typeid_cast<ASTIdentifier *>(expr.get()) || findColumn(source_name, columns) != columns.end())
			{
				array_join_result_to_source[result_name] = source_name;
			}
			else /// Это вложенная таблица.
			{
				bool found = false;
				for (const auto & column_name_type : columns)
				{
					String table_name = DataTypeNested::extractNestedTableName(column_name_type.name);
					String column_name = DataTypeNested::extractNestedColumnName(column_name_type.name);
					if (table_name == source_name)
					{
						array_join_result_to_source[DataTypeNested::concatenateNestedName(result_name, column_name)] = column_name_type.name;
						found = true;
						break;
					}
				}
				if (!found)
					throw Exception("No columns in nested table " + source_name, ErrorCodes::EMPTY_NESTED_TABLE);
			}
		}
	}
}


/// Заполняет array_join_result_to_source: по каким столбцам-массивам размножить, и как их после этого назвать.
void ExpressionAnalyzer::getArrayJoinedColumnsImpl(ASTPtr ast)
{
	if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			String table_name = DataTypeNested::extractNestedTableName(node->name);

			if (array_join_alias_to_name.count(node->name))
			{
				/// Был написан ARRAY JOIN со столбцом-массивом. Пример: SELECT K1 FROM ... ARRAY JOIN ParsedParams.Key1 AS K1
				array_join_result_to_source[node->name] = array_join_alias_to_name[node->name];	/// K1 -> ParsedParams.Key1
			}
			else if (array_join_alias_to_name.count(table_name))
			{
				/// Был написан ARRAY JOIN с вложенной таблицей. Пример: SELECT PP.Key1 FROM ... ARRAY JOIN ParsedParams AS PP
				String nested_column = DataTypeNested::extractNestedColumnName(node->name);	/// Key1
				array_join_result_to_source[node->name]	/// PP.Key1 -> ParsedParams.Key1
					= DataTypeNested::concatenateNestedName(array_join_alias_to_name[table_name], nested_column);
			}
			else if (array_join_name_to_alias.count(table_name))
			{
				/** Пример: SELECT ParsedParams.Key1 FROM ... ARRAY JOIN ParsedParams AS PP.
				  * То есть, в запросе используется исходный массив, размноженный по самому себе.
				  */

				String nested_column = DataTypeNested::extractNestedColumnName(node->name);	/// Key1
				array_join_result_to_source[	/// PP.Key1 -> ParsedParams.Key1
					DataTypeNested::concatenateNestedName(array_join_name_to_alias[table_name], nested_column)] = node->name;
			}
		}
	}
	else
	{
		for (auto & child : ast->children)
			if (!typeid_cast<const ASTSelectQuery *>(child.get()))
				getArrayJoinedColumnsImpl(child);
	}
}


void ExpressionAnalyzer::getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack)
{
	/// Если результат вычисления уже есть в блоке.
	if ((typeid_cast<ASTFunction *>(ast.get()) || typeid_cast<ASTLiteral *>(ast.get()))
		&& actions_stack.getSampleBlock().has(ast->getColumnName()))
		return;

	if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
	{
		std::string name = node->getColumnName();
		if (!only_consts && !actions_stack.getSampleBlock().has(name))
		{
			/// Запрошенного столбца нет в блоке.
			/// Если такой столбец есть в таблице, значит пользователь наверно забыл окружить его агрегатной функцией или добавить в GROUP BY.

			bool found = false;
			for (const auto & column_name_type : columns)
				if (column_name_type.name == name)
					found = true;

			if (found)
				throw Exception("Column " + name + " is not under aggregate function and not in GROUP BY.",
					ErrorCodes::NOT_AN_AGGREGATE);
		}
	}
	else if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
	{
		if (node->kind == ASTFunction::LAMBDA_EXPRESSION)
			throw Exception("Unexpected lambda expression", ErrorCodes::UNEXPECTED_EXPRESSION);

		/// Функция arrayJoin.
		if (node->kind == ASTFunction::ARRAY_JOIN)
		{
			if (node->arguments->children.size() != 1)
				throw Exception("arrayJoin requires exactly 1 argument", ErrorCodes::TYPE_MISMATCH);

			ASTPtr arg = node->arguments->children.at(0);
			getActionsImpl(arg, no_subqueries, only_consts, actions_stack);
			if (!only_consts)
			{
				String result_name = node->getColumnName();
				actions_stack.addAction(ExpressionAction::copyColumn(arg->getColumnName(), result_name));
				NameSet joined_columns;
				joined_columns.insert(result_name);
				actions_stack.addAction(ExpressionAction::arrayJoin(joined_columns, false));
			}

			return;
		}

		if (node->kind == ASTFunction::FUNCTION)
		{
			if (functionIsInOrGlobalInOperator(node->name))
			{
				if (!no_subqueries)
				{
					/// Найдем тип первого аргумента (потом getActionsImpl вызовется для него снова и ни на что не повлияет).
					getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);

					/// Превратим tuple или подзапрос в множество.
					makeSet(node, actions_stack.getSampleBlock());
				}
				else
				{
					if (!only_consts)
					{
						/// Мы в той части дерева, которую не собираемся вычислять. Нужно только определить типы.
						/// Не будем выполнять подзапросы и составлять множества. Вставим произвольный столбец правильного типа.
						ColumnWithTypeAndName fake_column;
						fake_column.name = node->getColumnName();
						fake_column.type = new DataTypeUInt8;
						actions_stack.addAction(ExpressionAction::addColumn(fake_column));
						getActionsImpl(node->arguments->children.at(0), no_subqueries, only_consts, actions_stack);
					}
					return;
				}
			}

			/// Особая функция indexHint. Всё, что внутри неё не вычисляется
			/// (а используется только для анализа индекса, см. PKCondition).
			if (node->name == "indexHint")
			{
				actions_stack.addAction(ExpressionAction::addColumn(ColumnWithTypeAndName(
					new ColumnConstUInt8(1, 1), new DataTypeUInt8, node->getColumnName())));
				return;
			}

			const FunctionPtr & function = FunctionFactory::instance().get(node->name, context);

			Names argument_names;
			DataTypes argument_types;
			bool arguments_present = true;

			/// Если у функции есть аргумент-лямбда-выражение, нужно определить его тип до рекурсивного вызова.
			bool has_lambda_arguments = false;

			for (auto & child : node->arguments->children)
			{
				ASTFunction * lambda = typeid_cast<ASTFunction *>(child.get());
				ASTSet * set = typeid_cast<ASTSet *>(child.get());
				if (lambda && lambda->name == "lambda")
				{
					/// Если аргумент - лямбда-выражение, только запомним его примерный тип.
					if (lambda->arguments->children.size() != 2)
						throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

					ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(lambda->arguments->children.at(0).get());

					if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
						throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

					has_lambda_arguments = true;
					argument_types.emplace_back(new DataTypeExpression(DataTypes(lambda_args_tuple->arguments->children.size())));
					/// Выберем название в следующем цикле.
					argument_names.emplace_back();
				}
				else if (set)
				{
					ColumnWithTypeAndName column;
					column.type = new DataTypeSet;

					/// Если аргумент - множество, заданное перечислением значений, дадим ему уникальное имя,
					///  чтобы множества с одинаковой записью не склеивались (у них может быть разный тип).
					if (set->is_explicit)
						column.name = getUniqueName(actions_stack.getSampleBlock(), "__set");
					else
						column.name = set->getColumnName();

					if (!actions_stack.getSampleBlock().has(column.name))
					{
						column.column = new ColumnSet(1, set->set);

						actions_stack.addAction(ExpressionAction::addColumn(column));
					}

					argument_types.push_back(column.type);
					argument_names.push_back(column.name);
				}
				else
				{
					/// Если аргумент не лямбда-выражение, вызовемся рекурсивно и узнаем его тип.
					getActionsImpl(child, no_subqueries, only_consts, actions_stack);
					std::string name = child->getColumnName();
					if (actions_stack.getSampleBlock().has(name))
					{
						argument_types.push_back(actions_stack.getSampleBlock().getByName(name).type);
						argument_names.push_back(name);
					}
					else
					{
						if (only_consts)
						{
							arguments_present = false;
						}
						else
						{
							throw Exception("Unknown identifier: " + name, ErrorCodes::UNKNOWN_IDENTIFIER);
						}
					}
				}
			}

			if (only_consts && !arguments_present)
				return;

			Names additional_requirements;

			if (has_lambda_arguments && !only_consts)
			{
				function->getLambdaArgumentTypes(argument_types);

				/// Вызовемся рекурсивно для лямбда-выражений.
				for (size_t i = 0; i < node->arguments->children.size(); ++i)
				{
					ASTPtr child = node->arguments->children[i];

					ASTFunction * lambda = typeid_cast<ASTFunction *>(child.get());
					if (lambda && lambda->name == "lambda")
					{
						DataTypeExpression * lambda_type = typeid_cast<DataTypeExpression *>(argument_types[i].get());
						ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(lambda->arguments->children.at(0).get());
						ASTs lambda_arg_asts = lambda_args_tuple->arguments->children;
						NamesAndTypesList lambda_arguments;

						for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
						{
							ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(lambda_arg_asts[j].get());
							if (!identifier)
								throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

							String arg_name = identifier->name;

							lambda_arguments.emplace_back(arg_name, lambda_type->getArgumentTypes()[j]);
						}

						actions_stack.pushLevel(lambda_arguments);
						getActionsImpl(lambda->arguments->children.at(1), no_subqueries, only_consts, actions_stack);
						ExpressionActionsPtr lambda_actions = actions_stack.popLevel();

						String result_name = lambda->arguments->children.at(1)->getColumnName();
						lambda_actions->finalize(Names(1, result_name));
						DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;
						argument_types[i] = new DataTypeExpression(lambda_type->getArgumentTypes(), result_type);

						Names captured = lambda_actions->getRequiredColumns();
						for (size_t j = 0; j < captured.size(); ++j)
							if (findColumn(captured[j], lambda_arguments) == lambda_arguments.end())
								additional_requirements.push_back(captured[j]);

						/// Не можем дать название getColumnName(),
						///  потому что оно не однозначно определяет выражение (типы аргументов могут быть разными).
						argument_names[i] = getUniqueName(actions_stack.getSampleBlock(), "__lambda");

						ColumnWithTypeAndName lambda_column;
						lambda_column.column = new ColumnExpression(1, lambda_actions, lambda_arguments, result_type, result_name);
						lambda_column.type = argument_types[i];
						lambda_column.name = argument_names[i];
						actions_stack.addAction(ExpressionAction::addColumn(lambda_column));
					}
				}
			}

			if (only_consts)
			{
				for (size_t i = 0; i < argument_names.size(); ++i)
				{
					if (!actions_stack.getSampleBlock().has(argument_names[i]))
					{
						arguments_present = false;
						break;
					}
				}
			}

			if (arguments_present)
				actions_stack.addAction(ExpressionAction::applyFunction(function, argument_names, node->getColumnName()),
										additional_requirements);
		}
	}
	else if (ASTLiteral * node = typeid_cast<ASTLiteral *>(ast.get()))
	{
		DataTypePtr type = apply_visitor(FieldToDataType(), node->value);

		ColumnWithTypeAndName column;
		column.column = type->createConstColumn(1, node->value);
		column.type = type;
		column.name = node->getColumnName();

		actions_stack.addAction(ExpressionAction::addColumn(column));
	}
	else
	{
		for (auto & child : ast->children)
			getActionsImpl(child, no_subqueries, only_consts, actions_stack);
	}
}


void ExpressionAnalyzer::getAggregates(const ASTPtr & ast, ExpressionActionsPtr & actions)
{
	/// Внутри WHERE и PREWHERE не может быть агрегатных функций.
	if (select_query && (ast.get() == select_query->where_expression.get() || ast.get() == select_query->prewhere_expression.get()))
	{
		assertNoAggregates(ast, "in WHERE or PREWHERE");
		return;
	}

	/// Если мы анализируем не запрос SELECT, а отдельное выражение, то в нём не может быть агрегатных функций.
	if (!select_query)
	{
		assertNoAggregates(ast, "in wrong place");
		return;
	}

	const ASTFunction * node = typeid_cast<const ASTFunction *>(ast.get());
	if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
	{
		has_aggregation = true;
		AggregateDescription aggregate;
		aggregate.column_name = node->getColumnName();

		/// Агрегатные функции уникализируются.
		for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
			if (aggregate_descriptions[i].column_name == aggregate.column_name)
				return;

		const ASTs & arguments = node->arguments->children;
		aggregate.argument_names.resize(arguments.size());
		DataTypes types(arguments.size());

		for (size_t i = 0; i < arguments.size(); ++i)
		{
			/// Внутри агрегатных функций не может быть других агрегатных функций.
			assertNoAggregates(arguments[i], "inside another aggregate function");

			getRootActions(arguments[i], true, false, actions);
			const std::string & name = arguments[i]->getColumnName();
			types[i] = actions->getSampleBlock().getByName(name).type;
			aggregate.argument_names[i] = name;
		}

		aggregate.function = context.getAggregateFunctionFactory().get(node->name, types);

		if (node->parameters)
		{
			const ASTs & parameters = typeid_cast<const ASTExpressionList &>(*node->parameters).children;
			Array params_row(parameters.size());

			for (size_t i = 0; i < parameters.size(); ++i)
			{
				const ASTLiteral * lit = typeid_cast<const ASTLiteral *>(parameters[i].get());
				if (!lit)
					throw Exception("Parameters to aggregate functions must be literals",
						ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);

				params_row[i] = lit->value;
			}

			aggregate.parameters = params_row;
			aggregate.function->setParameters(params_row);
		}

		aggregate.function->setArguments(types);

		aggregate_descriptions.push_back(aggregate);
	}
	else
	{
		for (const auto & child : ast->children)
			if (!typeid_cast<const ASTSubquery *>(child.get()) && !typeid_cast<const ASTSelectQuery *>(child.get()))
				getAggregates(child, actions);
	}
}


void ExpressionAnalyzer::assertNoAggregates(const ASTPtr & ast, const char * description)
{
	const ASTFunction * node = typeid_cast<const ASTFunction *>(ast.get());

	if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
		throw Exception("Aggregate function " + node->getColumnName()
			+ " is found " + String(description) + " in query", ErrorCodes::ILLEGAL_AGGREGATION);

	for (const auto & child : ast->children)
		if (!typeid_cast<const ASTSubquery *>(child.get()) && !typeid_cast<const ASTSelectQuery *>(child.get()))
			assertNoAggregates(child, description);
}


void ExpressionAnalyzer::assertSelect() const
{
	if (!select_query)
		throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::assertAggregation() const
{
	if (!has_aggregation)
		throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::initChain(ExpressionActionsChain & chain, const NamesAndTypesList & columns) const
{
	if (chain.steps.empty())
	{
		chain.settings = settings;
		chain.steps.emplace_back(std::make_shared<ExpressionActions>(columns, settings));
	}
}

/// "Большой" ARRAY JOIN.
void ExpressionAnalyzer::addMultipleArrayJoinAction(ExpressionActionsPtr & actions) const
{
	NameSet result_columns;
	for (const auto & result_source : array_join_result_to_source)
	{
		/// Дать столбцам новые имена, если надо.
		if (result_source.first != result_source.second)
			actions->add(ExpressionAction::copyColumn(result_source.second, result_source.first));

		/// Сделать ARRAY JOIN (заменить массивы на их внутренности) для столбцов в этими новыми именами.
		result_columns.insert(result_source.first);
	}

	actions->add(ExpressionAction::arrayJoin(result_columns, select_query->array_join_is_left));
}

bool ExpressionAnalyzer::appendArrayJoin(ExpressionActionsChain & chain, bool only_types)
{
	assertSelect();

	if (!select_query->array_join_expression_list)
		return false;

	initChain(chain, columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	getRootActions(select_query->array_join_expression_list, only_types, false, step.actions);

	addMultipleArrayJoinAction(step.actions);

	return true;
}

void ExpressionAnalyzer::addJoinAction(ExpressionActionsPtr & actions, bool only_types) const
{
	if (only_types)
		actions->add(ExpressionAction::ordinaryJoin(nullptr, columns_added_by_join));
	else
		for (auto & subquery_for_set : subqueries_for_sets)
			if (subquery_for_set.second.join)
				actions->add(ExpressionAction::ordinaryJoin(subquery_for_set.second.join, columns_added_by_join));
}

bool ExpressionAnalyzer::appendJoin(ExpressionActionsChain & chain, bool only_types)
{
	assertSelect();

	if (!select_query->join)
		return false;

	initChain(chain, columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	ASTJoin & ast_join = typeid_cast<ASTJoin &>(*select_query->join);
	if (ast_join.using_expr_list)
		getRootActions(ast_join.using_expr_list, only_types, false, step.actions);

	/// Не поддерживается два JOIN-а с одинаковым подзапросом, но разными USING-ами.
	String join_id = ast_join.table->getColumnName();

	SubqueryForSet & subquery_for_set = subqueries_for_sets[join_id];

	/// Особый случай - если справа JOIN указано имя таблицы, при чём, таблица имеет тип Join (заранее подготовленное отображение).
	/// TODO В этом синтаксисе не поддерживается указание имени БД.
	ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(ast_join.table.get());
	if (identifier)
	{
		StoragePtr table = context.tryGetTable("", identifier->name);

		if (table)
		{
			StorageJoin * storage_join = typeid_cast<StorageJoin *>(table.get());

			if (storage_join)
			{
				storage_join->assertCompatible(ast_join.kind, ast_join.strictness);
				/// TODO Проверять набор ключей.

				JoinPtr & join = storage_join->getJoin();
				subquery_for_set.join = join;
			}
		}
	}

	if (!subquery_for_set.join)
	{
		JoinPtr join = std::make_shared<Join>(join_key_names_left, join_key_names_right, settings.limits, ast_join.kind, ast_join.strictness);

		Names required_joined_columns(join_key_names_right.begin(), join_key_names_right.end());
		for (const auto & name_type : columns_added_by_join)
			required_joined_columns.push_back(name_type.name);

		/** Для GLOBAL JOIN-ов (в случае, например, push-метода выполнения GLOBAL подзапросов) происходит следующее:
		  * - в функции addExternalStorage подзапрос JOIN (SELECT ...) заменяется на JOIN _data1,
		  *   в объекте subquery_for_set выставляется этот подзапрос в качестве source и временная таблица _data1 в качестве table.
		  * - в этой функции видно выражение JOIN _data1.
		  */
		if (!subquery_for_set.source)
		{
			auto interpreter = interpretSubquery(ast_join.table, context, subquery_depth, required_joined_columns);
			subquery_for_set.source = new LazyBlockInputStream([interpreter]() mutable { return interpreter->execute().in; });
			subquery_for_set.source_sample = interpreter->getSampleBlock();
		}

		/// TODO Это не нужно выставлять, когда JOIN нужен только на удалённых серверах.
		subquery_for_set.join = join;
		subquery_for_set.join->setSampleBlock(subquery_for_set.source_sample);
	}

	addJoinAction(step.actions, false);

	return true;
}

bool ExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain, bool only_types)
{
	assertSelect();

	if (!select_query->where_expression)
		return false;

	initChain(chain, columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	step.required_output.push_back(select_query->where_expression->getColumnName());
	getRootActions(select_query->where_expression, only_types, false, step.actions);

	return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain, bool only_types)
{
	assertAggregation();

	if (!select_query->group_expression_list)
		return false;

	initChain(chain, columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	ASTs asts = select_query->group_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		step.required_output.push_back(asts[i]->getColumnName());
		getRootActions(asts[i], only_types, false, step.actions);
	}

	return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain, bool only_types)
{
	assertAggregation();

	initChain(chain, columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
	{
		for (size_t j = 0; j < aggregate_descriptions[i].argument_names.size(); ++j)
		{
			step.required_output.push_back(aggregate_descriptions[i].argument_names[j]);
		}
	}

	getActionsBeforeAggregation(select_query->select_expression_list, step.actions, only_types);

	if (select_query->having_expression)
		getActionsBeforeAggregation(select_query->having_expression, step.actions, only_types);

	if (select_query->order_expression_list)
		getActionsBeforeAggregation(select_query->order_expression_list, step.actions, only_types);
}

bool ExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain, bool only_types)
{
	assertAggregation();

	if (!select_query->having_expression)
		return false;

	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	step.required_output.push_back(select_query->having_expression->getColumnName());
	getRootActions(select_query->having_expression, only_types, false, step.actions);

	return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain, bool only_types)
{
	assertSelect();

	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	getRootActions(select_query->select_expression_list, only_types, false, step.actions);

	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		step.required_output.push_back(asts[i]->getColumnName());
	}
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain, bool only_types)
{
	assertSelect();

	if (!select_query->order_expression_list)
		return false;

	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	getRootActions(select_query->order_expression_list, only_types, false, step.actions);

	ASTs asts = select_query->order_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		ASTOrderByElement * ast = typeid_cast<ASTOrderByElement *>(asts[i].get());
		if (!ast || ast->children.size() != 1)
			throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
		ASTPtr order_expression = ast->children.at(0);
		step.required_output.push_back(order_expression->getColumnName());
	}

	return true;
}

void ExpressionAnalyzer::appendProjectResult(DB::ExpressionActionsChain & chain, bool only_types) const
{
	assertSelect();

	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();

	NamesWithAliases result_columns;

	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		result_columns.emplace_back(asts[i]->getColumnName(), asts[i]->getAliasOrColumnName());
		step.required_output.push_back(result_columns.back().second);
	}

	step.actions->add(ExpressionAction::project(result_columns));
}


Block ExpressionAnalyzer::getSelectSampleBlock()
{
	assertSelect();

	ExpressionActionsPtr temp_actions = std::make_shared<ExpressionActions>(aggregated_columns, settings);
	NamesWithAliases result_columns;

	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		result_columns.emplace_back(asts[i]->getColumnName(), asts[i]->getAliasOrColumnName());
		getRootActions(asts[i], true, false, temp_actions);
	}

	temp_actions->add(ExpressionAction::project(result_columns));

	return temp_actions->getSampleBlock();
}

void ExpressionAnalyzer::getActionsBeforeAggregation(ASTPtr ast, ExpressionActionsPtr & actions, bool no_subqueries)
{
	ASTFunction * node = typeid_cast<ASTFunction *>(ast.get());

	if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
		for (auto & argument : node->arguments->children)
			getRootActions(argument, no_subqueries, false, actions);
	else
		for (auto & child : ast->children)
			getActionsBeforeAggregation(child, actions, no_subqueries);
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool project_result)
{
	ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(columns, settings);
	NamesWithAliases result_columns;
	Names result_names;

	ASTs asts;

	if (auto node = typeid_cast<const ASTExpressionList *>(ast.get()))
		asts = node->children;
	else
		asts = ASTs(1, ast);

	for (size_t i = 0; i < asts.size(); ++i)
	{
		std::string name = asts[i]->getColumnName();
		std::string alias;
		if (project_result)
			alias = asts[i]->getAliasOrColumnName();
		else
			alias = name;
		result_columns.emplace_back(name, alias);
		result_names.push_back(alias);
		getRootActions(asts[i], false, false, actions);
	}

	if (project_result)
	{
		actions->add(ExpressionAction::project(result_columns));
	}
	else
	{
		/// Не будем удалять исходные столбцы.
		for (const auto & column_name_type : columns)
			result_names.push_back(column_name_type.name);
	}

	actions->finalize(result_names);

	return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
	ExpressionActionsPtr actions = std::make_shared<ExpressionActions>(NamesAndTypesList(), settings);

	getRootActions(ast, true, true, actions);

	return actions;
}

void ExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates) const
{
	for (const auto & name_and_type : aggregation_keys)
		key_names.emplace_back(name_and_type.name);

	aggregates = aggregate_descriptions;
}

void ExpressionAnalyzer::collectUsedColumns()
{
	/** Вычислим, какие столбцы требуются для выполнения выражения.
	  * Затем, удалим все остальные столбцы из списка доступных столбцов.
	  * После выполнения, columns будет содержать только список столбцов, нужных для чтения из таблицы.
	  */

	NameSet required;
	NameSet ignored;

	if (select_query && select_query->array_join_expression_list)
	{
		ASTs & expressions = select_query->array_join_expression_list->children;
		for (size_t i = 0; i < expressions.size(); ++i)
		{
			/// Игнорируем идентификаторы верхнего уровня из секции ARRAY JOIN.
			/// Их потом добавим отдельно.
			if (typeid_cast<ASTIdentifier *>(expressions[i].get()))
			{
				ignored.insert(expressions[i]->getColumnName());
			}
			else
			{
				/// Для выражений в ARRAY JOIN ничего игнорировать не нужно.
				NameSet empty;
				getRequiredColumnsImpl(expressions[i], required, empty, empty, empty);
			}

			ignored.insert(expressions[i]->getAliasOrColumnName());
		}
	}

	/** Также нужно не учитывать идентификаторы столбцов, получающихся путём JOIN-а.
	  * (Не считать, что они требуются для чтения из "левой" таблицы).
	  */
	NameSet available_joined_columns;
	collectJoinedColumns(available_joined_columns, columns_added_by_join);

	NameSet required_joined_columns;
	getRequiredColumnsImpl(ast, required, ignored, available_joined_columns, required_joined_columns);

	for (NamesAndTypesList::iterator it = columns_added_by_join.begin(); it != columns_added_by_join.end();)
	{
		if (required_joined_columns.count(it->name))
			++it;
		else
			columns_added_by_join.erase(it++);
	}

/*	for (const auto & name_type : columns_added_by_join)
		std::cerr << "JOINed column (required, not key): " << name_type.name << std::endl;
	std::cerr << std::endl;*/

	/// Вставляем в список требуемых столбцов столбцы, нужные для вычисления ARRAY JOIN.
	NameSet array_join_sources;
	for (const auto & result_source : array_join_result_to_source)
		array_join_sources.insert(result_source.second);

	for (const auto & column_name_type : columns)
		if (array_join_sources.count(column_name_type.name))
			required.insert(column_name_type.name);

	/// Нужно прочитать хоть один столбец, чтобы узнать количество строк.
	if (required.empty())
		required.insert(ExpressionActions::getSmallestColumn(columns));

	unknown_required_columns = required;

	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end();)
	{
		unknown_required_columns.erase(it->name);

		if (!required.count(it->name))
		{
			required.erase(it->name);
			columns.erase(it++);
		}
		else
			++it;
	}

	/// Возможно, среди неизвестных столбцов есть виртуальные. Удаляем их из списка неизвестных и добавляем
	/// в columns list, чтобы при дальнейшей обработке запроса они воспринимались как настоящие.
	if (storage)
	{
		for (auto it = unknown_required_columns.begin(); it != unknown_required_columns.end();)
		{
			if (storage->hasColumn(*it))
			{
				columns.push_back(storage->getColumn(*it));
				unknown_required_columns.erase(it++);
			}
			else
				++it;
		}
	}
}

void ExpressionAnalyzer::collectJoinedColumns(NameSet & joined_columns, NamesAndTypesList & joined_columns_name_type)
{
	if (!select_query || !select_query->join)
		return;

	auto & node = typeid_cast<ASTJoin &>(*select_query->join);

	Block nested_result_sample;
	if (const auto identifier = typeid_cast<const ASTIdentifier *>(node.table.get()))
	{
		const auto & table = context.getTable("", identifier->name);
		nested_result_sample = table->getSampleBlockNonMaterialized();
	}
	else if (typeid_cast<const ASTSubquery *>(node.table.get()))
	{
		const auto & subquery = node.table->children.at(0);
		nested_result_sample = InterpreterSelectQuery::getSampleBlock(subquery, context);
	}

	if (node.using_expr_list)
	{
		auto & keys = typeid_cast<ASTExpressionList &>(*node.using_expr_list);
		for (const auto & key : keys.children)
		{
			if (join_key_names_left.end() == std::find(join_key_names_left.begin(), join_key_names_left.end(), key->getColumnName()))
				join_key_names_left.push_back(key->getColumnName());
			else
				throw Exception("Duplicate column " + key->getColumnName() + " in USING list", ErrorCodes::DUPLICATE_COLUMN);

			if (join_key_names_right.end() == std::find(join_key_names_right.begin(), join_key_names_right.end(), key->getAliasOrColumnName()))
				join_key_names_right.push_back(key->getAliasOrColumnName());
			else
				throw Exception("Duplicate column " + key->getAliasOrColumnName() + " in USING list", ErrorCodes::DUPLICATE_COLUMN);
		}
	}

	for (const auto i : ext::range(0, nested_result_sample.columns()))
	{
		const auto & col = nested_result_sample.getByPosition(i);
		if (join_key_names_right.end() == std::find(join_key_names_right.begin(), join_key_names_right.end(), col.name)
			&& !joined_columns.count(col.name))	/// Дублирующиеся столбцы в подзапросе для JOIN-а не имеют смысла.
		{
			joined_columns.insert(col.name);
			joined_columns_name_type.emplace_back(col.name, col.type);
		}
	}

/*	for (const auto & name : join_key_names_left)
		std::cerr << "JOIN key (left): " << name << std::endl;
	for (const auto & name : join_key_names_right)
		std::cerr << "JOIN key (right): " << name << std::endl;
	std::cerr << std::endl;
	for (const auto & name : joined_columns)
		std::cerr << "JOINed column: " << name << std::endl;
	std::cerr << std::endl;*/
}

Names ExpressionAnalyzer::getRequiredColumns()
{
	if (!unknown_required_columns.empty())
		throw Exception("Unknown identifier: " + *unknown_required_columns.begin(), ErrorCodes::UNKNOWN_IDENTIFIER);

	Names res;
	for (const auto & column_name_type : columns)
		res.push_back(column_name_type.name);

	return res;
}

void ExpressionAnalyzer::getRequiredColumnsImpl(ASTPtr ast,
	NameSet & required_columns, NameSet & ignored_names,
	const NameSet & available_joined_columns, NameSet & required_joined_columns)
{
	/** Найдём все идентификаторы в запросе.
	  * Будем искать их рекурсивно, обходя в глубину AST.
	  * При этом:
	  * - для лямбда функций не будем брать формальные параметры;
	  * - не опускаемся в подзапросы (там свои идентификаторы);
	  * - некоторое исключение для секции ARRAY JOIN (в ней идентификаторы немного другие);
	  * - идентификаторы, доступные из JOIN-а, кладём в required_joined_columns.
	  */

	if (ASTIdentifier * node = typeid_cast<ASTIdentifier *>(ast.get()))
	{
		if (node->kind == ASTIdentifier::Column
			&& !ignored_names.count(node->name)
			&& !ignored_names.count(DataTypeNested::extractNestedTableName(node->name)))
		{
			if (!available_joined_columns.count(node->name))
				required_columns.insert(node->name);
			else
				required_joined_columns.insert(node->name);
		}

		return;
	}

	if (ASTFunction * node = typeid_cast<ASTFunction *>(ast.get()))
	{
		if (node->kind == ASTFunction::LAMBDA_EXPRESSION)
		{
			if (node->arguments->children.size() != 2)
				throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

			ASTFunction * lambda_args_tuple = typeid_cast<ASTFunction *>(node->arguments->children.at(0).get());

			if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
				throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);

			/// Не нужно добавлять формальные параметры лямбда-выражения в required_columns.
			Names added_ignored;
			for (auto & child : lambda_args_tuple->arguments->children)
			{
				ASTIdentifier * identifier = typeid_cast<ASTIdentifier *>(child.get());
				if (!identifier)
					throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);

				String & name = identifier->name;
				if (!ignored_names.count(name))
				{
					ignored_names.insert(name);
					added_ignored.push_back(name);
				}
			}

			getRequiredColumnsImpl(node->arguments->children.at(1),
				required_columns, ignored_names,
				available_joined_columns, required_joined_columns);

			for (size_t i = 0; i < added_ignored.size(); ++i)
				ignored_names.erase(added_ignored[i]);

			return;
		}

		/// Особая функция indexHint. Всё, что внутри неё не вычисляется
		/// (а используется только для анализа индекса, см. PKCondition).
		if (node->name == "indexHint")
			return;
	}

	ASTSelectQuery * select = typeid_cast<ASTSelectQuery *>(ast.get());

	/// Рекурсивный обход выражения.
	for (auto & child : ast->children)
	{
		/** Не пойдем в секцию ARRAY JOIN, потому что там нужно смотреть на имена не-ARRAY-JOIN-енных столбцов.
		  * Туда collectUsedColumns отправит нас отдельно.
		  */
		if (!typeid_cast<ASTSubquery *>(child.get()) && !typeid_cast<ASTSelectQuery *>(child.get()) &&
			!(select && child == select->array_join_expression_list))
			getRequiredColumnsImpl(child, required_columns, ignored_names, available_joined_columns, required_joined_columns);
    }
}

}
