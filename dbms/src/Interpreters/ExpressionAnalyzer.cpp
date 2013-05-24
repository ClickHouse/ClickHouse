#include <DB/DataTypes/FieldToDataType.h>

#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTAsterisk.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTSubquery.h>
#include <DB/Parsers/ASTSet.h>

#include <DB/DataTypes/DataTypeSet.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <DB/DataTypes/DataTypeExpression.h>
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnExpression.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>

#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageDistributed.h>


namespace DB
{


static std::string * GetAlias(ASTPtr & ast)
{
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		return &node->alias;
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		return &node->alias;
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		return &node->alias;
	}
	else
	{
		return NULL;
	}
}


void ExpressionAnalyzer::init()
{
	select_query = dynamic_cast<ASTSelectQuery *>(&*ast);
	has_aggregation = false;
	
	createAliasesDict(ast); /// Если есть агрегатные функции, присвоит has_aggregation=true.
	normalizeTree();
	
	if (select_query && (select_query->group_expression_list || select_query->having_expression))
		has_aggregation = true;
	
	if (has_aggregation)
	{
		if (!select_query)
			throw Exception("Aggregation not in select query", ErrorCodes::ILLEGAL_AGGREGATION);
		
		/// Действия до агрегации. Используются только для определения типов.
		ExpressionActions actions(columns);
		
		/// Найдем агрегатные функции.
		getAggregatesImpl(ast, actions);
		
		/// Найдем ключи агрегации.
		if (select_query->group_expression_list)
		{
			const ASTs & group_asts = select_query->group_expression_list->children;
			for (size_t i = 0; i < group_asts.size(); ++i)
			{
				getActionsImpl(group_asts[i], true, false, actions);
				NameAndTypePair key;
				key.first = group_asts[i]->getColumnName();
				key.second = actions.getSampleBlock().getByName(key.first).type;
				aggregation_keys.push_back(key);
			}
		}
	}
}


NamesAndTypesList::iterator ExpressionAnalyzer::findColumn(const String & name)
{
	NamesAndTypesList::iterator it;
	for (it = columns.begin(); it != columns.end(); ++it)
		if (it->first == name)
			break;
	return it;
}


void ExpressionAnalyzer::createAliasesDict(ASTPtr & ast)
{
	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			createAliasesDict(*it);
	
	std::string * alias = GetAlias(ast);
	if (alias && !alias->empty())
	{
		if (aliases.count(*alias) && ast->getTreeID() != aliases[*alias]->getTreeID())
		{
			throw Exception("Different expressions with the same alias " + *alias, ErrorCodes::MULTIPLE_EXPRESSIONS_FOR_ALIAS);
		}
		else
		{
			aliases[*alias] = ast;
		}
	}
}


StoragePtr ExpressionAnalyzer::getTable()
{
	if (const ASTSelectQuery * select = dynamic_cast<const ASTSelectQuery *>(&*ast))
	{
		if (select->table && !dynamic_cast<const ASTSelectQuery *>(&*select->table))
		{
			String database = select->database ? 
				dynamic_cast<const ASTIdentifier &>(*select->database).name :
				"";
			const String & table = dynamic_cast<const ASTIdentifier &>(*select->table).name;
			return context.tryGetTable(database, table);
		}
	}
	return StoragePtr();
}


bool ExpressionAnalyzer::needSignRewrite()
{
	if (settings.sign_rewrite && storage)
	{
		if (const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(&*storage))
			return merge_tree->getName() == "CollapsingMergeTree";
		if (const StorageDistributed * distributed = dynamic_cast<const StorageDistributed *>(&*storage))
			return !distributed->getSignColumnName().empty();
	}
	return false;
}


String ExpressionAnalyzer::getSignColumnName()
{
	if (const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(&*storage))
		return merge_tree->getSignColumnName();
	if (const StorageDistributed * distributed = dynamic_cast<const StorageDistributed *>(&*storage))
		return distributed->getSignColumnName();
	return "";
}


ASTPtr ExpressionAnalyzer::createSignColumn()
{
	ASTIdentifier * p_sign_column = new ASTIdentifier(ast->range, sign_column_name);
	ASTIdentifier & sign_column = *p_sign_column;
	ASTPtr sign_column_node = p_sign_column;
	sign_column.name = sign_column_name;
	return sign_column_node;
}


ASTPtr ExpressionAnalyzer::rewriteCount(const ASTFunction * node)
{
	/// 'Sign'
	ASTExpressionList * p_exp_list = new ASTExpressionList;
	ASTExpressionList & exp_list = *p_exp_list;
	ASTPtr exp_list_node = p_exp_list;
	exp_list.children.push_back(createSignColumn());
	
	/// sum(Sign)
	ASTFunction * p_sum = new ASTFunction;
	ASTFunction & sum = *p_sum;
	ASTPtr sum_node = p_sum;
	sum.name = "sum";
	sum.alias = node->alias;
	sum.arguments = exp_list_node;
	sum.children.push_back(exp_list_node);
	
	return sum_node;
}


ASTPtr ExpressionAnalyzer::rewriteSum(const ASTFunction * node)
{	
	/// 'x', 'Sign'
	ASTExpressionList * p_mult_exp_list = new ASTExpressionList;
	ASTExpressionList & mult_exp_list = *p_mult_exp_list;
	ASTPtr mult_exp_list_node = p_mult_exp_list;
	mult_exp_list.children.push_back(createSignColumn());
	mult_exp_list.children.push_back(node->arguments->children[0]);
	
	/// x * Sign
	ASTFunction * p_mult = new ASTFunction;
	ASTFunction & mult = *p_mult;
	ASTPtr mult_node = p_mult;
	mult.name = "multiply";
	mult.arguments = mult_exp_list_node;
	mult.children.push_back(mult_exp_list_node);
	
	/// 'x * Sign'
	ASTExpressionList * p_exp_list = new ASTExpressionList;
	ASTExpressionList & exp_list = *p_exp_list;
	ASTPtr exp_list_node = p_exp_list;
	exp_list.children.push_back(mult_node);
	
	/// sum(x * Sign)
	ASTFunction * p_sum = new ASTFunction;
	ASTFunction & sum = *p_sum;
	ASTPtr sum_node = p_sum;
	sum.name = "sum";
	sum.alias = node->alias;
	sum.arguments = exp_list_node;
	sum.children.push_back(exp_list_node);	
	
	return sum_node;
}


ASTPtr ExpressionAnalyzer::rewriteAvg(const ASTFunction * node)
{
	/// node без alias для переписывания числителя и знаменателя
	ASTPtr node_clone = node->clone();
	ASTFunction * node_clone_func = dynamic_cast<ASTFunction *>(&*node_clone);
	node_clone_func->alias = "";
	
	/// 'sum(Sign * x)', 'sum(Sign)'
	ASTExpressionList * p_div_exp_list = new ASTExpressionList;
	ASTExpressionList & div_exp_list = *p_div_exp_list;
	ASTPtr div_exp_list_node = p_div_exp_list;
	div_exp_list.children.push_back(rewriteSum(node_clone_func));
	div_exp_list.children.push_back(rewriteCount(node_clone_func));
	
	/// sum(Sign * x) / sum(Sign)
	ASTFunction * p_div = new ASTFunction;
	ASTFunction & div = *p_div;
	ASTPtr div_node = p_div;
	div.name = "divide";
	div.alias = node->alias;
	div.arguments = div_exp_list_node;
	div.children.push_back(div_exp_list_node);
	
	return div_node;
}


void ExpressionAnalyzer::considerSignRewrite(ASTPtr & ast)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	const String & name = node->name;
	if (name == "count")
		ast = rewriteCount(node);
	if (name == "sum")
		ast = rewriteSum(node);
	if (name == "avg")
		ast = rewriteAvg(node);
}


void ExpressionAnalyzer::normalizeTree()
{
	SetOfASTs tmp_set;
	MapOfASTs tmp_map;
	if (needSignRewrite())
		sign_column_name = getSignColumnName();
	normalizeTreeImpl(ast, tmp_map, tmp_set);
}


/// finished_asts - уже обработанные вершины (и на что они заменены)
/// current_asts - вершины в текущем стеке вызовов этого метода
void ExpressionAnalyzer::normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts)
{
	if (current_asts.count(ast))
	{
		throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);
	}
	if (finished_asts.count(ast))
	{
		ast = finished_asts[ast];
		return;
	}
	
	ASTPtr initial_ast = ast;
	current_asts.insert(initial_ast);
	
	/// rewrite правила, которые действуют при обходе сверху-вниз.
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		/** Нет ли в таблице столбца, название которого полностью совпадает с записью функции?
		  * Например, в таблице есть столбец "domain(URL)", и мы запросили domain(URL).
		  */
		String function_string = node->getColumnName();
		NamesAndTypesList::const_iterator it = findColumn(function_string);
		if (columns.end() != it)
		{
			ASTIdentifier * ast_id = new ASTIdentifier(node->range, std::string(node->range.first, node->range.second));
			ast_id->type = it->second;
			ast = ast_id;
			current_asts.insert(ast);
		}
	}
	
	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			normalizeTreeImpl(*it, finished_asts, current_asts);
	
	/// Если секция WHERE или HAVING состоит из одного алиаса, ссылку нужно заменить не только в children, но и в where_expression и having_expression.
	if (ASTSelectQuery * select = dynamic_cast<ASTSelectQuery *>(&*ast))
	{
		if (select->where_expression)
			normalizeTreeImpl(select->where_expression, finished_asts, current_asts);
		if (select->having_expression)
			normalizeTreeImpl(select->having_expression, finished_asts, current_asts);
	}
	
	if (dynamic_cast<ASTAsterisk *>(&*ast))
	{
		ASTExpressionList * all_columns = new ASTExpressionList(ast->range);
		for (NamesAndTypesList::const_iterator it = columns.begin(); it != columns.end(); ++it)
			all_columns->children.push_back(new ASTIdentifier(ast->range, it->first));
		ast = all_columns;
		current_asts.insert(ast);

		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			normalizeTreeImpl(*it, finished_asts, current_asts);
	}
	else if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->name == "lambda")
		{
			node->is_lambda_expression = true;
		}
		else if (context.getAggregateFunctionFactory().isAggregateFunctionName(node->name))
		{
			node->is_aggregate_function = true;
			has_aggregation = true;
			if (!sign_column_name.empty())
				considerSignRewrite(ast);
		}
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			/// Если это алиас
			Aliases::const_iterator jt = aliases.find(node->name);
			if (jt != aliases.end())
			{
				/// Заменим его на соответствующий узел дерева
				ast = jt->second;

				normalizeTreeImpl(ast, finished_asts, current_asts);
			}
			
			/// Проверим имеет ли смысл sign-rewrite
			if (node->name == sign_column_name)
				throw Exception("Requested Sign column while sign-rewrite is on.", ErrorCodes::QUERY_SECTION_DOESNT_MAKE_SENSE);
		}
	}
	
	current_asts.erase(initial_ast);
	current_asts.erase(ast);
	finished_asts[initial_ast] = ast;
}


/// ast - ASTFunction с названием in или notIn.
void ExpressionAnalyzer::makeSet(ASTPtr ast)
{
	throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED);
}


static std::string getUniqueName(const Block & block, const std::string & prefix)
{
	int i = 1;
	while (block.has(prefix + Poco::NumberFormatter::format(i)))
		++i;
	return prefix + Poco::NumberFormatter::format(i);
}


void ExpressionAnalyzer::getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActions & actions)
{
	/// Если результат вычисления уже есть в блоке.
	if ((dynamic_cast<ASTFunction *>(&*ast) || dynamic_cast<ASTLiteral *>(&*ast))
		&& actions.getSampleBlock().has(ast->getColumnName()))
		return;
	
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->is_lambda_expression)
			throw Exception("Unexpected expression", ErrorCodes::UNEXPECTED_EXPRESSION);
		
		if (!node->is_aggregate_function)
		{
			if (node->name == "in" || node->name == "notIn")
			{
				if (!no_subqueries)
				{
					/// Превратим tuple или подзапрос в множество.
					makeSet(ast);
				}
				else
				{
					/// Мы в той части дерева, которую не собираемся вычислять. Нужно только определить типы.
					/// Не будем выполнять подзапросы и составлять множества. Вставим произвольный столбец правильного типа.
					ColumnWithNameAndType fake_column;
					fake_column.name = node->getColumnName();
					fake_column.type = new DataTypeUInt8;
					fake_column.column = new ColumnConstUInt8(1, 0);
					actions.add(ExpressionActions::Action(fake_column));
					getActionsImpl(node->arguments, no_subqueries, only_consts, actions);
					return;
				}
			}
			
			FunctionPtr function = context.getFunctionFactory().get(node->name, context);
			
			Names argument_names;
			DataTypes argument_types;
			
			/// Если у функции есть аргумент-лямбда-выражение, нужно определить его тип до рекурсивного вызова.
			bool has_lambda_arguments = false;
			
			for (size_t i = 0; i < node->arguments->children.size(); ++i)
			{
				ASTPtr child = node->arguments->children[i];
				
				ASTFunction * lambda = dynamic_cast<ASTFunction *>(&*child);
				if (lambda && lambda->name == "lambda")
				{
					/// Если аргумент лямбда-функция, только запомним ее примерный тип.
					if (lambda->arguments->children.size() != 2)
						throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
					
					ASTFunction * lambda_args_tuple = dynamic_cast<ASTFunction *>(&*lambda->arguments->children[0]);
					
					if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
						throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);
					
					has_lambda_arguments = true;
					argument_types.push_back(new DataTypeExpression(DataTypes(lambda_args_tuple->arguments->children.size())));
					/// Не можем дать название child->getColumnName(),
					/// потому что оно не однозначно определяет выражение (типы аргументов могут быть разными).
					argument_names.push_back(getUniqueName(actions.getSampleBlock(), "__lambda"));
				}
				else
				{
					/// Если аргумент не лямбда-функция, вызовемся рекурсивно и узнаем его тип.
					getActionsImpl(child, no_subqueries, only_consts, actions);
					std::string name = child->getColumnName();
					argument_types.push_back(actions.getSampleBlock().getByName(name).type);
					argument_names.push_back(name);
				}
			}
			
			if (has_lambda_arguments && !only_consts)
			{
				function->getLambdaArgumentTypes(argument_types);
				
				/// Вызовемся рекурсивно для лямбда-выражений.
				for (size_t i = 0; i < node->arguments->children.size(); ++i)
				{
					ASTPtr child = node->arguments->children[i];
					
					ASTFunction * lambda = dynamic_cast<ASTFunction *>(&*child);
					if (lambda && lambda->name == "lambda")
					{
						DataTypeExpression * lambda_type = dynamic_cast<DataTypeExpression *>(&*argument_types[i]);
						ASTFunction * lambda_args_tuple = dynamic_cast<ASTFunction *>(&*lambda->arguments->children[0]);
						ASTs lambda_arg_asts = lambda_args_tuple->arguments->children;
						NamesAndTypes lambda_args;
						NamesAndTypesList initial_columns = columns;
						
						for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
						{
							ASTIdentifier * identifier = dynamic_cast<ASTIdentifier *>(&*lambda_arg_asts[j]);
							if (!identifier)
								throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);
							
							String arg_name = identifier->name;
							NameAndTypePair arg(arg_name, lambda_type->getArgumentTypes()[j]);
							
							NamesAndTypesList::iterator it = findColumn(arg_name);
							if (it != columns.end())
								it->second = arg.second;
							else
								columns.push_front(arg);
							
							lambda_args.push_back(arg);
						}
						
						ExpressionActionsPtr lambda_actions = new ExpressionActions(columns);
						getActionsImpl(lambda->arguments->children[1], no_subqueries, only_consts, *lambda_actions);
						
						columns = initial_columns;
						
						String result_name = lambda->arguments->children[1]->getColumnName();
						lambda_actions->finalize(NamesWithAliases(1, NameWithAlias(result_name, "")));
						DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;
						
						ColumnWithNameAndType lambda_column;
						lambda_column.column = new ColumnExpression(1, lambda_actions, lambda_args, result_type, result_name);
						lambda_column.type = argument_types[i];
						lambda_column.name = argument_names[i];
						actions.add(ExpressionActions::Action(lambda_column));
					}
				}
			}
			
			bool should_add = true;
			if (only_consts)
			{
				for (size_t i = 0; i < argument_names.size(); ++i)
				{
					if (!actions.getSampleBlock().has(argument_names[i]))
					{
						should_add = false;
						break;
					}
				}
			}
			
			if (should_add)
				actions.add(ExpressionActions::Action(function, argument_names, node->getColumnName()));
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		DataTypePtr type = apply_visitor(FieldToDataType(), node->value);
		ColumnWithNameAndType column;
		column.column = type->createConstColumn(1, node->value);
		column.type = type;
		column.name = node->getColumnName();
		
		actions.add(ExpressionActions::Action(column));
	}
	else if (ASTSet * node = dynamic_cast<ASTSet *>(&*ast))
	{
		/// Множество в секции IN.
		ColumnWithNameAndType column;
		column.column = new ColumnSet(1, node->set);
		column.type = new DataTypeSet;
		column.name = node->getColumnName();
		
		actions.add(ExpressionActions::Action(column));
	}
	else
	{
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			getActionsImpl(*it, no_subqueries, only_consts, actions);
	}

	/// Проверка ограничений на максимальное количество столбцов.

	const Block & block = actions.getSampleBlock();
	
	const Limits & limits = settings.limits;
	if (limits.max_temporary_columns && block.columns() > limits.max_temporary_columns)
		throw Exception("Too much temporary columns: " + block.dumpNames()
			+ ". Maximum: " + Poco::NumberFormatter::format(limits.max_temporary_columns),
			ErrorCodes::TOO_MUCH_TEMPORARY_COLUMNS);

	size_t non_const_columns = 0;
	for (size_t i = 0, size = block.columns(); i < size; ++i)
		if (!block.getByPosition(i).column->isConst())
			++non_const_columns;

	if (limits.max_temporary_non_const_columns && non_const_columns > limits.max_temporary_non_const_columns)
	{
		std::stringstream list_of_non_const_columns;
		for (size_t i = 0, size = block.columns(); i < size; ++i)
			if (!block.getByPosition(i).column->isConst())
				list_of_non_const_columns << (i == 0 ? "" : ", ") << block.getByPosition(i).name;
		
		throw Exception("Too much temporary non-const columns: " + list_of_non_const_columns.str()
			+ ". Maximum: " + Poco::NumberFormatter::format(limits.max_temporary_non_const_columns),
			ErrorCodes::TOO_MUCH_TEMPORARY_NON_CONST_COLUMNS);
	}
}


void ExpressionAnalyzer::getAggregatesImpl(ASTPtr ast, ExpressionActions & actions)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	if (node && node->is_aggregate_function)
	{
		AggregateDescription aggregate;
		aggregate.column_name = node->getColumnName();
		
		for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
			if (aggregate_descriptions[i].column_name == aggregate.column_name)
				return;
		
		ASTs & arguments = node->arguments->children;
		aggregate.argument_names.resize(arguments.size());
		DataTypes types(arguments.size());
		
		for (size_t i = 0; i < arguments.size(); ++i)
		{
			getActionsImpl(arguments[i], true, false, actions);
			const std::string & name = arguments[i]->getColumnName();
			types[i] = actions.getSampleBlock().getByName(name).type;
			aggregate.argument_names[i] = name;
		}
		
		aggregate.function = context.getAggregateFunctionFactory().get(node->name, types);;
		
		aggregate_descriptions.push_back(aggregate);
	}
	else
	{
		for (size_t i = 0; i < ast->children.size(); ++i)
		{
			getAggregatesImpl(ast->children[i], actions);
		}
	}
}


ExpressionActionsPtr ExpressionAnalyzer::getActionsAfterAggregation()
{
	if (!has_aggregation)
		throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
	
	NamesAndTypesList aggregated_columns = aggregation_keys;
	for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
	{
		AggregateDescription & desc = aggregate_descriptions[i];
		aggregated_columns.push_back(NameAndTypePair(desc.column_name, desc.function->getReturnType()));
	}
	
	ExpressionActionsPtr actions = new ExpressionActions(aggregated_columns);
	NamesWithAliases result_columns;
	
	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), asts[i]->getAlias()));
		getActionsImpl(asts[i], false, false, *actions);
	}
	
	if (select_query->having_expression)
	{
		result_columns.push_back(NameWithAlias(select_query->having_expression->getColumnName(), ""));
		getActionsImpl(select_query->having_expression, false, false, *actions);
	}
	
	if (select_query->order_expression_list)
	{
		asts = select_query->order_expression_list->children;
		for (size_t i = 0; i < asts.size(); ++i)
		{
			result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), ""));
			getActionsImpl(asts[i], false, false, *actions);
		}
	}
	
	actions->finalize(result_columns);
	
	return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getActionsBeforeAggregation()
{
	if (!has_aggregation)
		throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
	
	ExpressionActionsPtr actions = new ExpressionActions(columns);
	NamesWithAliases result_columns;
	
	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		getActionsBeforeAggregationImpl(asts[i], *actions, result_columns);
	}
	
	if (select_query->having_expression)
	{
		getActionsBeforeAggregationImpl(select_query->having_expression, *actions, result_columns);
	}
	
	if (select_query->order_expression_list)
	{
		asts = select_query->order_expression_list->children;
		for (size_t i = 0; i < asts.size(); ++i)
		{
			getActionsBeforeAggregationImpl(asts[i], *actions, result_columns);
		}
	}
	
	if (select_query->where_expression)
	{
		result_columns.push_back(NameWithAlias(select_query->where_expression->getColumnName(), ""));
		getActionsImpl(select_query->where_expression, false, false, *actions);
	}
	
	if (select_query->group_expression_list)
	{
		asts = select_query->group_expression_list->children;
		for (size_t i = 0; i < asts.size(); ++i)
		{
			result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), ""));
			getActionsImpl(asts[i], false, false, *actions);
		}
	}
	
	actions->finalize(result_columns);
	
	return actions;
}


void ExpressionAnalyzer::getActionsBeforeAggregationImpl(ASTPtr ast, ExpressionActions & actions, NamesWithAliases & result_columns)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	if (node && node->is_aggregate_function)
	{
		ASTs & arguments = node->arguments->children;
		
		for (size_t i = 0; i < arguments.size(); ++i)
		{
			result_columns.push_back(NameWithAlias(arguments[i]->getColumnName(), ""));
			getActionsImpl(arguments[i], false, false, actions);
		}
	}
	else
	{
		for (size_t i = 0; i < ast->children.size(); ++i)
		{
			getActionsBeforeAggregationImpl(ast->children[i], actions, result_columns);
		}
	}
}


ExpressionActionsPtr ExpressionAnalyzer::getActions()
{
	if (has_aggregation)
		throw Exception("Expression has aggregation", ErrorCodes::LOGICAL_ERROR);
	
	ExpressionActionsPtr actions = new ExpressionActions(columns);
	NamesWithAliases result_columns;
	
	if (select_query)
	{
		ASTs asts = select_query->select_expression_list->children;
		for (size_t i = 0; i < asts.size(); ++i)
		{
			result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), asts[i]->getAlias()));
			getActionsImpl(asts[i], false, false, *actions);
		}
		
		if (select_query->having_expression)
		{
			result_columns.push_back(NameWithAlias(select_query->having_expression->getColumnName(), ""));
			getActionsImpl(select_query->having_expression, false, false, *actions);
		}
		
		if (select_query->order_expression_list)
		{
			asts = select_query->order_expression_list->children;
			for (size_t i = 0; i < asts.size(); ++i)
			{
				result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), ""));
				getActionsImpl(asts[i], false, false, *actions);
			}
		}
		
		if (select_query->where_expression)
		{
			result_columns.push_back(NameWithAlias(select_query->where_expression->getColumnName(), ""));
			getActionsImpl(select_query->where_expression, false, false, *actions);
		}
	}
	else if (ASTExpressionList * node = dynamic_cast<ASTExpressionList *>(&*ast))
	{
		ASTs asts = node->children;
		for (size_t i = 0; i < asts.size(); ++i)
		{
			result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), asts[i]->getAlias()));
			getActionsImpl(asts[i], false, false, *actions);
		}
	}
	else
	{
		result_columns.push_back(NameWithAlias(ast->getColumnName(), ast->getAlias()));
		getActionsImpl(ast, false, false, *actions);
	}
	
	actions->finalize(result_columns);
	
	return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
	if (has_aggregation)
		throw Exception("Expression has aggregation", ErrorCodes::LOGICAL_ERROR);
	
	ExpressionActionsPtr actions = new ExpressionActions(NamesAndTypesList());
	
	getActionsImpl(ast, true, true, *actions);
	
	return actions;
}

void ExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates)
{
	for (NamesAndTypesList::iterator it = aggregation_keys.begin(); it != aggregation_keys.end(); ++it)
		key_names.push_back(it->first);
	aggregates = aggregate_descriptions;
}

}
