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
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/Columns/ColumnSet.h>
#include <DB/Columns/ColumnExpression.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>

#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageDistributed.h>


namespace DB
{


static std::string * getAlias(ASTPtr & ast)
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
	
	removeUnusedColumns();
	
	/// Найдем агрегатные функции.
	if (select_query && (select_query->group_expression_list || select_query->having_expression))
		has_aggregation = true;
	
	ExpressionActions temp_actions(columns, settings);
	getAggregatesImpl(ast, temp_actions);
	
	if (has_aggregation)
	{
		assertSelect();
		
		/// Найдем ключи агрегации.
		if (select_query->group_expression_list)
		{
			NameSet unique_keys;
			const ASTs & group_asts = select_query->group_expression_list->children;
			for (size_t i = 0; i < group_asts.size(); ++i)
			{
				getRootActionsImpl(group_asts[i], true, false, temp_actions);
				NameAndTypePair key;
				key.first = group_asts[i]->getColumnName();
				key.second = temp_actions.getSampleBlock().getByName(key.first).type;
				aggregation_keys.push_back(key);
				
				if (!unique_keys.count(key.first))
				{
					aggregated_columns.push_back(key);
					unique_keys.insert(key.first);
				}
			}
		}
		
		for (size_t i = 0; i < aggregate_descriptions.size(); ++i)
		{
			AggregateDescription & desc = aggregate_descriptions[i];
			aggregated_columns.push_back(NameAndTypePair(desc.column_name, desc.function->getReturnType()));
		}
	}
	else
	{
		aggregated_columns = columns;	
	}
}


NamesAndTypesList::iterator ExpressionAnalyzer::findColumn(const String & name, NamesAndTypesList & cols)
{
	NamesAndTypesList::iterator it;
	for (it = cols.begin(); it != cols.end(); ++it)
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
	
	std::string * alias = getAlias(ast);
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


bool ExpressionAnalyzer::considerSignRewrite(ASTPtr & ast)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	if (!node)
		return false;
	const String & name = node->name;
	if (name == "count")
		ast = rewriteCount(node);
	else if (name == "sum")
		ast = rewriteSum(node);
	else if (name == "avg")
		ast = rewriteAvg(node);
	else
		return false;
	return true;
}


void ExpressionAnalyzer::normalizeTree()
{
	SetOfASTs tmp_set;
	MapOfASTs tmp_map;
	if (needSignRewrite())
		sign_column_name = getSignColumnName();
	normalizeTreeImpl(ast, tmp_map, tmp_set, "", false);
}


/// finished_asts - уже обработанные вершины (и на что они заменены)
/// current_asts - вершины в текущем стеке вызовов этого метода
/// current_alias - алиас, повешенный на предка ast (самого глебокого из предков с алиасами)
/// in_sign_rewritten - находимся ли мы в поддереве, полученном в результате sign rewrite
void ExpressionAnalyzer::normalizeTreeImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts, std::string current_alias, bool in_sign_rewritten)
{
	if (finished_asts.count(ast))
	{
		ast = finished_asts[ast];
		return;
	}
	
	ASTPtr initial_ast = ast;
	current_asts.insert(initial_ast);

	std::string * my_alias = getAlias(ast);
	if (my_alias && !my_alias->empty())
		current_alias = *my_alias;
	
	/// rewrite правила, которые действуют при обходе сверху-вниз.
	
	if (!in_sign_rewritten && !sign_column_name.empty())
		in_sign_rewritten = considerSignRewrite(ast);
	
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
			ast = ast_id;
			current_asts.insert(ast);
		}
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			/// Если это алиас, но не родительский алиас (чтобы работали конструкции вроде "SELECT column+1 AS column").
			Aliases::const_iterator jt = aliases.find(node->name);
			if (jt != aliases.end() && current_alias != node->name)
			{
				/// Заменим его на соответствующий узел дерева
				if (current_asts.count(jt->second))
					throw Exception("Cyclic aliases", ErrorCodes::CYCLIC_ALIASES);
				ast = jt->second;
			}
			else
			{
				/// Проверим имеет ли смысл sign-rewrite
				if (!in_sign_rewritten && sign_column_name != "" && node->name == sign_column_name)
					throw Exception("Requested Sign column while sign-rewrite is on.", ErrorCodes::QUERY_SECTION_DOESNT_MAKE_SENSE);
			}
		}
	}
	else if (ASTExpressionList * node = dynamic_cast<ASTExpressionList *>(&*ast))
	{
		/// Заменим * на список столбцов.
		ASTs & asts = node->children;
		for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
		{
			if (ASTAsterisk * asterisk = dynamic_cast<ASTAsterisk *>(&*asts[i]))
			{
				ASTs all_columns;
				for (NamesAndTypesList::const_iterator it = columns.begin(); it != columns.end(); ++it)
					all_columns.push_back(new ASTIdentifier(asterisk->range, it->first));
				asts.erase(asts.begin() + i);
				asts.insert(asts.begin() + i, all_columns.begin(), all_columns.end());
			}
		}
	}
	
	/// Рекурсивные вызовы. Не опускаемся в подзапросы.
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			normalizeTreeImpl(*it, finished_asts, current_asts, current_alias, in_sign_rewritten);
	
	/// Если секция WHERE или HAVING состоит из одного алиаса, ссылку нужно заменить не только в children, но и в where_expression и having_expression.
	if (ASTSelectQuery * select = dynamic_cast<ASTSelectQuery *>(&*ast))
	{
		if (select->where_expression)
			normalizeTreeImpl(select->where_expression, finished_asts, current_asts, current_alias, in_sign_rewritten);
		if (select->having_expression)
			normalizeTreeImpl(select->having_expression, finished_asts, current_asts, current_alias, in_sign_rewritten);
	}
	
	/// Действия, выполняемые снизу вверх.

	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->name == "lambda")
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
	}
	
	current_asts.erase(initial_ast);
	current_asts.erase(ast);
	finished_asts[initial_ast] = ast;
}


void ExpressionAnalyzer::makeSet(ASTFunction * node, const Block & sample_block)
{
	/** Нужно преобразовать правый аргумент в множество.
	  * Это может быть перечисление значений или подзапрос.
	  * Перечисление значений парсится как функция tuple.
	  */
	IAST & args = *node->arguments;
	ASTPtr & arg = args.children[1];
	if (dynamic_cast<ASTSubquery *>(&*arg))
	{
		/// Исполняем подзапрос, превращаем результат в множество, и кладём это множество на место подзапроса.
		InterpreterSelectQuery interpreter(arg->children[0], context, QueryProcessingStage::Complete, subquery_depth + 1);
		ASTSet * ast_set = new ASTSet(arg->getColumnName());
		ast_set->set = new Set(settings.limits);
		ast_set->set->create(interpreter.execute());
		arg = ast_set;
	}
	else if (ASTFunction * set_func = dynamic_cast<ASTFunction *>(&*arg))
	{
		/// Случай явного перечисления значений.
		if (set_func->name != "tuple")
			throw Exception("Incorrect type of 2nd argument for function " + node->name + ". Must be subquery or set of values.",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		DataTypes set_element_types;
		ASTPtr & left_arg = args.children[0];

		ASTFunction * left_arg_tuple = dynamic_cast<ASTFunction *>(&*left_arg);

		if (left_arg_tuple && left_arg_tuple->name == "tuple")
		{
			for (ASTs::const_iterator it = left_arg_tuple->arguments->children.begin();
				it != left_arg_tuple->arguments->children.end();
				++it)
			set_element_types.push_back(sample_block.getByName((*it)->getColumnName()).type);
		}
		else
		{
			DataTypePtr left_type = sample_block.getByName(left_arg->getColumnName()).type;
			if (DataTypeArray * array_type = dynamic_cast<DataTypeArray *>(&*left_type))
				set_element_types.push_back(array_type->getNestedType());
			else
				set_element_types.push_back(left_type);
		}

		ASTSet * ast_set = new ASTSet(arg->getColumnName());
		ast_set->set = new Set(settings.limits);
		ast_set->set->create(set_element_types, set_func->arguments);
		arg = ast_set;
	}
	else if (!dynamic_cast<ASTSet *>(&*arg))
		throw Exception("Incorrect type of 2nd argument for function " + node->name + ". Must be subquery or set of values.",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
}


static std::string getUniqueName(const Block & block, const std::string & prefix)
{
	int i = 1;
	while (block.has(prefix + toString(i)))
		++i;
	return prefix + toString(i);
}


void ExpressionAnalyzer::getRootActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ExpressionActions & actions)
{
	ScopeStack scopes(actions, settings);
	getActionsImpl(ast, no_subqueries, only_consts, scopes);
	actions = *scopes.popLevel();
}


void ExpressionAnalyzer::getActionsImpl(ASTPtr ast, bool no_subqueries, bool only_consts, ScopeStack & actions_stack)
{
	/// Если результат вычисления уже есть в блоке.
	if ((dynamic_cast<ASTFunction *>(&*ast) || dynamic_cast<ASTLiteral *>(&*ast) || dynamic_cast<ASTSet *>(&*ast))
		&& actions_stack.getSampleBlock().has(ast->getColumnName()))
		return;
	
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->kind == ASTFunction::LAMBDA_EXPRESSION)
			throw Exception("Unexpected expression", ErrorCodes::UNEXPECTED_EXPRESSION);
		
		if (node->kind == ASTFunction::ARRAY_JOIN)
		{
			if (node->arguments->children.size() != 1)
				throw Exception("arrayJoin requires exactly 1 argument", ErrorCodes::TYPE_MISMATCH);
			ASTPtr arg = node->arguments->children[0];
			getActionsImpl(arg, no_subqueries, only_consts, actions_stack);
			if (!only_consts)
				actions_stack.addAction(ExpressionActions::Action::arrayJoin(arg->getColumnName(), node->getColumnName()));
			
			return;
		}
		
		if (node->kind == ASTFunction::FUNCTION)
		{
			if (node->name == "in" || node->name == "notIn")
			{
				if (!no_subqueries)
				{
					/// Найдем тип первого аргумента (потом getActionsImpl вызовется для него снова и ни на что не повлияет).
					getActionsImpl(node->arguments->children[0], no_subqueries, only_consts, actions_stack);
					/// Превратим tuple или подзапрос в множество.
					makeSet(node, actions_stack.getSampleBlock());
				}
				else
				{
					/// Мы в той части дерева, которую не собираемся вычислять. Нужно только определить типы.
					/// Не будем выполнять подзапросы и составлять множества. Вставим произвольный столбец правильного типа.
					ColumnWithNameAndType fake_column;
					fake_column.name = node->getColumnName();
					fake_column.type = new DataTypeUInt8;
					fake_column.column = new ColumnConstUInt8(1, 0);
					actions_stack.addAction(ExpressionActions::Action::addColumn(fake_column));
					getActionsImpl(node->arguments, no_subqueries, only_consts, actions_stack);
					return;
				}
			}
			
			FunctionPtr function = context.getFunctionFactory().get(node->name, context);
			
			Names argument_names;
			DataTypes argument_types;
			bool arguments_present = true;
			
			/// Если у функции есть аргумент-лямбда-выражение, нужно определить его тип до рекурсивного вызова.
			bool has_lambda_arguments = false;
			
			for (size_t i = 0; i < node->arguments->children.size(); ++i)
			{
				ASTPtr child = node->arguments->children[i];
				
				ASTFunction * lambda = dynamic_cast<ASTFunction *>(&*child);
				if (lambda && lambda->name == "lambda")
				{
					/// Если аргумент - лямбда-выражение, только запомним его примерный тип.
					if (lambda->arguments->children.size() != 2)
						throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
					
					ASTFunction * lambda_args_tuple = dynamic_cast<ASTFunction *>(&*lambda->arguments->children[0]);
					
					if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
						throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);
					
					has_lambda_arguments = true;
					argument_types.push_back(new DataTypeExpression(DataTypes(lambda_args_tuple->arguments->children.size())));
					/// Выберем название в следующем цикле.
					argument_names.push_back("");
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
					
					ASTFunction * lambda = dynamic_cast<ASTFunction *>(&*child);
					if (lambda && lambda->name == "lambda")
					{
						DataTypeExpression * lambda_type = dynamic_cast<DataTypeExpression *>(&*argument_types[i]);
						ASTFunction * lambda_args_tuple = dynamic_cast<ASTFunction *>(&*lambda->arguments->children[0]);
						ASTs lambda_arg_asts = lambda_args_tuple->arguments->children;
						NamesAndTypesList lambda_arguments;
						
						for (size_t j = 0; j < lambda_arg_asts.size(); ++j)
						{
							ASTIdentifier * identifier = dynamic_cast<ASTIdentifier *>(&*lambda_arg_asts[j]);
							if (!identifier)
								throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);
							
							String arg_name = identifier->name;
							NameAndTypePair arg(arg_name, lambda_type->getArgumentTypes()[j]);
							
							lambda_arguments.push_back(arg);
						}
						
						actions_stack.pushLevel(lambda_arguments);
						getActionsImpl(lambda->arguments->children[1], no_subqueries, only_consts, actions_stack);
						ExpressionActionsPtr lambda_actions = actions_stack.popLevel();
						
						String result_name = lambda->arguments->children[1]->getColumnName();
						lambda_actions->finalize(Names(1, result_name));
						DataTypePtr result_type = lambda_actions->getSampleBlock().getByName(result_name).type;
						argument_types[i] = new DataTypeExpression(lambda_type->getArgumentTypes(), result_type);
						
						Names captured = lambda_actions->getRequiredColumns();
						for (size_t j = 0; j < captured.size(); ++j)
						{
							if (findColumn(captured[j], lambda_arguments) == lambda_arguments.end())
								additional_requirements.push_back(captured[j]);
						}
						
						/// Не можем дать название getColumnName(),
						///  потому что оно не однозначно определяет выражение (типы аргументов могут быть разными).
						argument_names[i] = getUniqueName(actions_stack.getSampleBlock(), "__lambda");
						
						ColumnWithNameAndType lambda_column;
						lambda_column.column = new ColumnExpression(1, lambda_actions, lambda_arguments, result_type, result_name);
						lambda_column.type = argument_types[i];
						lambda_column.name = argument_names[i];
						actions_stack.addAction(ExpressionActions::Action::addColumn(lambda_column));
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
				actions_stack.addAction(ExpressionActions::Action::applyFunction(function, argument_names, node->getColumnName()),
										additional_requirements);
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		DataTypePtr type = apply_visitor(FieldToDataType(), node->value);
		ColumnWithNameAndType column;
		column.column = type->createConstColumn(1, node->value);
		column.type = type;
		column.name = node->getColumnName();
		
		actions_stack.addAction(ExpressionActions::Action::addColumn(column));
	}
	else if (ASTSet * node = dynamic_cast<ASTSet *>(&*ast))
	{
		/// Множество в секции IN.
		ColumnWithNameAndType column;
		column.column = new ColumnSet(1, node->set);
		column.type = new DataTypeSet;
		column.name = node->getColumnName();
		
		actions_stack.addAction(ExpressionActions::Action::addColumn(column));
	}
	else
	{
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			getActionsImpl(*it, no_subqueries, only_consts, actions_stack);
	}
}


void ExpressionAnalyzer::getAggregatesImpl(ASTPtr ast, ExpressionActions & actions)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
	{
		has_aggregation = true;
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
			getRootActionsImpl(arguments[i], true, false, actions);
			const std::string & name = arguments[i]->getColumnName();
			types[i] = actions.getSampleBlock().getByName(name).type;
			aggregate.argument_names[i] = name;
		}
		
		aggregate.function = context.getAggregateFunctionFactory().get(node->name, types);
		
		if (node->parameters)
		{
			ASTs & parameters = dynamic_cast<ASTExpressionList &>(*node->parameters).children;
			Row params_row(parameters.size());
			
			for (size_t i = 0; i < parameters.size(); ++i)
			{
				ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*parameters[i]);
				if (!lit)
					throw Exception("Parameters to aggregate functions must be literals", ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);
				
				params_row[i] = lit->value;
			}
			
			aggregate.function->setParameters(params_row);
		}
		
		aggregate.function->setArguments(types);
		
		aggregate_descriptions.push_back(aggregate);
	}
	else
	{
		for (size_t i = 0; i < ast->children.size(); ++i)
		{
			ASTPtr child = ast->children[i];
			if (!dynamic_cast<ASTSubquery *>(&*child) && !dynamic_cast<ASTSelectQuery *>(&*child))
				getAggregatesImpl(child, actions);
		}
	}
}

void ExpressionAnalyzer::assertSelect()
{
	if (!select_query)
		throw Exception("Not a select query", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::assertAggregation()
{
	if (!has_aggregation)
		throw Exception("No aggregation", ErrorCodes::LOGICAL_ERROR);
}

void ExpressionAnalyzer::initChain(ExpressionActionsChain & chain, NamesAndTypesList & columns)
{
	if (chain.steps.empty())
	{
		chain.settings = settings;
		chain.steps.push_back(ExpressionActionsChain::Step(new ExpressionActions(columns, settings)));
	}
}

bool ExpressionAnalyzer::appendWhere(ExpressionActionsChain & chain)
{
	assertSelect();
	
	if (!select_query->where_expression)
		return false;
	
	initChain(chain, columns);
	ExpressionActionsChain::Step & step = chain.steps.back();
	
	step.required_output.push_back(select_query->where_expression->getColumnName());
	getRootActionsImpl(select_query->where_expression, false, false, *step.actions);
	
	return true;
}

bool ExpressionAnalyzer::appendGroupBy(ExpressionActionsChain & chain)
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
		getRootActionsImpl(asts[i], false, false, *step.actions);
	}
	
	return true;
}

void ExpressionAnalyzer::appendAggregateFunctionsArguments(ExpressionActionsChain & chain)
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
	
	getActionsBeforeAggregationImpl(select_query->select_expression_list, *step.actions);
	
	if (select_query->having_expression)
		getActionsBeforeAggregationImpl(select_query->having_expression, *step.actions);
	
	if (select_query->order_expression_list)
		getActionsBeforeAggregationImpl(select_query->order_expression_list, *step.actions);
}

bool ExpressionAnalyzer::appendHaving(ExpressionActionsChain & chain)
{
	assertAggregation();
	
	if (!select_query->having_expression)
		return false;
	
	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();
	
	step.required_output.push_back(select_query->having_expression->getColumnName());
	getRootActionsImpl(select_query->having_expression, false, false, *step.actions);
	
	return true;
}

void ExpressionAnalyzer::appendSelect(ExpressionActionsChain & chain)
{
	assertSelect();
	
	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();
	
	getRootActionsImpl(select_query->select_expression_list, false, false, *step.actions);
	
	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		step.required_output.push_back(asts[i]->getColumnName());
	}
}

bool ExpressionAnalyzer::appendOrderBy(ExpressionActionsChain & chain)
{
	assertSelect();
	
	if (!select_query->order_expression_list)
		return false;
	
	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();
	
	getRootActionsImpl(select_query->order_expression_list, false, false, *step.actions);
	
	ASTs asts = select_query->order_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		ASTOrderByElement * ast = dynamic_cast<ASTOrderByElement *>(&*asts[i]);
		if (!ast || ast->children.size() != 1)
			throw Exception("Bad order expression AST", ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
		ASTPtr order_expression = ast->children[0];
		step.required_output.push_back(order_expression->getColumnName());
	}
	
	return true;
}

void ExpressionAnalyzer::appendProjectResult(DB::ExpressionActionsChain & chain)
{
	assertSelect();
	
	initChain(chain, aggregated_columns);
	ExpressionActionsChain::Step & step = chain.steps.back();
	
	NamesWithAliases result_columns;
	
	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), asts[i]->getAlias()));
		step.required_output.push_back(result_columns.back().second);
	}
	
	step.actions->add(ExpressionActions::Action::project(result_columns));
}


Block ExpressionAnalyzer::getSelectSampleBlock()
{
	assertSelect();
	
	ExpressionActions temp_actions(aggregated_columns, settings);
	NamesWithAliases result_columns;
	
	ASTs asts = select_query->select_expression_list->children;
	for (size_t i = 0; i < asts.size(); ++i)
	{
		result_columns.push_back(NameWithAlias(asts[i]->getColumnName(), asts[i]->getAlias()));
		getRootActionsImpl(asts[i], true, false, temp_actions);
	}
	
	temp_actions.add(ExpressionActions::Action::project(result_columns));
	
	return temp_actions.getSampleBlock();
}

void ExpressionAnalyzer::getActionsBeforeAggregationImpl(ASTPtr ast, ExpressionActions & actions)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	if (node && node->kind == ASTFunction::AGGREGATE_FUNCTION)
	{
		ASTs & arguments = node->arguments->children;
		
		for (size_t i = 0; i < arguments.size(); ++i)
		{
			getRootActionsImpl(arguments[i], false, false, actions);
		}
	}
	else
	{
		for (size_t i = 0; i < ast->children.size(); ++i)
		{
			getActionsBeforeAggregationImpl(ast->children[i], actions);
		}
	}
}


ExpressionActionsPtr ExpressionAnalyzer::getActions(bool project_result)
{
	ExpressionActionsPtr actions = new ExpressionActions(columns, settings);
	NamesWithAliases result_columns;
	Names result_names;
	
	ASTs asts;
	
	if (ASTExpressionList * node = dynamic_cast<ASTExpressionList *>(&*ast))
		asts = node->children;
	else
		asts = ASTs(1, ast);
	
	for (size_t i = 0; i < asts.size(); ++i)
	{
		std::string name = asts[i]->getColumnName();
		std::string alias;
		if (project_result)
			alias = asts[i]->getAlias();
		else
			alias = name;
		result_columns.push_back(NameWithAlias(name, alias));
		result_names.push_back(alias);
		getRootActionsImpl(asts[i], false, false, *actions);
	}
	
	if (project_result)
	{
		actions->add(ExpressionActions::Action::project(result_columns));
	}
	else
	{
		/// Не будем удалять исходные столбцы.
		for (NamesAndTypesList::const_iterator it = columns.begin(); it != columns.end(); ++it)
			result_names.push_back(it->first);
	}
	
	actions->finalize(result_names);
	
	return actions;
}


ExpressionActionsPtr ExpressionAnalyzer::getConstActions()
{
	ExpressionActionsPtr actions = new ExpressionActions(NamesAndTypesList(), settings);
	
	getRootActionsImpl(ast, true, true, *actions);
	
	return actions;
}

void ExpressionAnalyzer::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates)
{
	for (NamesAndTypesList::iterator it = aggregation_keys.begin(); it != aggregation_keys.end(); ++it)
		key_names.push_back(it->first);
	aggregates = aggregate_descriptions;
}

void ExpressionAnalyzer::removeUnusedColumns()
{
	NamesSet required;
	NamesSet ignored;
	getRequiredColumnsImpl(ast, required, ignored);
	
	/// Нужно прочитать хоть один столбец, чтобы узнать количество строк.
	if (required.empty())
		required.insert(ExpressionActions::getSmallestColumn(columns));
	
	unknown_required_columns = required;
	
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end();)
	{
		NamesAndTypesList::iterator it0 = it;
		++it;
		
		unknown_required_columns.erase(it0->first);
		
		if (!required.count(it0->first))
		{
			required.erase(it0->first);
			columns.erase(it0);
		}
	}
}

Names ExpressionAnalyzer::getRequiredColumns()
{
	if (!unknown_required_columns.empty())
		throw Exception("Unknown identifier: " + *unknown_required_columns.begin(), ErrorCodes::UNKNOWN_IDENTIFIER);
	
	Names res;
	for (NamesAndTypesList::iterator it = columns.begin(); it != columns.end(); ++it)
		res.push_back(it->first);
	return res;
}

void ExpressionAnalyzer::getRequiredColumnsImpl(ASTPtr ast, NamesSet & required_columns, NamesSet & ignored_names)
{
	if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column && !ignored_names.count(node->name))
			required_columns.insert(node->name);
		return;
	}
	
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->kind == ASTFunction::LAMBDA_EXPRESSION)
		{
			if (node->arguments->children.size() != 2)
				throw Exception("lambda requires two arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			ASTFunction * lambda_args_tuple = dynamic_cast<ASTFunction *>(&*node->arguments->children[0]);
			
			if (!lambda_args_tuple || lambda_args_tuple->name != "tuple")
				throw Exception("First argument of lambda must be a tuple", ErrorCodes::TYPE_MISMATCH);
			
			/// Не нужно добавлять параметры лямбда-выражения в required_columns.
			Names added_ignored;
			for (size_t i = 0 ; i < lambda_args_tuple->arguments->children.size(); ++i)
			{
				ASTIdentifier * identifier = dynamic_cast<ASTIdentifier *>(&*lambda_args_tuple->arguments->children[i]);
				if (!identifier)
					throw Exception("lambda argument declarations must be identifiers", ErrorCodes::TYPE_MISMATCH);
				std::string name = identifier->name;
				if (!ignored_names.count(name))
				{
					ignored_names.insert(name);
					added_ignored.push_back(name);
				}
			}
			
			getRequiredColumnsImpl(node->arguments->children[1], required_columns, ignored_names);
			
			for (size_t i = 0; i < added_ignored.size(); ++i)
				ignored_names.erase(added_ignored[i]);
			
			return;
		}
	}
	
	for (size_t i = 0; i < ast->children.size(); ++i)
	{
		ASTPtr child = ast->children[i];
		if (!dynamic_cast<ASTSubquery *>(&*child) && !dynamic_cast<ASTSelectQuery *>(&*child))
			getRequiredColumnsImpl(child, required_columns, ignored_names);
	}
}

}
