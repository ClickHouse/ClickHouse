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
#include <DB/Functions/FunctionsMiscellaneous.h>
#include <DB/Columns/ColumnSet.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/Expression.h>

#include <DB/Storages/StorageMergeTree.h>


namespace DB
{


NamesAndTypesList::const_iterator Expression::findColumn(const String & name)
{
	NamesAndTypesList::const_iterator it;
	for (it = columns.begin(); it != columns.end(); ++it)
		if (it->first == name)
			break;
	return it;
}


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


void Expression::createAliasesDict(ASTPtr & ast)
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


StoragePtr Expression::getTable()
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


bool Expression::needSignRewrite()
{
	if (settings.sign_rewrite && storage)
		if (const StorageMergeTree * merge_tree = dynamic_cast<const StorageMergeTree *>(&*storage))
			return merge_tree->getName() == "CollapsingMergeTree";
	return false;
}


String Expression::getSignColumnName()
{
	return dynamic_cast<const StorageMergeTree *>(&*storage)->getSignColumnName();
}


DataTypes Expression::getArgumentTypes(const ASTExpressionList & exp_list)
{
	DataTypes argument_types;
	for (ASTs::const_iterator it = exp_list.children.begin(); it != exp_list.children.end(); ++it)
		argument_types.push_back(getType(*it));
	return argument_types;
}


ASTPtr Expression::createSignColumn()
{
	ASTIdentifier * p_sign_column = new ASTIdentifier(ast->range, sign_column_name);
	ASTIdentifier & sign_column = *p_sign_column;
	ASTPtr sign_column_node = p_sign_column;
	sign_column.name = sign_column_name;
	sign_column.type = storage->getDataTypeByName(sign_column_name);
	return sign_column_node;
}


ASTPtr Expression::rewriteCount(const ASTFunction * node)
{
	DataTypePtr sign_type = storage->getDataTypeByName(sign_column_name);
	DataTypes argument_types(1, sign_type);
	
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
	sum.aggregate_function = context.getAggregateFunctionFactory().get(sum.name, argument_types);
	sum.return_type = sum.aggregate_function->getReturnType();
	
	required_columns.insert(sign_column_name);
	
	return sum_node;
}


ASTPtr Expression::rewriteSum(const ASTFunction * node)
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
	mult.function = context.getFunctionFactory().get(mult.name, context);
	mult.arguments = mult_exp_list_node;
	mult.children.push_back(mult_exp_list_node);
	mult.return_type = mult.function->getReturnType(getArgumentTypes(mult_exp_list));
	
	/// 'x * Sign'
	ASTExpressionList * p_exp_list = new ASTExpressionList;
	ASTExpressionList & exp_list = *p_exp_list;
	ASTPtr exp_list_node = p_exp_list;
	exp_list.children.push_back(mult_node);
	
	DataTypes argument_types(1, mult.return_type);
	
	/// sum(x * Sign)
	ASTFunction * p_sum = new ASTFunction;
	ASTFunction & sum = *p_sum;
	ASTPtr sum_node = p_sum;
	sum.name = "sum";
	sum.alias = node->alias;
	sum.arguments = exp_list_node;
	sum.children.push_back(exp_list_node);	
	sum.aggregate_function = context.getAggregateFunctionFactory().get(sum.name, argument_types);
	sum.return_type = sum.aggregate_function->getReturnType();
	
	required_columns.insert(sign_column_name);
	
	return sum_node;
}


ASTPtr Expression::rewriteAvg(const ASTFunction * node)
{
	/// 'sum(Sign * x)', 'sum(Sign)'
	ASTExpressionList * p_div_exp_list = new ASTExpressionList;
	ASTExpressionList & div_exp_list = *p_div_exp_list;
	ASTPtr div_exp_list_node = p_div_exp_list;
	div_exp_list.children.push_back(rewriteSum(node));
	div_exp_list.children.push_back(rewriteCount(node));	
	
	/// sum(Sign * x) / sum(Sign)
	ASTFunction * p_div = new ASTFunction;
	ASTFunction & div = *p_div;
	ASTPtr div_node = p_div;
	div.name = "divide";
	div.alias = node->alias;
	div.function = context.getFunctionFactory().get(div.name, context);
	div.arguments = div_exp_list_node;
	div.children.push_back(div_exp_list_node);
	div.return_type = div.function->getReturnType(getArgumentTypes(div_exp_list));
	
	required_columns.insert(sign_column_name);
	
	return div_node;
}


void Expression::considerSignRewrite(ASTPtr & ast)
{
	ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast);
	const String & name = node->aggregate_function->getName();
	if (name == "count")
		ast = rewriteCount(node);
	if (name == "sum")
		ast = rewriteSum(node);
	if (name == "avg")
		ast = rewriteAvg(node);
}


void Expression::addSemantic(ASTPtr & ast)
{
	SetOfASTs tmp_set;
	MapOfASTs tmp_map;
	if (needSignRewrite())
		sign_column_name = getSignColumnName();
	addSemanticImpl(ast,tmp_map, tmp_set);
}


/// finished_asts - уже обработанные вершины (и на что они заменены)
/// current_asts - вершины, в текущем стеке вызовов этого метода
void Expression::addSemanticImpl(ASTPtr & ast, MapOfASTs & finished_asts, SetOfASTs & current_asts)
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
			required_columns.insert(function_string);
			ast = ast_id;
			current_asts.insert(ast);
		}
	}
	
	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			addSemanticImpl(*it, finished_asts, current_asts);
	
	/// Если секция WHERE или HAVING состоит одного алиаса, ссылку нужно заменить не только в children, но и в where_expression и having_expression.
	if (ASTSelectQuery * select = dynamic_cast<ASTSelectQuery *>(&*ast))
	{
		if (select->where_expression)
			addSemanticImpl(select->where_expression, finished_asts, current_asts);
		if (select->having_expression)
			addSemanticImpl(select->having_expression, finished_asts, current_asts);
	}
	
	if (dynamic_cast<ASTAsterisk *>(&*ast))
	{
		ASTExpressionList * all_columns = new ASTExpressionList(ast->range);
		for (NamesAndTypesList::const_iterator it = columns.begin(); it != columns.end(); ++it)
			all_columns->children.push_back(new ASTIdentifier(ast->range, it->first));
		ast = all_columns;
		current_asts.insert(ast);

		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			addSemanticImpl(*it, finished_asts, current_asts);
	}
	else if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		/// Типы аргументов
		DataTypes argument_types;
		ASTs & arguments = dynamic_cast<ASTExpressionList &>(*node->arguments).children;

		for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
			argument_types.push_back(getType(*it));

		node->aggregate_function = context.getAggregateFunctionFactory().tryGet(node->name, argument_types);
		if (node->aggregate_function.isNull())
			node->function = context.getFunctionFactory().get(node->name, context);

		/// Получаем типы результата
		if (node->aggregate_function)
		{
			/// Также устанавливаем параметры агрегатной функции, если есть. Поддерживаются только литералы, пока без constant folding-а.
			if (node->parameters)
			{
				ASTs & parameters = dynamic_cast<ASTExpressionList &>(*node->parameters).children;
				size_t params_size = parameters.size();
				Row params_row(params_size);

				for (size_t i = 0; i < params_size; ++i)
				{
					ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*parameters[i]);
					if (!lit)
						throw Exception("Parameters to aggregate functions must be literals", ErrorCodes::PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS);
					
					params_row[i] = lit->value;
				}

				node->aggregate_function->setParameters(params_row);
			}
			
			node->aggregate_function->setArguments(argument_types);
			node->return_type = node->aggregate_function->getReturnType();
			
			if (!sign_column_name.empty())
				considerSignRewrite(ast);
		}
		else if (FunctionTupleElement * func_tuple_elem = dynamic_cast<FunctionTupleElement *>(&*node->function))
		{
			/// Особый случай - для функции tupleElement обычный метод getReturnType не работает.
			if (arguments.size() != 2)
				throw Exception("Function tupleElement requires exactly two arguments: tuple and element index.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			node->return_type = func_tuple_elem->getReturnType(argument_types, safeGet<UInt64>(dynamic_cast<ASTLiteral &>(*arguments[1]).value));
		}
		else
			node->return_type = node->function->getReturnType(argument_types);
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			NamesAndTypesList::const_iterator it = findColumn(node->name);
			if (it == columns.end())
			{
				/// Если это алиас
				Aliases::const_iterator jt = aliases.find(node->name);
				if (jt == aliases.end())
					throw Exception("Unknown identifier " + node->name, ErrorCodes::UNKNOWN_IDENTIFIER);

				/// Заменим его на соответствующий узел дерева
				ast = jt->second;

				addSemanticImpl(ast, finished_asts, current_asts);
			}
			else
			{
				node->type = it->second;
				required_columns.insert(node->name);
			}
			
			/// Проверим имеет ли смысл sign-rewrite
			if (node->name == sign_column_name)
				throw Exception("Requested Sign column while sign-rewrite is on.", ErrorCodes::QUERY_SECTION_DOESNT_MAKE_SENSE);
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		node->type = apply_visitor(FieldToDataType(), node->value);
	}
	
	current_asts.erase(initial_ast);
	current_asts.erase(ast);
	finished_asts[initial_ast] = ast;
}


DataTypePtr Expression::getType(ASTPtr ast)
{
	if (ASTFunction * arg = dynamic_cast<ASTFunction *>(&*ast))
		return arg->return_type;
	else if (ASTIdentifier * arg = dynamic_cast<ASTIdentifier *>(&*ast))
		return arg->type;
	else if (ASTLiteral * arg = dynamic_cast<ASTLiteral *>(&*ast))
		return arg->type;
	else if (dynamic_cast<ASTSubquery *>(&*ast) || dynamic_cast<ASTSet *>(&*ast))
		return new DataTypeSet;
	else
		throw Exception("Unknown type of AST node " + ast->getID(), ErrorCodes::UNKNOWN_TYPE_OF_AST_NODE);
}


void Expression::glueTree(ASTPtr ast)
{
	Subtrees subtrees;
	glueTreeImpl(ast, subtrees);
}


void Expression::glueTreeImpl(ASTPtr ast, Subtrees & subtrees)
{
	/// Обход в глубину. Не опускаемся в подзапросы.

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			glueTreeImpl(*it, subtrees);

	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		String tree_id = node->arguments->getTreeID();
		if (subtrees.end() == subtrees.find(tree_id))
			subtrees[tree_id] = node->arguments;
		else
			node->arguments = subtrees[tree_id];
	}
	else if (ASTExpressionList * node = dynamic_cast<ASTExpressionList *>(&*ast))
	{
		for (ASTs::iterator it = node->children.begin(); it != node->children.end(); ++it)
		{
			String tree_id = (*it)->getTreeID();
			if (subtrees.end() == subtrees.find(tree_id))
			{
				subtrees[tree_id] = *it;
			}
			else
			{
				/// Перед заменой поддерева запомним алиас.
				std::string * alias_ptr = GetAlias(*it);
				std::string initial_alias;
				if (alias_ptr)
					initial_alias = *alias_ptr;
				
				*it = subtrees[tree_id];
				
				/// Объединим два алиаса.
				alias_ptr = GetAlias(*it);
				if (alias_ptr)
				{
					if (alias_ptr->empty())
					{
						*alias_ptr = initial_alias;
					}
					else if (!initial_alias.empty() && initial_alias != *alias_ptr)
					{
						throw Exception("Multiple aliases " + initial_alias + " and " + *alias_ptr + " for the same expression", ErrorCodes::MULTIPLE_ALIASES_FOR_EXPRESSION);
					}
				}
			}
		}
	}
}


Names Expression::getRequiredColumns()
{
	Names res;
	res.reserve(required_columns.size());
	for (NamesSet::const_iterator it = required_columns.begin(); it != required_columns.end(); ++it)
		res.push_back(*it);
	return res;
}


void Expression::execute(Block & block, unsigned part_id, bool only_consts)
{
	executeImpl(ast, block, part_id, only_consts);
}


String Expression::getExecutionID(unsigned part_id) const
{
	std::stringstream res;
	NamesSet known_names;
	getExecutionIDImpl(ast, part_id, res, known_names);
	return res.str();
}


void Expression::clearTemporaries(Block & block)
{
	clearTemporariesImpl(ast, block);
}


void Expression::clearTemporariesImpl(ASTPtr ast, Block & block)
{
	/** Очистку ненужных столбцов будем делать так:
	  * - будем собирать нужные столбцы:
	  * - пройдём по выражению;
	  * - корневые столбцы считаем нужными;
	  * - если столбца ещё нет в блоке, то считаем его нужным, и всех его детей тоже.
	  */

	NeedColumns need_columns;
	collectNeedColumns(ast, block, need_columns);

	Block cleared_block;

	for (size_t i = 0, columns = block.columns(); i < columns; ++i)
		if (need_columns.end() != need_columns.find(block.getByPosition(i).name))
			cleared_block.insert(block.getByPosition(i));

	/** Список нужных столбцов может оказаться пустым, например, в случае запроса SELECT count() FROM t.
	  * Но, при выполнении такого запроса, из таблицы считывается первый попавшийся столбец,
	  *  чтобы узнать количество строк, и он нужен, хотя не виден в запросе.
	  * То есть, в таких случаях, удалять "ненужные" столбцы не нужно.
	  */
	if (cleared_block)
		block = cleared_block;
}


void Expression::collectNeedColumns(ASTPtr ast, Block & block, NeedColumns & need_columns, bool top_level, bool all_children_need)
{
	bool is_column =
		dynamic_cast<ASTIdentifier *>(&*ast)
		|| dynamic_cast<ASTLiteral *>(&*ast)
		|| dynamic_cast<ASTFunction *>(&*ast)
		|| dynamic_cast<ASTSet *>(&*ast);

	bool need_this_column = false;
		
	if (is_column)
	{
		if (all_children_need)
		{
			need_this_column = true;
		}
		else if (!block.has(ast->getColumnName()))
		{
			/// Если столбца ещё нет в блоке, то считаем его нужным, и всех его детей тоже.
			need_this_column = true;
			all_children_need = true;
		}
		else if (top_level)
		{
			/// Корневые столбцы считаем нужными.
			need_this_column = true;
			top_level = false;
		}
	}

	if (need_this_column)
		need_columns.insert(ast->getColumnName());

	/// Обход сверху-вниз. Не опускаемся в подзапросы.
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			collectNeedColumns(*it, block, need_columns, top_level, all_children_need);
}


void Expression::executeImpl(ASTPtr ast, Block & block, unsigned part_id, bool only_consts)
{
	/// Если результат вычисления уже есть в блоке.
	if ((dynamic_cast<ASTFunction *>(&*ast) || dynamic_cast<ASTLiteral *>(&*ast)) && block.has(ast->getColumnName()))
		return;

	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			executeImpl(*it, block, part_id, only_consts);

	/// Если это - не указанная часть дерева.
	if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
		return;

	/** Столбцы из таблицы уже загружены в блок (если не указано only_consts).
	  * Вычисление состоит в добавлении в блок новых столбцов - констант и результатов вычислений функций.
	  */
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->function)
		{
			/// Вставляем в блок столбцы - результаты вычисления функции
			ColumnNumbers argument_numbers;

			ASTs arguments = node->arguments->children;
			for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
			{
				String column_name = (*it)->getColumnName();
				if (only_consts && (!block.has(column_name) || !block.getByName(column_name).column->isConst()))
					return;

				argument_numbers.push_back(block.getPositionByName(column_name));
			}

			ColumnWithNameAndType column;
			column.type = node->return_type;
			column.name = node->getColumnName();

			size_t result_number = block.columns();
			block.insert(column);

			node->function->execute(block, argument_numbers, result_number);
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		ColumnWithNameAndType column;
		column.column = node->type->createConstColumn(block.rows(), node->value);
		column.type = node->type;
		column.name = node->getColumnName();

		block.insert(column);
	}
	else if (ASTSet * node = dynamic_cast<ASTSet *>(&*ast))
	{
		/// Множество в секции IN.
		ColumnWithNameAndType column;
		column.column = new ColumnSet(block.rows(), node->set);
		column.type = new DataTypeSet;
		column.name = node->getColumnName();

		block.insert(column);
	}

	/// Проверка ограничений на максимальное количество столбцов.

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


void Expression::getExecutionIDImpl(ASTPtr ast, unsigned part_id, std::stringstream & res, NamesSet & known_names) const
{
	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	for (ASTs::const_iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<const ASTSelectQuery *>(&**it))
			getExecutionIDImpl(*it, part_id, res, known_names);

	/// Если это - не указанная часть дерева.
	if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
		return;

	if (dynamic_cast<ASTFunction *>(&*ast)
		|| dynamic_cast<ASTLiteral *>(&*ast)
		|| dynamic_cast<ASTSet *>(&*ast))
	{
		String name = ast->getColumnName();
		if (known_names.end() == known_names.find(name))
		{
			res << (known_names.empty() ? "" : ", ") << name;
			known_names.insert(name);
		}
	}
}


Block Expression::projectResult(Block & block, bool without_duplicates_and_aliases, unsigned part_id, ASTPtr subtree)
{
	Block res;
	collectFinalColumns(subtree ? subtree : ast, block, res, without_duplicates_and_aliases, part_id);
	return res;
}


String Expression::getProjectionID(bool without_duplicates_and_aliases, unsigned part_id, ASTPtr subtree) const
{
	std::stringstream res;
	NamesSet known_names;
	getProjectionIDImpl(subtree ? subtree : ast, without_duplicates_and_aliases, part_id, res, known_names);
	return res.str();
}


void Expression::collectFinalColumns(ASTPtr ast, Block & src, Block & dst, bool without_duplicates_and_aliases, unsigned part_id)
{
	/// Обход в глубину, который не заходит внутрь функций и подзапросов.
	if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
	{
		if (!dynamic_cast<ASTFunction *>(&*ast))
			for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
				if (!dynamic_cast<ASTSelectQuery *>(&**it))
					collectFinalColumns(*it, src, dst, without_duplicates_and_aliases, part_id);
		return;
	}

	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (ident->kind == ASTIdentifier::Column)
		{
			ColumnWithNameAndType col = src.getByName(ast->getColumnName());
			col.name = without_duplicates_and_aliases ? ast->getColumnName() : ast->getAlias();
			without_duplicates_and_aliases ? dst.insertUnique(col) : dst.insert(col);
		}
	}
	else if (dynamic_cast<ASTLiteral *>(&*ast) || dynamic_cast<ASTFunction *>(&*ast))
	{
		ColumnWithNameAndType col = src.getByName(ast->getColumnName());
		col.name = without_duplicates_and_aliases ? ast->getColumnName() : ast->getAlias();
		without_duplicates_and_aliases ? dst.insertUnique(col) : dst.insert(col);
	}
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			if (!dynamic_cast<ASTSelectQuery *>(&**it))
				collectFinalColumns(*it, src, dst, without_duplicates_and_aliases, part_id);
}


void Expression::getProjectionIDImpl(ASTPtr ast, bool without_duplicates_and_aliases, unsigned part_id, std::stringstream & res, NamesSet & known_names) const
{
	/// Обход в глубину, который не заходит внутрь функций и подзапросов.
	if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
	{
		if (!dynamic_cast<ASTFunction *>(&*ast))
			for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
				if (!dynamic_cast<ASTSelectQuery *>(&**it))
					getProjectionIDImpl(*it, without_duplicates_and_aliases, part_id, res, known_names);
		return;
	}

	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (ident->kind == ASTIdentifier::Column)
		{
			String name = without_duplicates_and_aliases ? ast->getColumnName() : ast->getAlias();

			if (!without_duplicates_and_aliases || known_names.end() == known_names.find(name))
			{
				res << (known_names.empty() ? "" : ", ") << name;
				known_names.insert(name);
			}
		}
	}
	else if (dynamic_cast<ASTLiteral *>(&*ast) || dynamic_cast<ASTFunction *>(&*ast))
	{
		String name = without_duplicates_and_aliases ? ast->getColumnName() : ast->getAlias();

		if (!without_duplicates_and_aliases || known_names.end() == known_names.find(name))
		{
			res << (known_names.empty() ? "" : ", ") << name;
			known_names.insert(name);
		}
	}
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			if (!dynamic_cast<ASTSelectQuery *>(&**it))
				getProjectionIDImpl(*it, without_duplicates_and_aliases, part_id, res, known_names);
}


DataTypes Expression::getReturnTypes()
{
	DataTypes res;
	getReturnTypesImpl(ast, res);
	return res;
}


void Expression::getReturnTypesImpl(ASTPtr ast, DataTypes & res)
{
	/// Обход в глубину, который не заходит внутрь функций и подзапросов.
	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (ident->kind == ASTIdentifier::Column)
			res.push_back(ident->type);
	}
	else if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*ast))
		res.push_back(lit->type);
	else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		res.push_back(func->return_type);
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			if (!dynamic_cast<ASTSelectQuery *>(&**it))
				getReturnTypesImpl(*it, res);
}


Block Expression::getSampleBlock()
{
	Block res;
	getSampleBlockImpl(ast, res);
	return res;
}


void Expression::getSampleBlockImpl(ASTPtr ast, Block & res)
{
	ColumnWithNameAndType col;

	/// Обход в глубину, который не заходит внутрь функций и подзапросов.
	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (ident->kind == ASTIdentifier::Column)
		{
			col.name = ident->getAlias();
			col.type = ident->type;
			res.insert(col);
		}
	}
	else if (ASTLiteral * lit = dynamic_cast<ASTLiteral *>(&*ast))
	{
		col.name = lit->getAlias();
		col.type = lit->type;
		res.insert(col);
	}
	else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
	{
		col.name = func->getAlias();
		col.type = func->return_type;
		res.insert(col);
	}
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			if (!dynamic_cast<ASTSelectQuery *>(&**it))
				getSampleBlockImpl(*it, res);
}


void Expression::getAggregateInfoImpl(ASTPtr ast, Names & key_names, AggregateDescriptions & aggregates, NamesSet & processed)
{
	if (ASTSelectQuery * select = dynamic_cast<ASTSelectQuery *>(&*ast))
	{
		if (select->group_expression_list)
		{
			for (ASTs::iterator it = select->group_expression_list->children.begin(); it != select->group_expression_list->children.end(); ++it)
			{
				if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&**it))
				{
					if (ident->kind == ASTIdentifier::Column)
						key_names.push_back((*it)->getColumnName());
				}
				else if (dynamic_cast<ASTLiteral *>(&**it) || dynamic_cast<ASTFunction *>(&**it))
					key_names.push_back((*it)->getColumnName());
			}
		}
	}

	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (func->aggregate_function && processed.end() == processed.find(ast->getColumnName()))
		{
			AggregateDescription desc;
			desc.function = func->aggregate_function;
			desc.column_name = ast->getColumnName();

			for (ASTs::iterator it = func->arguments->children.begin(); it != func->arguments->children.end(); ++it)
				desc.argument_names.push_back((*it)->getColumnName());

			aggregates.push_back(desc);
			processed.insert(ast->getColumnName());
		}
	}

	/// Обход в глубину. Не опускаемся в подзапросы.
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			getAggregateInfoImpl(*it, key_names, aggregates, processed);
}


void Expression::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates)
{
	NamesSet processed;
	getAggregateInfoImpl(ast, key_names, aggregates, processed);
}


bool Expression::hasAggregatesImpl(ASTPtr ast)
{
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		if (func->aggregate_function)
			return true;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it) && hasAggregatesImpl(*it))
			return true;
		
	return false;
}


bool Expression::hasAggregates()
{
	return hasAggregatesImpl(ast);
}


void Expression::markBeforeAggregationImpl(ASTPtr ast, unsigned before_part_id, unsigned after_part_id, bool below)
{
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		if (func->aggregate_function)
			below = true;
	
	if (below)
		ast->part_id |= before_part_id;
	else
		ast->part_id |= after_part_id;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			markBeforeAggregationImpl(*it, before_part_id, after_part_id, below);
}


void Expression::markBeforeAggregation(unsigned before_part_id, unsigned after_part_id)
{
	markBeforeAggregationImpl(ast, before_part_id, after_part_id);
}


bool Expression::getArrayJoinInfoImpl(ASTPtr ast, String & column_name)
{
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (func->name == "arrayJoin")
		{
			column_name = func->arguments->children.at(0)->getColumnName();
			return true;
		}
	}

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it) && getArrayJoinInfoImpl(*it, column_name))
			return true;

	return false;
}


void Expression::markBeforeArrayJoinImpl(ASTPtr ast, unsigned part_id, bool below)
{
	if (below)
		ast->part_id |= part_id;

	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		if (func->name == "arrayJoin")
			below = true;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			markBeforeArrayJoinImpl(*it, part_id, below);
}


bool Expression::getArrayJoinInfo(String & column_name)
{
	return getArrayJoinInfoImpl(ast, column_name);
}


void Expression::markBeforeArrayJoin(unsigned part_id)
{
	markBeforeArrayJoinImpl(ast, part_id);
}


void Expression::makeSets(size_t subquery_depth, unsigned part_id)
{
	makeSetsImpl(ast, subquery_depth, part_id);
}


void Expression::makeSetsImpl(ASTPtr ast, size_t subquery_depth, unsigned part_id)
{
	bool made = false;
	
	/// Обход в глубину. Ищем выражения IN.
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (func->name == "in" || func->name == "notIn")
		{
			made = true;
			
			if (func->children.size() != 1)
				throw Exception("Function IN requires exactly 2 arguments",
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			/// У функции должно быть два аргумента.
			ASTExpressionList & args = dynamic_cast<ASTExpressionList &>(*func->children[0]);
			if (args.children.size() != 2)
				throw Exception("Function IN requires exactly 2 arguments",
					ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			/// В левом аргументе IN могут быть вложенные секции IN.
			if (!dynamic_cast<ASTSelectQuery *>(&*args.children[0]))
				makeSetsImpl(args.children[0], subquery_depth, part_id);
			
			/// Проверим, что мы в правильной части дерева.
			if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
				return;
				
			/** Нужно преобразовать правый аргумент в множество.
			  * Это может быть перечисление значений или подзапрос.
			  * Перечисление значений парсится как функция tuple.
			  */
			ASTPtr & arg = args.children[1];
			if (dynamic_cast<ASTSubquery *>(&*arg))
			{
				/// Исполняем подзапрос, превращаем результат в множество, и кладём это множество на место подзапроса.
			    InterpreterSelectQuery interpreter(arg->children[0], context, QueryProcessingStage::Complete, subquery_depth + 1);
				ASTSet * ast_set = new ASTSet(arg->getColumnName());
				ast_set->set = new Set;
				ast_set->set->create(interpreter.execute());
				arg = ast_set;
			}
			else if (ASTFunction * set_func = dynamic_cast<ASTFunction *>(&*arg))
			{
				/// Случай явного перечисления значений.
				if (set_func->name != "tuple")
					throw Exception("Incorrect type of 2nd argument for function IN. Must be subquery or set of values.",
						ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

				DataTypes set_element_types;
				ASTPtr & left_arg = args.children[0];

				ASTFunction * left_arg_tuple = dynamic_cast<ASTFunction *>(&*left_arg);

				if (left_arg_tuple && left_arg_tuple->name == "tuple")
				{
					for (ASTs::const_iterator it = left_arg_tuple->arguments->children.begin();
						it != left_arg_tuple->arguments->children.end();
						++it)
						set_element_types.push_back(getType(*it));
				}
				else
				{
					DataTypePtr left_type = getType(left_arg);
					if (DataTypeArray * array_type = dynamic_cast<DataTypeArray *>(&*left_type))
						set_element_types.push_back(array_type->getNestedType());
					else
						set_element_types.push_back(left_type);
				}

				ASTSet * ast_set = new ASTSet(arg->getColumnName());
				ast_set->set = new Set;
				ast_set->set->create(set_element_types, set_func->arguments);
				arg = ast_set;
			}
			else if (!dynamic_cast<ASTSet *>(&*arg))
				throw Exception("Incorrect type of 2nd argument for function IN. Must be subquery or set of values.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
	}

	/// Рекурсивно обходим всё дерево, не опускаясь в только что созданные множества.
	if (!made)
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			if (!dynamic_cast<ASTSelectQuery *>(&**it))
				makeSetsImpl(*it, subquery_depth, part_id);
}


void Expression::resolveScalarSubqueries(size_t subquery_depth, unsigned part_id)
{
	resolveScalarSubqueriesImpl(ast, subquery_depth, part_id);
}


void Expression::resolveScalarSubqueriesImpl(ASTPtr & ast, size_t subquery_depth, unsigned part_id)
{
	/// Обход в глубину. Ищем подзапросы.
	if (ASTSubquery * subquery = dynamic_cast<ASTSubquery *>(&*ast))
	{
		/// Проверим, что мы в правильной части дерева.
		if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
			return;
		
		/// Исполняем подзапрос, превращаем результат в множество, и кладём это множество на место подзапроса.
	    InterpreterSelectQuery interpreter(subquery->children[0], context, QueryProcessingStage::Complete, subquery_depth + 1);
		BlockInputStreamPtr res_stream = interpreter.execute();
		Block res_block = res_stream->read();

		/// В блоке должна быть одна строка.
		if (res_stream->read() || res_block.rows() != 1)
			throw Exception("Scalar subquery must return exactly one row.", ErrorCodes::INCORRECT_RESULT_OF_SCALAR_SUBQUERY);

		/// Создаём литерал или tuple.
		if (res_block.columns() == 1)
		{
			ASTLiteral * lit = new ASTLiteral(subquery->range, (*res_block.getByPosition(0).column)[0]);
			lit->type = res_block.getByPosition(0).type;
			/// NOTE можно разрешить подзапросам иметь alias.
			ast = lit;
		}
		else
		{
			ASTFunction * tuple = new ASTFunction(subquery->range);
			ASTExpressionList * args = new ASTExpressionList(subquery->range);

			size_t columns = res_block.columns();
			for (size_t i = 0; i < columns; ++i)
				args->children.push_back(new ASTLiteral(subquery->range, (*res_block.getByPosition(i).column)[0]));
			
			tuple->name = "tuple";
			tuple->arguments = args;
			tuple->children.push_back(tuple->arguments);
			ast = tuple;
			addSemantic(ast);
		}
	}
	else
	{
		/// Обходим рекурсивно, но не опускаемся в правую часть секции IN.
		bool recurse = true;

		if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		{
			if (func->name == "in" || func->name == "notIn")
			{
				recurse = false;
				
				if (func->children.size() == 1)
				{
					ASTExpressionList * args = dynamic_cast<ASTExpressionList *>(&*func->children[0]);
					if (args && args->children.size() == 2)
					{
						resolveScalarSubqueriesImpl(args->children[0], subquery_depth, part_id);
					}
				}
			}
		}
		if (recurse)
			for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
				if (!dynamic_cast<ASTSelectQuery *>(&**it))	/// А также не опускаемся в подзапросы в секции FROM.
					resolveScalarSubqueriesImpl(*it, subquery_depth, part_id);
	}
}

}
