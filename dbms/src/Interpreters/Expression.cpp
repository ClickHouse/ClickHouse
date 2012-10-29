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


namespace DB
{


NamesAndTypesList::const_iterator Expression::findColumn(const String & name)
{
	NamesAndTypesList::const_iterator it;
	for (it = context.getColumns().begin(); it != context.getColumns().end(); ++it)
		if (it->first == name)
			break;
	return it;
}


void Expression::createAliasesDict(ASTPtr & ast)
{
	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			createAliasesDict(*it);
	
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (!node->alias.empty())
			aliases[node->alias] = ast;
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (!node->alias.empty())
			aliases[node->alias] = ast;
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		if (!node->alias.empty())
			aliases[node->alias] = ast;
	}
}


void Expression::addSemantic(ASTPtr & ast)
{
	/// rewrite правила, которые действуют при обходе сверху-вниз.
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		/** Нет ли в таблице столбца, название которого полностью совпадает с записью функции?
		  * Например, в таблице есть столбец "domain(URL)", и мы запросили domain(URL).
		  */
		String function_string = node->getColumnName();
		NamesAndTypesList::const_iterator it = findColumn(function_string);
		if (context.getColumns().end() != it)
		{
			ASTIdentifier * ast_id = new ASTIdentifier(node->range, std::string(node->range.first, node->range.second));
			ast_id->type = it->second;
			required_columns.insert(function_string);
			ast = ast_id;
		}
	}
	
	/// Обход снизу-вверх. Не опускаемся в подзапросы.
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			addSemantic(*it);
	
	if (dynamic_cast<ASTAsterisk *>(&*ast))
	{
		ASTExpressionList * all_columns = new ASTExpressionList(ast->range);
		for (NamesAndTypesList::const_iterator it = context.getColumns().begin(); it != context.getColumns().end(); ++it)
			all_columns->children.push_back(new ASTIdentifier(ast->range, it->first));
		ast = all_columns;

		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			addSemantic(*it);
	}
	else if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		Functions::const_iterator it = context.getFunctions().find(node->name);

		/// Типы аргументов
		DataTypes argument_types;
		ASTs & arguments = dynamic_cast<ASTExpressionList &>(*node->arguments).children;

		for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
			argument_types.push_back(getType(*it));

		node->aggregate_function = context.getAggregateFunctionsFactory().tryGet(node->name, argument_types);
		if (it == context.getFunctions().end() && node->aggregate_function.isNull())
			throw Exception("Unknown function " + node->name, ErrorCodes::UNKNOWN_FUNCTION);
		if (it != context.getFunctions().end())
			node->function = it->second;

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
		}
		else if (FunctionTupleElement * func_tuple_elem = dynamic_cast<FunctionTupleElement *>(&*node->function))
		{
			/// Особый случай - для функции tupleElement обычный метод getReturnType не работает.
			if (arguments.size() != 2)
				throw Exception("Function tupleElement requires exactly two arguments: tuple and element index.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
			
			node->return_type = func_tuple_elem->getReturnType(argument_types, boost::get<UInt64>(dynamic_cast<ASTLiteral &>(*arguments[1]).value));
		}
		else
			node->return_type = node->function->getReturnType(argument_types);
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			NamesAndTypesList::const_iterator it = findColumn(node->name);
			if (it == context.getColumns().end())
			{
				/// Если это алиас
				Aliases::const_iterator jt = aliases.find(node->name);
				if (jt == aliases.end())
					throw Exception("Unknown identifier " + node->name, ErrorCodes::UNKNOWN_IDENTIFIER);

				/// Заменим его на соответствующий узел дерева
				ast = jt->second;

				for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
					addSemantic(*it);
			}
			else
			{
				node->type = it->second;
				required_columns.insert(node->name);
			}
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		node->type = boost::apply_visitor(FieldToDataType(), node->value);
	}
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
				subtrees[tree_id] = *it;
			else
				*it = subtrees[tree_id];
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
}


Block Expression::projectResult(Block & block, bool without_duplicates_and_aliases, unsigned part_id, ASTPtr subtree)
{
	Block res;
	collectFinalColumns(subtree ? subtree : ast, block, res, without_duplicates_and_aliases, part_id);
	return res;
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
			col.name = ident->name;
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


void Expression::markBeforeAggregationImpl(ASTPtr ast, unsigned before_part_id, bool below)
{
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		if (func->aggregate_function)
			below = true;
	
	if (below)
		ast->part_id |= before_part_id;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		if (!dynamic_cast<ASTSelectQuery *>(&**it))
			markBeforeAggregationImpl(*it, before_part_id, below);
}


void Expression::markBeforeAggregation(unsigned before_part_id)
{
	markBeforeAggregationImpl(ast, before_part_id);
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


void Expression::makeSets()
{
	makeSetsImpl(ast);
}


void Expression::makeSetsImpl(ASTPtr ast)
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
				
			/** Нужно преобразовать правый аргумент в множество.
			  * Это может быть перечисление значений или подзапрос.
			  * Перечисление значений парсится как функция tuple.
			  */
			ASTPtr & arg = args.children[1];
			if (dynamic_cast<ASTSubquery *>(&*arg))
			{
				/// Исполняем подзапрос, превращаем результат в множество, и кладём это множество на место подзапроса.
			    InterpreterSelectQuery interpreter(arg->children[0], context, QueryProcessingStage::Complete);
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

				if (ASTFunction * left_arg_tuple = dynamic_cast<ASTFunction *>(&*left_arg))
					for (ASTs::const_iterator it = left_arg_tuple->arguments->children.begin();
						it != left_arg_tuple->arguments->children.end();
						++it)
						set_element_types.push_back(getType(*it));
				else
					set_element_types.push_back(getType(left_arg));

				ASTSet * ast_set = new ASTSet(arg->getColumnName());
				ast_set->set = new Set;
				ast_set->set->create(set_element_types, set_func->arguments);
				arg = ast_set;
			}
			else
				throw Exception("Incorrect type of 2nd argument for function IN. Must be subquery or set of values.",
					ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
		}
	}

	/// Рекурсивно обходим всё дерево, не опускаясь в только что созданные множества.
	if (!made)
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			if (!dynamic_cast<ASTSelectQuery *>(&**it))
				makeSetsImpl(*it);
}


void Expression::resolveScalarSubqueries()
{
	resolveScalarSubqueriesImpl(ast);
}


void Expression::resolveScalarSubqueriesImpl(ASTPtr & ast)
{
	/// Обход в глубину. Ищем подзапросы.
	if (ASTSubquery * subquery = dynamic_cast<ASTSubquery *>(&*ast))
	{
		/// Исполняем подзапрос, превращаем результат в множество, и кладём это множество на место подзапроса.
	    InterpreterSelectQuery interpreter(subquery->children[0], context, QueryProcessingStage::Complete);
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
		/// Обходим рекурсивно, но не опускаемся в секции IN.
		bool recurse = true;

		if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
			if (func->name == "in" || func->name == "notIn")
				recurse = false;

		if (recurse)
			for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
				if (!dynamic_cast<ASTSelectQuery *>(&**it))	/// А также не опускаемся в подзапросы в секции FROM.
					resolveScalarSubqueriesImpl(*it);
	}
}

}
