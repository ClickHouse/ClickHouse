#include <DB/DataTypes/FieldToDataType.h>

#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTAsterisk.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTSelectQuery.h>

#include <DB/Interpreters/Expression.h>


namespace DB
{


void Expression::addSemantic(ASTPtr & ast)
{
	/// Обход в глубину
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		addSemantic(*it);
	
	if (dynamic_cast<ASTAsterisk *>(&*ast))
	{
		ASTExpressionList * all_columns = new ASTExpressionList(ast->range);
		for (NamesAndTypesMap::const_iterator it = context.columns.begin(); it != context.columns.end(); ++it)
			all_columns->children.push_back(new ASTIdentifier(ast->range, it->first));
		ast = all_columns;

		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			addSemantic(*it);
	}
	else if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		/** Нет ли в таблице столбца, название которого полностью совпадает с записью функции?
		  * Например, в таблице есть столбец "domain(URL)", и мы запросили domain(URL).
		  */
		String function_string = node->getColumnName();
		NamesAndTypesMap::const_iterator it = context.columns.find(function_string);
		if (context.columns.end() != it)
		{
			ASTIdentifier * ast_id = new ASTIdentifier(node->range, node->name);
			ast_id->type = it->second;
			required_columns.insert(function_string);
			ast = ast_id;
		}
		else
		{
			Functions::const_iterator it = context.functions->find(node->name);

			/// Типы аргументов
			DataTypes argument_types;
			ASTs & arguments = dynamic_cast<ASTExpressionList &>(*node->arguments).children;

			for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
			{
				if (ASTFunction * arg = dynamic_cast<ASTFunction *>(&**it))
					argument_types.push_back(arg->return_type);
				else if (ASTIdentifier * arg = dynamic_cast<ASTIdentifier *>(&**it))
					argument_types.push_back(arg->type);
				else if (ASTLiteral * arg = dynamic_cast<ASTLiteral *>(&**it))
					argument_types.push_back(arg->type);
			}

			node->aggregate_function = context.aggregate_function_factory->tryGet(node->name, argument_types);
			if (it == context.functions->end() && node->aggregate_function.isNull())
				throw Exception("Unknown function " + node->name, ErrorCodes::UNKNOWN_FUNCTION);
			if (it != context.functions->end())
				node->function = it->second;

			/// Получаем типы результата
			if (node->aggregate_function)
			{
				node->aggregate_function->setArguments(argument_types);
				node->return_type = node->aggregate_function->getReturnType();
			}
			else
				node->return_type = node->function->getReturnType(argument_types);
		}
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			NamesAndTypesMap::const_iterator it = context.columns.find(node->name);
			if (it == context.columns.end())
				throw Exception("Unknown identifier " + node->name, ErrorCodes::UNKNOWN_IDENTIFIER);

			node->type = it->second;
			required_columns.insert(node->name);
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		node->type = boost::apply_visitor(FieldToDataType(), node->value);
	}
}


void Expression::glueTree(ASTPtr ast)
{
	Subtrees subtrees;
	glueTreeImpl(ast, subtrees);
}


void Expression::glueTreeImpl(ASTPtr ast, Subtrees & subtrees)
{
	/// Обход в глубину

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
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


void Expression::setNotCalculated(unsigned part_id, ASTPtr subtree)
{
	if (!subtree)
		subtree = ast;

	subtree->calculated = false;
	
	for (ASTs::iterator it = subtree->children.begin(); it != subtree->children.end(); ++it)
		setNotCalculated(part_id, *it);
}


void Expression::execute(Block & block, unsigned part_id)
{
	executeImpl(ast, block, part_id);
}


void Expression::executeImpl(ASTPtr ast, Block & block, unsigned part_id)
{
	/// Обход в глубину

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		executeImpl(*it, block, part_id);

	if (ast->calculated || !((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
		return;

	/** Столбцы из таблицы уже загружены в блок.
	  * Вычисление состоит в добавлении в блок новых столбцов - констант и результатов вычислений функций.
	  */
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (node->function)
		{
			/// Вставляем в блок столбцы - результаты вычисления функции
			ColumnNumbers argument_numbers;

			ColumnWithNameAndType column;
			column.type = node->return_type;
			column.name = node->getColumnName();

			size_t result_number = block.columns();
			block.insert(column);

			ASTs arguments = node->arguments->children;
			for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
				argument_numbers.push_back(block.getPositionByName((*it)->getColumnName()));

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

	ast->calculated = true;
}


Block Expression::projectResult(Block & block, bool without_duplicates, unsigned part_id, ASTPtr subtree)
{
	Block res;
	collectFinalColumns(subtree ? subtree : ast, block, res, without_duplicates, part_id);
	return res;
}


void Expression::collectFinalColumns(ASTPtr ast, Block & src, Block & dst, bool without_duplicates, unsigned part_id)
{
	/// Обход в глубину, который не заходит внутрь функций.
	if (!((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0)))
	{
		if (!dynamic_cast<ASTFunction *>(&*ast))
			for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
				collectFinalColumns(*it, src, dst, without_duplicates, part_id);
		return;
	}

	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (ident->kind == ASTIdentifier::Column)
			without_duplicates ? dst.insertUnique(src.getByName(ast->getColumnName())) : dst.insert(src.getByName(ast->getColumnName()));
	}
	else if (dynamic_cast<ASTLiteral *>(&*ast) || dynamic_cast<ASTFunction *>(&*ast))
		without_duplicates ? dst.insertUnique(src.getByName(ast->getColumnName())) : dst.insert(src.getByName(ast->getColumnName()));
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			collectFinalColumns(*it, src, dst, without_duplicates, part_id);
}


DataTypes Expression::getReturnTypes()
{
	DataTypes res;
	getReturnTypesImpl(ast, res);
	return res;
}


void Expression::getReturnTypesImpl(ASTPtr ast, DataTypes & res)
{
	/// Обход в глубину, который не заходит внутрь функций.
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

	/// Обход в глубину, который не заходит внутрь функций.
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
		col.name = lit->getColumnName();
		col.type = lit->type;
		res.insert(col);
	}
	else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
	{
		col.name = func->getColumnName();
		col.type = func->return_type;
		res.insert(col);
	}
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			getSampleBlockImpl(*it, res);
}


void Expression::getAggregateInfoImpl(ASTPtr ast, Names & key_names, AggregateDescriptions & aggregates, NamesSet & processed)
{
	/// Обход в глубину
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

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
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
		if (hasAggregatesImpl(*it))
			return true;
		
	return false;
}


bool Expression::hasAggregates()
{
	return hasAggregatesImpl(ast);
}


void Expression::markBeforeAndAfterAggregationImpl(ASTPtr ast, unsigned before_part_id, unsigned after_part_id, bool below)
{
	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		if (func->aggregate_function)
			below = true;
	
	if (below)
		ast->part_id |= before_part_id;
	else
		ast->part_id |= after_part_id;

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		markBeforeAndAfterAggregationImpl(*it, before_part_id, after_part_id, below);
}


void Expression::markBeforeAndAfterAggregation(unsigned before_part_id, unsigned after_part_id)
{
	markBeforeAndAfterAggregationImpl(ast, before_part_id, after_part_id);
}

}
