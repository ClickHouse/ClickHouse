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


static std::string getName(ASTPtr & ast)
{
	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
		return ident->name;
	else
		return ast->getTreeID();
}


void Expression::addSemantic(ASTPtr & ast)
{
	if (dynamic_cast<ASTAsterisk *>(&*ast))
	{
		ASTExpressionList * all_columns = new ASTExpressionList(ast->range);
		for (NamesAndTypes::const_iterator it = context.columns.begin(); it != context.columns.end(); ++it)
			all_columns->children.push_back(new ASTIdentifier(ast->range, it->first));
		ast = all_columns;
	}
	else if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		Functions::const_iterator it = context.functions->find(node->name);
		node->aggregate_function = context.aggregate_function_factory->tryGet(node->name);
		if (it == context.functions->end() && node->aggregate_function.isNull())
			throw Exception("Unknown function " + node->name, ErrorCodes::UNKNOWN_FUNCTION);
		if (it != context.functions->end())
			node->function = it->second;
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (node->kind == ASTIdentifier::Column)
		{
			NamesAndTypes::const_iterator it = context.columns.find(node->name);
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

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		addSemantic(*it);
}


void Expression::checkTypes(ASTPtr ast)
{
	/// Обход в глубину
	
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		checkTypes(*it);

	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
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


void Expression::setNotCalculated(ASTPtr ast, unsigned part_id)
{
	if ((ast->part_id & part_id) || (ast->part_id == 0 && part_id == 0))
		ast->calculated = false;
	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		setNotCalculated(*it, part_id);
}


void Expression::execute(Block & block, unsigned part_id)
{
	setNotCalculated(ast, part_id);
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
			column.name = getName(ast);

			size_t result_number = block.columns();
			block.insert(column);

			ASTs arguments = node->arguments->children;
			for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
				argument_numbers.push_back(block.getPositionByName(getName(*it)));

			node->function->execute(block, argument_numbers, result_number);
		}
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		ColumnWithNameAndType column;
		column.column = node->type->createConstColumn(block.rows(), node->value);
		column.type = node->type;
		column.name = getName(ast);

		block.insert(column);
	}

	ast->calculated = true;
}


Block Expression::projectResult(Block & block, bool without_duplicates, unsigned part_id)
{
	Block res;
	collectFinalColumns(ast, block, res, without_duplicates, part_id);
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
			without_duplicates ? dst.insertUnique(src.getByName(getName(ast))) : dst.insert(src.getByName(getName(ast)));
	}
	else if (dynamic_cast<ASTLiteral *>(&*ast) || dynamic_cast<ASTFunction *>(&*ast))
		without_duplicates ? dst.insertUnique(src.getByName(getName(ast))) : dst.insert(src.getByName(getName(ast)));
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


void Expression::getAggregateInfoImpl(ASTPtr ast, Names & key_names, AggregateDescriptions & aggregates)
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
						key_names.push_back(getName(*it));
				}
				else if (dynamic_cast<ASTLiteral *>(&**it) || dynamic_cast<ASTFunction *>(&**it))
					key_names.push_back(getName(*it));
			}
		}
	}

	if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
	{
		if (func->aggregate_function)
		{
			AggregateDescription desc;
			desc.function = func->aggregate_function;

			for (ASTs::iterator it = func->arguments->children.begin(); it != func->arguments->children.end(); ++it)
				desc.argument_names.push_back(getName(*it));

			aggregates.push_back(desc);
		}
	}

	for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
		getAggregateInfoImpl(*it, key_names, aggregates);
}


void Expression::getAggregateInfo(Names & key_names, AggregateDescriptions & aggregates)
{
	getAggregateInfoImpl(ast, key_names, aggregates);
}

	
}
