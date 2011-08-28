#include <DB/DataTypes/FieldToDataType.h>

#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTAsterisk.h>
#include <DB/Parsers/ASTExpressionList.h>

#include <DB/Interpreters/Expression.h>


namespace DB
{

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
		if (it == context.functions->end())
			throw Exception("Unknown function " + node->name, ErrorCodes::UNKNOWN_FUNCTION);

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
				argument_types.insert(argument_types.end(), arg->return_types.begin(), arg->return_types.end());
			else if (ASTIdentifier * arg = dynamic_cast<ASTIdentifier *>(&**it))
				argument_types.push_back(arg->type);
			else if (ASTLiteral * arg = dynamic_cast<ASTLiteral *>(&**it))
				argument_types.push_back(arg->type);
		}

		/// Получаем типы результата
		node->return_types = node->function->getReturnTypes(argument_types);
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
	if (ast->part_id == part_id)
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

	if (ast->calculated || ast->part_id != part_id)
		return;

	/** Столбцы из таблицы уже загружены в блок.
	  * Вычисление состоит в добавлении в блок новых столбцов - констант и результатов вычислений функций.
	  */
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		/// Вставляем в блок столбцы - результаты вычисления функции
		ColumnNumbers argument_numbers;
		ColumnNumbers & result_numbers = node->return_column_numbers;
		result_numbers.clear();
				
		size_t res_num = 0;
		for (DataTypes::const_iterator it = node->return_types.begin(); it != node->return_types.end(); ++it)
		{
			ColumnWithNameAndType column;
			column.type = *it;
			column.name = node->getTreeID() + "_" + Poco::NumberFormatter::format(res_num);

			result_numbers.push_back(block.columns());
			block.insert(column);
			++res_num;
		}

		ASTs arguments = node->arguments->children;
		for (ASTs::iterator it = arguments.begin(); it != arguments.end(); ++it)
		{
			if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&**it))
				argument_numbers.push_back(block.getPositionByName(ident->name));
			else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&**it))
				argument_numbers.insert(argument_numbers.end(), func->return_column_numbers.begin(), func->return_column_numbers.end());
			else
				argument_numbers.push_back(block.getPositionByName((*it)->getTreeID()));
		}

		node->function->execute(block, argument_numbers, result_numbers);
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		ColumnWithNameAndType column;
		column.column = node->type->createConstColumn(block.rows(), node->value);
		column.type = node->type;
		column.name = node->getTreeID();

		block.insert(column);
	}

	ast->calculated = true;
}


Block Expression::projectResult(Block & block, unsigned part_id)
{
	Block res;
	collectFinalColumns(ast, block, res, part_id);
	return res;
}


void Expression::collectFinalColumns(ASTPtr ast, Block & src, Block & dst, unsigned part_id)
{
	/// Обход в глубину, который не заходит внутрь функций.
	if (ast->part_id != part_id)
	{
		if (!dynamic_cast<ASTFunction *>(&*ast))
			for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
				collectFinalColumns(*it, src, dst, part_id);
		return;
	}

	if (ASTIdentifier * ident = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		if (ident->kind == ASTIdentifier::Column)
			dst.insert(src.getByName(ident->name));
	}
	else if (dynamic_cast<ASTLiteral *>(&*ast))
		dst.insert(src.getByName(ast->getTreeID()));
	else if (ASTFunction * func = dynamic_cast<ASTFunction *>(&*ast))
		for (ColumnNumbers::const_iterator jt = func->return_column_numbers.begin(); jt != func->return_column_numbers.end(); ++jt)
			dst.insert(src.getByPosition(*jt));
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			collectFinalColumns(*it, src, dst, part_id);
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
		res.insert(res.end(), func->return_types.begin(), func->return_types.end());
	else
		for (ASTs::iterator it = ast->children.begin(); it != ast->children.end(); ++it)
			getReturnTypesImpl(*it, res);
}

	
}
