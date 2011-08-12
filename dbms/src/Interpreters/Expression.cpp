#include <DB/DataTypes/FieldToDataType.h>

#include <DB/Parsers/ASTFunction.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTExpressionList.h>

#include <DB/Interpreters/Expression.h>


namespace DB
{

void Expression::addSemantic(ASTPtr ast)
{
	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		Functions::const_iterator it = context.functions.find(node->name);
		if (it == context.functions.end())
			throw Exception("Unknown function " + node->name, ErrorCodes::UNKNOWN_FUNCTION);

		node->function = it->second;
	}
	else if (ASTIdentifier * node = dynamic_cast<ASTIdentifier *>(&*ast))
	{
		NamesAndTypes::const_iterator it = context.columns.find(node->name);
		if (it == context.columns.end())
			throw Exception("Unknown identifier " + node->name, ErrorCodes::UNKNOWN_IDENTIFIER);

		node->type = it->second;
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		node->type = boost::apply_visitor(FieldToDataType(), node->value);
	}

	ASTs children = ast->getChildren();
	for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
		addSemantic(*it);
}


void Expression::checkTypes(ASTPtr ast)
{
	/// Обход в глубину
	
	ASTs children = ast->getChildren();
	for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
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

	ASTs children = ast->getChildren();
	for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
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


void Expression::setNotCalculated(ASTPtr ast)
{
	ast->calculated = false;
	ASTs children = ast->getChildren();
	for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
		setNotCalculated(ast);
}


void Expression::execute(Block & block)
{
	setNotCalculated(ast);
	executeImpl(ast, block);
}


void Expression::executeImpl(ASTPtr ast, Block & block)
{
	/// Обход в глубину

	ASTs children = ast->getChildren();
	for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
		executeImpl(*it, block);

	if (ast->calculated)
		return;

	/** Столбцы из таблицы уже загружены в блок.
	  * Вычисление состоит в добавлении в блок новых столбцов - констант и результатов вычислений функций.
	  */

	if (ASTFunction * node = dynamic_cast<ASTFunction *>(&*ast))
	{
		
	}
	else if (ASTLiteral * node = dynamic_cast<ASTLiteral *>(&*ast))
	{
		
	}
}

	
}
