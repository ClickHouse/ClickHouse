#pragma once

#include <DB/Interpreters/Context.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTLiteral.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Storages/StoragePtr.h>

namespace DB
{

class VirtualColumnUtils
{
public:
/// Вычислить минимальный числовый суффикс, который надо добавить к строке, чтобы она не присутствовала в множестве
static String chooseSuffix(const NamesAndTypesList & columns, const String & name)
{
	int id = 0;
	String current_suffix;
	while (true)
	{
		bool done = true;
		for (const auto & it : columns)
			if (it.first == name + current_suffix)
			{
				done = false;
				break;
			}
		if (done) break;
		++id;
		current_suffix = toString<Int32>(id);
	}
	return current_suffix;
}

/// Вычислить минимальный общий числовый суффикс, который надо добавить к каждой строке,
/// чтобы ниодна не присутствовала в множестве.
static String chooseSuffixForSet(const NamesAndTypesList & columns, const std::vector<String> & names)
{
	int id = 0;
	String current_suffix;
	while (true)
	{
		bool done = true;
		for (const auto & it : columns)
		{
			for (size_t i = 0; i < names.size(); ++i)
			{
				if (it.first == names[i] + current_suffix)
				{
					done = false;
					break;
				}
			}
			if (!done)
				break;
		}
		if (done)
			break;
		id ++;
		current_suffix = toString<Int32>(id);
	}
	return current_suffix;
}

/// На данный момент не дописана и не используется.
static void rewriteEntityInAst(ASTPtr ast, const String & column_name, const Field &value)
{
	{
		ASTSelectQuery & select = dynamic_cast<ASTSelectQuery &>(*ast);
		ASTExpressionList & node = dynamic_cast<ASTExpressionList &>(*select.select_expression_list);
		ASTs & asts = node.children;
		ASTLiteral * cur = new ASTLiteral(StringRange(NULL, NULL), value);
		cur->alias = column_name;
		ASTPtr column_value = cur;
		asts.insert(asts.begin(), column_value);
		return;
	}

	if (ASTExpressionList * node = dynamic_cast<ASTExpressionList *>(&*ast))
	{
		ASTs & asts = node->children;
		for (int i = static_cast<int>(asts.size()) - 1; i >= 0; --i)
		{
			if (ASTIdentifier * child = dynamic_cast<ASTIdentifier *>(&*asts[i]))
			{
				if (child->kind == ASTIdentifier::Column && child->getColumnName() == column_name)
				{
					ASTLiteral * cur = new ASTLiteral(StringRange(NULL, NULL), value);
					cur->alias = column_name;

					ASTPtr column_value = cur;

					asts.erase(asts.begin() + i);
					asts.insert(asts.begin() + i, column_value);
				}
			}
		}
	}

	for (auto it : ast->children)
		rewriteEntityInAst(it, column_name, value);
}

};

}
