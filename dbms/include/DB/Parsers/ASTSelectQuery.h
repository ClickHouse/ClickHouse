#pragma once

#include <DB/Parsers/IAST.h>
#include <DB/Parsers/ASTQueryWithOutput.h>
#include <DB/Parsers/ASTExpressionList.h>
#include <DB/Parsers/ASTFunction.h>

namespace DB
{


/** SELECT запрос
  */
class ASTSelectQuery : public ASTQueryWithOutput
{
public:
	bool distinct;
	ASTPtr select_expression_list;
	ASTPtr database;
	ASTPtr table;	/// Идентификатор, табличная функция или подзапрос (рекурсивно ASTSelectQuery)
	ASTPtr array_join_expression_list;
	bool final;
	ASTPtr sample_size;
	ASTPtr prewhere_expression;
	ASTPtr where_expression;
	ASTPtr group_expression_list;
	bool group_by_with_totals;
	ASTPtr having_expression;
	ASTPtr order_expression_list;
	ASTPtr limit_offset;
	ASTPtr limit_length;

	ASTSelectQuery() : distinct(false), final(false), group_by_with_totals(false) {}
	ASTSelectQuery(StringRange range_) : ASTQueryWithOutput(range_), distinct(false), final(false), group_by_with_totals(false) {}
	
	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return "SelectQuery"; };


	/// Переписывает select_expression_list, чтобы вернуть только необходимые столбцы в правильном порядке.
	void rewriteSelectExpressionList(const Names & column_names)
	{
		ASTPtr result = new ASTExpressionList;
		ASTs asts = select_expression_list->children;

		for (size_t i = 0; i < column_names.size(); ++i)
		{
			bool done = 0;
			for (size_t j = 0; j < asts.size(); ++j)
			{
				if (asts[j]->getAlias() == column_names[i])
				{
					result->children.push_back(asts[j]->clone());
					done = 1;
				}
			}
			if (!done)
				throw Exception("Error while rewriting expressioin list for select query."
					" Could not find alias: " + column_names[i],
					DB::ErrorCodes::UNKNOWN_IDENTIFIER);

		}

		for (auto & child : children)
		{
			if (child == select_expression_list)
			{
				child = result;
				break;
			}
		}
		select_expression_list = result;
	}

	ASTPtr clone() const
	{
		ASTSelectQuery * res = new ASTSelectQuery(*this);
		res->children.clear();

		if (select_expression_list) { res->select_expression_list 	= select_expression_list->clone(); 	res->children.push_back(res->select_expression_list); }
		if (database) 				{ res->database 				= database->clone(); 				res->children.push_back(res->database); }
		if (table) 					{ res->table 					= table->clone(); 					res->children.push_back(res->table); }
		if (array_join_expression_list) 	{ res->array_join_expression_list	= array_join_expression_list->clone(); 	res->children.push_back(res->array_join_expression_list); }
		if (sample_size) 			{ res->sample_size				= sample_size->clone(); 			res->children.push_back(res->sample_size); }
		if (prewhere_expression)		{ res->prewhere_expression 		= prewhere_expression->clone(); 		res->children.push_back(res->prewhere_expression); }
		if (where_expression)		{ res->where_expression 		= where_expression->clone(); 		res->children.push_back(res->where_expression); }
		if (group_expression_list) 	{ res->group_expression_list 	= group_expression_list->clone(); 	res->children.push_back(res->group_expression_list); }
		if (having_expression) 		{ res->having_expression 		= having_expression->clone(); 		res->children.push_back(res->having_expression); }
		if (order_expression_list) 	{ res->order_expression_list 	= order_expression_list->clone(); 	res->children.push_back(res->order_expression_list); }
		if (limit_offset) 			{ res->limit_offset 			= limit_offset->clone(); 			res->children.push_back(res->limit_offset); }
		if (limit_length) 			{ res->limit_length 			= limit_length->clone(); 			res->children.push_back(res->limit_length); }
		if (format) 				{ res->format 					= format->clone(); 					res->children.push_back(res->format); }

		return res;
	}
};

}
