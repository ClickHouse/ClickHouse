#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;

	
/** Выбирает из блока только столбцы, являющиеся результатом вычисления выражения.
  * Следует применять после ExpressionBlockInputStream.
  */
class ProjectionBlockInputStream : public IProfilingBlockInputStream
{
public:
	ProjectionBlockInputStream(
		BlockInputStreamPtr input_,
		ExpressionPtr expression_,
		bool without_duplicates_and_aliases_ = false,
		unsigned part_id_ = 0,
		ASTPtr subtree_ = NULL)
		: input(input_), expression(expression_), without_duplicates_and_aliases(without_duplicates_and_aliases_), part_id(part_id_), subtree(subtree_)
	{
		children.push_back(input);
	}

	Block readImpl()
	{
		Block res = input->read();
		if (!res)
			return res;

		return expression->projectResult(res, without_duplicates_and_aliases, part_id, subtree);
	}

	String getName() const { return "ProjectionBlockInputStream"; }

	BlockInputStreamPtr clone() { return new ProjectionBlockInputStream(input, expression, without_duplicates_and_aliases, part_id, subtree); }

private:
	BlockInputStreamPtr input;
	ExpressionPtr expression;
	bool without_duplicates_and_aliases;
	unsigned part_id;
	ASTPtr subtree;
};

}
