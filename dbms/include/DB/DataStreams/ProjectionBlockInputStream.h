#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;

	
/** Выбирает из блока только столбцы, являющиеся результатом вычисления выражения.
  * Следует применять после ExpressionBlockInputStream.
  */
class ProjectionBlockInputStream : public IBlockInputStream
{
public:
	ProjectionBlockInputStream(
		BlockInputStreamPtr input_,
		SharedPtr<Expression> expression_,
		bool without_duplicates_ = false,
		unsigned part_id_ = 0)
		: input(input_), expression(expression_), without_duplicates(without_duplicates_), part_id(part_id_) {}

	Block read()
	{
		Block res = input->read();
		if (!res)
			return res;

		return expression->projectResult(res, without_duplicates, part_id);
	}

private:
	BlockInputStreamPtr input;
	SharedPtr<Expression> expression;
	bool without_duplicates;
	unsigned part_id;
};

}
