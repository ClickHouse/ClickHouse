#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;

	
/** Подставляет в Block оригинальные имена столбцов до их переписывания при sign rewrite.
 * 
  * При распределенной обработке запроса sign rewrite осуществляется на удаленных серверах,
  * а Projection осуществляется на локальном сервере. Поэтому локальный сервер не знает
  * о новых именах столбцов, которые получились после sign rewrite и будет искать в блоке
  * оригинальные имена.
  */
class OriginalColumnNameSubstitutorBlockInputStream : public IProfilingBlockInputStream
{
public:
	OriginalColumnNameSubstitutorBlockInputStream(
		BlockInputStreamPtr input_,
		ExpressionPtr expression_)
		: expression(expression_)
	{
		children.push_back(input_);
	}

	String getName() const { return "OriginalColumnNameSubstitutorBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "OriginalColumnNameSubstitutor(" << children.back()->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		Block res = children.back()->read();
		if (!res)
			return res;

		return expression->substituteOriginalColumnNames(res);
	}

private:
	ExpressionPtr expression;
};

}
