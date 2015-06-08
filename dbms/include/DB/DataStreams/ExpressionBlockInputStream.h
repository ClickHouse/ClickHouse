#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/ExpressionActions.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Выполняет над блоком вычисление некоторого выражения.
  * Выражение состоит из идентификаторов столбцов из блока, констант, обычных функций.
  * Например: hits * 2 + 3, url LIKE '%yandex%'
  * Выражение обрабатывает каждую строку независимо от других.
  */
class ExpressionBlockInputStream : public IProfilingBlockInputStream
{
public:
	ExpressionBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_)
		: expression(expression_)
	{
		children.push_back(input_);
	}

	String getName() const override { return "Expression"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Expression(" << children.back()->getID() << ", " << expression->getID() << ")";
		return res.str();
	}

	const Block & getTotals() override
	{
		if (IProfilingBlockInputStream * child = dynamic_cast<IProfilingBlockInputStream *>(&*children.back()))
		{
			totals = child->getTotals();
			expression->executeOnTotals(totals);
		}

		return totals;
	}

protected:
	Block readImpl() override
	{
		Block res = children.back()->read();
		if (!res)
			return res;

		expression->execute(res);

		return res;
	}

private:
	ExpressionActionsPtr expression;
};

}
