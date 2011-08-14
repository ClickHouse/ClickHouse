#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Выполняет над блоком вычисление некоторого выражения.
  * Выражение состоит из идентификаторов столбцов из блока, констант, обычных функций.
  * Например: hits * 2 + 3, instr("yandex", url)
  * Выражение не меняет количество строк в потоке, и обрабатывает каждую строку независимо от других.
  */
class ExpressionBlockInputStream : public IBlockInputStream
{
public:
	ExpressionBlockInputStream(SharedPtr<IBlockInputStream> input_, SharedPtr<Expression> expression_)
		: input(input_), expression(expression_) {}

	Block read()
	{
		Block res = input->read();
		if (!res)
			return res;

		expression->execute(res);
		return res;
	}

private:
	SharedPtr<IBlockInputStream> input;
	SharedPtr<Expression> expression;
};

}
