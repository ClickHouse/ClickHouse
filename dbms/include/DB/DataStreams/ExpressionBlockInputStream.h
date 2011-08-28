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
  * part_id - идентификатор части выражения, которую надо вычислять.
  *  Например, может потребоваться вычислить только часть выражения в секции WHERE.
  */
class ExpressionBlockInputStream : public IBlockInputStream
{
public:
	ExpressionBlockInputStream(BlockInputStreamPtr input_, SharedPtr<Expression> expression_, unsigned part_id_ = 0)
		: input(input_), expression(expression_), part_id(part_id_) {}

	Block read()
	{
		Block res = input->read();
		if (!res)
			return res;

		expression->execute(res, part_id);
		return res;
	}

private:
	BlockInputStreamPtr input;
	SharedPtr<Expression> expression;
	unsigned part_id;
};

}
