#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Interpreters/Expression.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

using Poco::SharedPtr;


/** Выполняет над блоком вычисление некоторого выражения.
  * Выражение состоит из идентификаторов столбцов из блока, констант, обычных функций.
  * Например: hits * 2 + 3, instr("yandex", url)
  * Выражение не меняет количество строк в потоке, и обрабатывает каждую строку независимо от других.
  * is_first - если часть вычислений является первой для блока.
  *  В этом случае, перед обработкой каждого блока, сбрасываются флаги, что элемент в дереве запроса уже был вычислен.
  *  При вложенных применениях нескольких ExpressionBlockInputStream, нужно указать is_first только у первого,
  *   чтобы у последующих не дублировались вычисления.
  * part_id - идентификатор части выражения, которую надо вычислять.
  *  Например, может потребоваться вычислить только часть выражения в секции WHERE.
  */
class ExpressionBlockInputStream : public IProfilingBlockInputStream
{
public:
	ExpressionBlockInputStream(BlockInputStreamPtr input_, SharedPtr<Expression> expression_, bool is_first_ = true, unsigned part_id_ = 0)
		: input(input_), expression(expression_), is_first(is_first_), part_id(part_id_)
	{
		children.push_back(input);
	}

	Block readImpl()
	{
		Block res = input->read();
		if (!res)
			return res;

		if (is_first)
			expression->setNotCalculated(part_id);
		
		expression->execute(res, part_id);
		return res;
	}

	String getName() const { return "ExpressionBlockInputStream"; }

private:
	BlockInputStreamPtr input;
	SharedPtr<Expression> expression;
	bool is_first;
	unsigned part_id;
};

}
