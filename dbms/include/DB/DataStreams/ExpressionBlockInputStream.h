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
  * part_id - идентификатор части выражения, которую надо вычислять.
  *  Например, может потребоваться вычислить только часть выражения в секции WHERE.
  * clear_temporaries - удалить временные столбцы из блока, которые больше не понадобятся ни для каких вычислений.
  */
class ExpressionBlockInputStream : public IProfilingBlockInputStream
{
public:
	ExpressionBlockInputStream(BlockInputStreamPtr input_, ExpressionPtr expression_, unsigned part_id_ = 0, bool clear_temporaries_ = false)
		: expression(expression_), part_id(part_id_), clear_temporaries(clear_temporaries_)
	{
		children.push_back(input_);
		input = &*children.back();
	}

	String getName() const { return "ExpressionBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "Expression(" << input->getID() << ", " << expression->getExecutionID(part_id) << ")";
		return res.str();
	}

protected:
	Block readImpl()
	{
		Block res = input->read();
		if (!res)
			return res;

		expression->execute(res, part_id);

		if (clear_temporaries)
			expression->clearTemporaries(res);
			
		return res;
	}

private:
	IBlockInputStream * input;
	ExpressionPtr expression;
	unsigned part_id;
	bool clear_temporaries;
};

}
