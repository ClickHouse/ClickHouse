#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class ExpressionActions;

/** Выполняет над блоком вычисление некоторого выражения.
  * Выражение состоит из идентификаторов столбцов из блока, констант, обычных функций.
  * Например: hits * 2 + 3, url LIKE '%yandex%'
  * Выражение обрабатывает каждую строку независимо от других.
  */
class ExpressionBlockInputStream : public IProfilingBlockInputStream
{
private:
	using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
	ExpressionBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_);

	String getName() const override;
	String getID() const override;
	const Block & getTotals() override;

protected:
	Block readImpl() override;

private:
	ExpressionActionsPtr expression;
};

}
