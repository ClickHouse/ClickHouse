#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

class ExpressionActions;


/** Реализует операции WHERE, HAVING.
  * На вход подаётся поток блоков и выражение, добавляющее в блок один столбец типа ColumnUInt8, содержащий условия фильтрации.
  * Выражение вычисляется и возвращается поток блоков, в котором содержатся только отфильтрованные строки.
  */
class FilterBlockInputStream : public IProfilingBlockInputStream
{
private:
	using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

public:
	/// filter_column_ - номер столбца с условиями фильтрации.
	FilterBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_, ssize_t filter_column_);
	FilterBlockInputStream(BlockInputStreamPtr input_, ExpressionActionsPtr expression_, const String & filter_column_name_);

	String getName() const override;
	String getID() const override;
	const Block & getTotals() override;

protected:
	Block readImpl() override;

private:
	ExpressionActionsPtr expression;
	ssize_t filter_column;
	String filter_column_name;

	bool is_first = true;
	bool filter_always_true = false;
	bool filter_always_false = false;
};

}
