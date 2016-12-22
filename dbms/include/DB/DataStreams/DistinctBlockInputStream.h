#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Interpreters/Limits.h>
#include <DB/Interpreters/SetVariants.h>

namespace DB
{

/** Из потока блоков оставляет только уникальные строки.
  * Для реализации SELECT DISTINCT ... .
  * Если указан ненулевой limit - прекращает выдавать строки после того, как накопилось limit строк
  *  - для оптимизации SELECT DISTINCT ... LIMIT ... .
  */
class DistinctBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Пустой columns_ значит все столбцы.
	DistinctBlockInputStream(BlockInputStreamPtr input_, const Limits & limits, size_t limit_, Names columns_);

	String getName() const override { return "Distinct"; }

	String getID() const override;

protected:
	Block readImpl() override;

private:
	bool checkLimits() const;

	ConstColumnPlainPtrs getKeyColumns(const Block & block) const;

	template <typename Method>
	void buildFilter(
		Method & method,
		const ConstColumnPlainPtrs & key_columns,
		IColumn::Filter & filter,
		size_t rows) const;


	Names columns_names;
	SetVariants data;
	Sizes key_sizes;
	size_t limit;

	/// Ограничения на максимальный размер множества
	size_t max_rows;
	size_t max_bytes;
	OverflowMode overflow_mode;
};

}
