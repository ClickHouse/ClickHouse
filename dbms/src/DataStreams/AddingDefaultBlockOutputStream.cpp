#include <DB/DataStreams/AddingDefaultBlockOutputStream.h>

#include <DB/Core/Block.h>

namespace DB
{
void AddingDefaultBlockOutputStream::write(const DB::Block & block)
{
	Block res = block;

	/// Вычисляет явно указанные (в column_defaults) значения по-умолчанию.
	/** @todo if somehow block does not contain values for implicitly-defaulted columns that are prerequisites
		 *	for explicitly-defaulted ones, exception will be thrown during evaluating such columns
		 *	(implicitly-defaulted columns are evaluated on the line after following one. */
	evaluateMissingDefaults(res, *required_columns, column_defaults, context);

	/// Добавляет не указанные значения по-умолчанию.
	if (!only_explicit_column_defaults)
		/// @todo this line may be moved before `evaluateMissingDefaults` with passing {required_columns - explicitly-defaulted columns}
		res.addDefaults(*required_columns);

	output->write(res);
}

void AddingDefaultBlockOutputStream::flush()
{
	output->flush();
}

void AddingDefaultBlockOutputStream::writePrefix()
{
	output->writePrefix();
}

void AddingDefaultBlockOutputStream::writeSuffix()
{
	output->writeSuffix();
}
}
