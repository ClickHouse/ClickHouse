#include <DataStreams/AddingDefaultBlockOutputStream.h>

#include <Core/Block.h>

namespace DB
{
void AddingDefaultBlockOutputStream::write(const DB::Block & block)
{
    Block res = block;

    /// Computes explicitly specified values (in column_defaults) by default.
    /** @todo if somehow block does not contain values for implicitly-defaulted columns that are prerequisites
         *    for explicitly-defaulted ones, exception will be thrown during evaluating such columns
         *    (implicitly-defaulted columns are evaluated on the line after following one. */
    evaluateMissingDefaults(res, *required_columns, column_defaults, context);

    /// Adds not specified default values.
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
