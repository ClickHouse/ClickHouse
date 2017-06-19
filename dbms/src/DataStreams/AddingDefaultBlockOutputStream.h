#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Columns/ColumnConst.h>
#include <Storages/ColumnDefault.h>
#include <Interpreters/Context.h>
#include <Interpreters/evaluateMissingDefaults.h>


namespace DB
{


/** Adds missing columns to the block with default values.
  * These columns are materialized (not constants).
  */
class AddingDefaultBlockOutputStream : public IBlockOutputStream
{
public:
    AddingDefaultBlockOutputStream(
        BlockOutputStreamPtr output_,
        NamesAndTypesListPtr required_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context_,
        bool only_explicit_column_defaults_)
        : output(output_), required_columns(required_columns_),
          column_defaults(column_defaults_), context(context_),
          only_explicit_column_defaults(only_explicit_column_defaults_)
    {
    }

    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;
    NamesAndTypesListPtr required_columns;
    const ColumnDefaults column_defaults;
    Context context;
    bool only_explicit_column_defaults;
};


}
