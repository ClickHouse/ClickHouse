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
        const BlockOutputStreamPtr & output_,
        const Block & header_,
        NamesAndTypesList required_columns_,
        const ColumnDefaults & column_defaults_,
        const Context & context_,
        bool only_explicit_column_defaults_)
        : output(output_), header(header_), required_columns(required_columns_),
          column_defaults(column_defaults_), context(context_),
          only_explicit_column_defaults(only_explicit_column_defaults_)
    {
    }

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;
    Block header;
    NamesAndTypesList required_columns;
    const ColumnDefaults column_defaults;
    const Context & context;
    bool only_explicit_column_defaults;
};


}
