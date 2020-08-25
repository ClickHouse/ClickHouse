#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Columns/ColumnConst.h>
#include <Storages/ColumnDefault.h>
#include <Interpreters/Context.h>


namespace DB
{


/** This stream adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * All three types of columns are materialized (not constants).
  */
class AddingDefaultBlockOutputStream : public IBlockOutputStream
{
public:
    AddingDefaultBlockOutputStream(
        const BlockOutputStreamPtr & output_,
        const Block & header_,
        const Block & output_block_,
        const ColumnDefaults & column_defaults_,
        const Context & context_)
        : output(output_), header(header_), output_block(output_block_),
          column_defaults(column_defaults_), context(context_)
    {
    }

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;
    const Block header;
    /// Blocks after this stream should have this structure
    const Block output_block;
    const ColumnDefaults column_defaults;
    const Context & context;
};


}
