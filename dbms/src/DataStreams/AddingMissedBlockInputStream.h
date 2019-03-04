#pragma once

#include <DataStreams/IBlockInputStream.h>
#include <Storages/ColumnDefault.h>


namespace DB
{


/** This stream adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * All three types of columns are materialized (not constants).
  */
class AddingMissedBlockInputStream : public IBlockInputStream
{
public:
    AddingMissedBlockInputStream(
        const BlockInputStreamPtr & input_,
        const Block & header_,
        const ColumnDefaults & column_defaults_,
        const Context & context_);

    String getName() const override { return "AddingMissed"; }
    Block getHeader() const override { return header; }

private:
    Block readImpl() override;

    BlockInputStreamPtr input;
    /// Blocks after this stream should have this structure
    const Block header;
    const ColumnDefaults column_defaults;
    const Context & context;
};


}
