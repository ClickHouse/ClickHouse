#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Columns/ColumnConst.h>
#include <Storages/ColumnsDescription.h>


namespace DB
{

class ExpressionActions;
using ExpressionActionsPtr = std::shared_ptr<ExpressionActions>;

class Context;

/** This stream adds three types of columns into block
  * 1. Columns, that are missed inside request, but present in table without defaults (missed columns)
  * 2. Columns, that are missed inside request, but present in table with defaults (columns with default values)
  * 3. Columns that materialized from other columns (materialized columns)
  * Also the stream can substitute NULL into DEFAULT value in case of INSERT SELECT query (null_as_default) if according setting is 1.
  * All three types of columns are materialized (not constants).
  */
class AddingDefaultBlockOutputStream : public IBlockOutputStream
{
public:
    AddingDefaultBlockOutputStream(
        const BlockOutputStreamPtr & output_,
        const Block & header_,
        const ColumnsDescription & columns_,
        ContextPtr context_,
        bool null_as_default_ = false);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    BlockOutputStreamPtr output;
    const Block header;
    ExpressionActionsPtr adding_defaults_actions;
};


}
