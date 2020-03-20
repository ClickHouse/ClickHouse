#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/ConstraintsDescription.h>
#include <Interpreters/StorageID.h>


namespace DB
{

/** Check for constraints violation. If anything is found - throw an exception with detailed error message.
  * Otherwise just pass block to output unchanged.
  */

class CheckConstraintsBlockOutputStream : public IBlockOutputStream
{
public:
    CheckConstraintsBlockOutputStream(
        StorageID table_,
        BlockOutputStreamPtr output_,
        Block header_,
        ConstraintsDescription constraints_,
        const Context & context_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    StorageID table_id;
    BlockOutputStreamPtr output;
    Block header;
    const ConstraintsDescription constraints;
    const ConstraintsExpressions expressions;
    size_t rows_written = 0;
};
}
