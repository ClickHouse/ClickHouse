#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/ConstraintsDescription.h>


namespace DB
{

/** Check for constraints violation. If anything is found - throw an exception with detailed error message.
  * Otherwise just pass block to output unchanged.
  */

class CheckConstraintsBlockOutputStream : public IBlockOutputStream
{
public:
    CheckConstraintsBlockOutputStream(
            const String & table_,
            const BlockOutputStreamPtr & output_,
            const Block & header_,
            const ConstraintsDescription & constraints_,
            const Context & context_);

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    String table;
    BlockOutputStreamPtr output;
    Block header;
    const ConstraintsDescription constraints;
    const ConstraintsExpressions expressions;
    size_t rows_written = 0;
};
}
