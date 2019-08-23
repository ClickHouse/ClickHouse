#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/ConstraintsDescription.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int VIOLATED_CONSTRAINT;
}

class CheckConstraintsBlockOutputStream : public IBlockOutputStream
{
public:
    CheckConstraintsBlockOutputStream(
            const String & table_,
            const BlockOutputStreamPtr & output_,
            const Block & header_,
            const ConstraintsDescription & constraints_,
            const Context & context_)
            : table(table_),
              output(output_),
              header(header_),
              constraints(constraints_),
              expressions(constraints_.getExpressions(context_, header.getNamesAndTypesList())),
              rows_written(0)
    { }

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

private:
    const ColumnUInt8* executeOnBlock(Block & block, const ExpressionActionsPtr & constraint);
    std::vector<size_t> findAllWrong(const void *data, size_t size);

    String table;
    BlockOutputStreamPtr output;
    Block header;
    const ConstraintsDescription constraints;
    const ConstraintsExpressions expressions;
    size_t rows_written;
};
}
