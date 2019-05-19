#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/ConstraintsDescription.h>
#include <DataStreams/OneBlockInputStream.h>
#include <DataStreams/ExpressionBlockInputStream.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CONSTRAINTS_ARE_NOT_SATISFIED;
}

class CheckConstraintsBlockOutputStream : public IBlockOutputStream
{
public:
    CheckConstraintsBlockOutputStream(
            const BlockOutputStreamPtr & output_,
            const Block & header_,
            const ConstraintsDescription & constraints_,
            const Context & context_)
            : output(output_),
              header(header_),
              constraints(constraints_),
              expressions(constraints_.getExpressions(context_, header.getNamesAndTypesList()))
    { }

    Block getHeader() const override { return header; }
    void write(const Block & block) override;

    void flush() override;

    void writePrefix() override;
    void writeSuffix() override;

    bool checkConstraintOnBlock(const Block & block, const ExpressionActionsPtr & constraint);

private:
    BlockOutputStreamPtr output;
    Block header;
    const ConstraintsDescription constraints;
    const ConstraintsExpressions expressions;
};
}
