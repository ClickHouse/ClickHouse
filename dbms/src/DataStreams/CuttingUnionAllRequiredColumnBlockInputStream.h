#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/** Cutting block by required columns.
  * Use required columns to cutting source blocks if required columns is not empty
  *
  * Example:
  *     source_columns   : [X,X,A,B,X]
  *     required_columns : [X,A,X]
  *     output_columns   : [X,A,X]
  */
class CuttingUnionAllRequiredColumnBlockInputStream : public IProfilingBlockInputStream
{
public:
    CuttingUnionAllRequiredColumnBlockInputStream(const BlockInputStreamPtr & input_, const Names & required_columns);

    Block getHeader() const override { return header; };
    String getName() const override { return "CuttingUnionAllRequiredColumn"; }

private:
    bool need_cutting{false};
    Block header;
    std::vector<size_t> cutting_pos;

    Block readImpl() override;
};

}
