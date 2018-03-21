#pragma once

#include <Core/Block.h>
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class CuttingUnionAllRequiredColumnBlockInputStream : public IProfilingBlockInputStream
{
public:
    CuttingUnionAllRequiredColumnBlockInputStream(const BlockInputStreamPtr input_, const Names & required_columns);

    Block getHeader() const override { return header; };
    String getName() const override { return "CuttingUnionAllRequiredColumn"; }

private:
    bool need_cutting{false};
    Block header;
    std::vector<size_t> cutting_pos;

    Block readImpl() override;
};

}
