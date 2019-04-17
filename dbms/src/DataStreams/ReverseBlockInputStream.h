#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>
#include <Common/PODArray.h>

namespace DB
{

class ReverseBlockInputStream : public IProfilingBlockInputStream
{
public:
    ReverseBlockInputStream(const BlockInputStreamPtr& input);

    String getName() const override;

    Block getHeader() const override;

protected:
    Block readImpl() override;
};

} // namespace DB
