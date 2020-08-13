#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace DB
{

/// Reverses an order of rows in every block in a data stream.
class ReverseBlockInputStream : public IBlockInputStream
{
public:
    ReverseBlockInputStream(const BlockInputStreamPtr & input);

    String getName() const override;
    Block getHeader() const override;

protected:
    Block readImpl() override;
};

}
