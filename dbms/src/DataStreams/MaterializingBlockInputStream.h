#pragma once

#include <DataStreams/IBlockInputStream.h>

namespace DB
{

/** Converts columns-constants to full columns ("materializes" them).
  */
class MaterializingBlockInputStream : public IBlockInputStream
{
public:
    MaterializingBlockInputStream(const BlockInputStreamPtr & input);
    String getName() const override;
    Block getHeader() const override;

protected:
    Block readImpl() override;
};

}
