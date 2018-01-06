#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

/** Converts columns-constants to full columns ("materializes" them).
  */
class MaterializingBlockInputStream : public IProfilingBlockInputStream
{
public:
    MaterializingBlockInputStream(const BlockInputStreamPtr & input);
    String getName() const override;
    String getID() const override;
    Block getHeader() override;

protected:
    Block readImpl() override;
};

}
