#pragma once

#include <Common/config.h>
#if USE_PARQUET
#    include <DataStreams/IProfilingBlockInputStream.h>

namespace DB
{

class Context;

class ParquetBlockInputStream : public IProfilingBlockInputStream
{
public:
    ParquetBlockInputStream(ReadBuffer & istr_, const Block & header_, const Context & context_);

    String getName() const override { return "Parquet"; }
    Block getHeader() const override;

protected:
    Block readImpl() override;

private:
    ReadBuffer & istr;
    Block header;

    // TODO: check that this class implements every part of its parent

    const Context & context;
};

}

#endif
