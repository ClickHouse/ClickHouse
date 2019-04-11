#pragma once
#include <Processors/ISource.h>
#include <Processors/Sources/InputStreamHolder.h>

namespace DB
{


class SourceFromTotals : public ISource
{
public:
    explicit SourceFromTotals(InputStreamHolders holders_);

    String getName() const override { return "SourceFromTotals"; }

    Chunk generate() override;

private:
    bool generated = false;
    InputStreamHolders holders;
};

}
