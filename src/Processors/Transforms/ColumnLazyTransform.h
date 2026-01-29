#pragma once

#include <Processors/ISimpleTransform.h>

namespace DB
{

struct LazilyReadInfo;
using LazilyReadInfoPtr = std::shared_ptr<LazilyReadInfo>;

class MergeTreeLazilyReader;
using MergeTreeLazilyReaderPtr = std::unique_ptr<MergeTreeLazilyReader>;

class RuntimeDataflowStatisticsCacheUpdater;
using RuntimeDataflowStatisticsCacheUpdaterPtr = std::shared_ptr<RuntimeDataflowStatisticsCacheUpdater>;

class ColumnLazyTransform : public ISimpleTransform
{
public:
    ColumnLazyTransform(
        SharedHeader header_, MergeTreeLazilyReaderPtr lazy_column_reader_, RuntimeDataflowStatisticsCacheUpdaterPtr updater_ = nullptr);

    static Block transformHeader(Block header);

    String getName() const override { return "ColumnLazyTransform"; }

protected:
    void transform(Chunk & chunk) override;

private:
    MergeTreeLazilyReaderPtr lazy_column_reader;

    RuntimeDataflowStatisticsCacheUpdaterPtr updater;
};

}
