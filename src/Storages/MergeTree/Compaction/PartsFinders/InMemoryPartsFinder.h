#pragma once

#include <Storages/MergeTree/MergeTreeData.h>

#include <Storages/MergeTree/Compaction/PartsFinders/IPartsFinder.h>

namespace DB
{

class InMemoryPartsFinder : public IPartsFinder
{
public:
    explicit InMemoryPartsFinder(const MergeTreeData & data_);
    ~InMemoryPartsFinder() override = default;

    FutureMergedMutatedPartPtr constructFuturePart(const MergeSelectorChoice & choice) const override;

private:
    const MergeTreeData & data;
};

}
