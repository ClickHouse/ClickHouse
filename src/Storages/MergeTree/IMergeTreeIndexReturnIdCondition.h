#pragma once

#include <Storages/MergeTree/IMergeTreeIndices.h>

namespace DB
{

class IMergeTreeIndexReturnIdCondition : public IMergeTreeIndexCondition {
public:
    virtual ~IMergeTreeIndexReturnIdCondition() override = default;
    
    virtual bool alwaysUnknownOrTrue() const override = 0;

    virtual bool mayBeTrueOnGranule(MergeTreeIndexGranulePtr granule) const override = 0;

    virtual std::vector<int32_t> returnIdRecords(MergeTreeIndexGranulePtr granule) const = 0;
};


using MergeTreeIndexReturnIdConditionPtr = std::shared_ptr<IMergeTreeIndexReturnIdCondition>;
using MergeTreeIndexReturnIdConditions = std::vector<MergeTreeIndexReturnIdConditionPtr>;

}
