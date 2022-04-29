#include <Storages/MergeTree/MergeTreeIndices.h>
#include <vector>
namespace DB{

class IMergeTreeIndexConditionAnn: public IMergeTreeIndexCondition
{
    public:
    virtual std::vector<bool> getUsefulGranules(MergeTreeIndexGranulePtr idx_granule) const = 0;
};
}
