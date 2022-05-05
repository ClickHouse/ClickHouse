#include <Storages/MergeTree/MergeTreeIndices.h>
#include <vector>
namespace DB{

// condition interface for Ann indexes. Returns vector of indexes of ranges in granule which are useful for query.
class IMergeTreeIndexConditionAnn: public IMergeTreeIndexCondition
{
    public:
    virtual std::vector<size_t> getUsefulRanges(MergeTreeIndexGranulePtr idx_granule) const = 0;
};
}
