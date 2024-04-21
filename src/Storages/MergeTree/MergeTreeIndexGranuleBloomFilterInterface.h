#include <Storages/MergeTree/MergeTreeIndices.h>

namespace DB
{
    class MergeTreeIndexGranuleBloomFilterInterface : public IMergeTreeIndexGranule
    {
    public:
        virtual const std::vector<BloomFilterPtr> & getFilters() const = 0;
    };
}
