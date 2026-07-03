#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>

namespace DB
{

void registerSimpleMergeSelector(MergeSelectorFactory & factory);
void registerStochasticSimpleMergeSelector(MergeSelectorFactory & factory);
void registerTrivialMergeSelector(MergeSelectorFactory & factory);
void registerAllMergeSelector(MergeSelectorFactory & factory);

void registerMergeSelectors()
{
    auto & factory = MergeSelectorFactory::instance();

    registerSimpleMergeSelector(factory);
    registerStochasticSimpleMergeSelector(factory);
    registerTrivialMergeSelector(factory);
    registerAllMergeSelector(factory);
}

}
