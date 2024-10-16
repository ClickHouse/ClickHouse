#include <Storages/MergeTree/MergeSelectors/MergeSelector.h>

#include <Storages/MergeTree/MergeSelectors/MergeSelectorFactory.h>

namespace DB
{

void registerSimpleMergeSelector(MergeSelectorFactory & factory);
void registerSimpleMergeSelectorWithBlurryBase(MergeSelectorFactory & factory);
void registerAllMergeSelector(MergeSelectorFactory & factory);
void registerTTLDeleteMergeSelector(MergeSelectorFactory & factory);
void registerTTLRecompressMergeSelector(MergeSelectorFactory & factory);

void registerMergeSelectors()
{
    auto & factory = MergeSelectorFactory::instance();

    registerSimpleMergeSelector(factory);
    registerSimpleMergeSelectorWithBlurryBase(factory);
    registerAllMergeSelector(factory);
    registerTTLDeleteMergeSelector(factory);
    registerTTLRecompressMergeSelector(factory);
}

}
