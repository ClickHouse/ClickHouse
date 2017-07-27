#include "ComplexKeyCacheDictionary.h"

#include <Dictionaries/DictionaryBlockInputStream.h>
#include <Common/ProfilingScopedRWLock.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event DictCacheLockReadNs;
}

namespace DB
{

BlockInputStreamPtr ComplexKeyCacheDictionary::getBlockInputStream(const Names & column_names, size_t max_block_size) const
{
    std::vector<StringRef> keys;
    {
        const ProfilingScopedReadRWLock read_lock{rw_lock, ProfileEvents::DictCacheLockReadNs};

        for (auto idx : ext::range(0, cells.size()))
            if (!isEmptyCell(idx)
                && !cells[idx].isDefault())
                keys.push_back(cells[idx].key);
    }

    using BlockInputStreamType = DictionaryBlockInputStream<ComplexKeyCacheDictionary, UInt64>;
    return std::make_shared<BlockInputStreamType>(shared_from_this(), max_block_size, keys, column_names);
}

}
