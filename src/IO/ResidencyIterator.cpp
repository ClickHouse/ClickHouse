#include <IO/ResidencyIterator.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{


CacheViewPtr probeView(
    ICacheProvider & provider, const StoredObject & object, size_t object_file_offset, ByteRange span)
{
    /// Read-only ONE-shot: the ranged `probeRange` resolves the whole span
    /// (hits with readers, writer-less misses); assemble the held view.
    auto view = std::make_unique<CacheView>();
    for (auto & r : provider.probe()->resolve(object, object_file_offset, span))
    {
        if (r.kind == ICacheProvider::Resolution::Kind::Hit)
            view->hit_entries.push_back(HitEntry{r.range, std::move(r.reader)});
        else if (r.kind == ICacheProvider::Resolution::Kind::Miss)
            view->miss_entries.push_back(MissEntry{r.range, std::move(r.writer)});
    }
    return view;
}

}
