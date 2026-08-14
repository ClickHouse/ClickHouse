#include <IO/ResidencyIterator.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{


CacheViewPtr probeView(
    ICacheProvider & provider, const StoredObject & object, size_t object_file_offset, ByteRange span)
{
    /// Read-only ONE-shot: the ranged `resolve` covers the whole span (hits with
    /// readers, misses with writers when the provider populates); assemble the view.
    auto view = std::make_unique<CacheView>();
    for (auto & r : provider.resolve(object, object_file_offset, span))
    {
        if (r.kind == ICacheProvider::CacheResolution::Kind::Hit)
            view->hit_entries.push_back(HitEntry{r.range, std::move(r.reader)});
        else if (r.kind == ICacheProvider::CacheResolution::Kind::Miss)
            view->miss_entries.push_back(MissEntry{r.range, std::move(r.writer)});
    }
    return view;
}

}
