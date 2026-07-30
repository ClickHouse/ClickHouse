#include <IO/ResidencyIterator.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{


CacheViewPtr probeView(
    ICacheProvider & provider, const StoredObject & object, size_t object_file_offset, ByteRange span)
{
    auto probe = provider.probe();
    auto view = std::make_unique<CacheView>();
    size_t collected_until = span.offset;
    size_t pos = span.offset;
    while (pos < span.end())
    {
        auto r = probe->lookAt(object, object_file_offset, pos, span.end());
        if (r.kind == ICacheProvider::Resolution::Kind::End)
            break;
        if (r.range.end() > collected_until)
        {
            if (r.kind == ICacheProvider::Resolution::Kind::Hit)
                view->hit_entries.push_back(HitEntry{r.range, std::move(r.reader)});
            else
                view->miss_entries.push_back(MissEntry{r.range, nullptr});
            collected_until = std::max(collected_until, r.range.end());
        }
        pos = std::max(pos + 1, r.range.end());
    }
    return view;
}

}
