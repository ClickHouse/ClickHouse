#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowMap.h>
#include <Common/ProfileEvents.h>
#include <stdexcept>

namespace ProfileEvents
{
    extern const Event CASRefMaterializeInPlace;
    extern const Event CASRefMaterializeCopy;
}

namespace DB::Cas
{

std::pair<const String &, const RefCommittedRow &> RefCowMap::const_iterator::operator*() const
{
    return at_overlay
        ? std::pair<const String &, const RefCommittedRow &>(overlay_it->first, *overlay_it->second)
        : std::pair<const String &, const RefCommittedRow &>(base_it->first, base_it->second);
}

void RefCowMap::const_iterator::normalize()
{
    /// Drop overlay tombstones (and the base row each one shadows) until the next live overlay
    /// entry, or exhaustion. A tombstone is only actually consumed once it is next-in-merge-order
    /// (its key <= base_it's current key, the same tie-break the final at_overlay check below
    /// uses) -- a tombstone further ahead than base_it must stay put, or base_it would later walk
    /// straight past its (still-hidden) target with nothing left in overlay to hide it.
    while (overlay_it != overlay_end && !overlay_it->second.has_value()
           && (base_it == base_end || overlay_it->first <= base_it->first))
    {
        const String key = overlay_it->first;
        ++overlay_it;
        if (base_it != base_end && base_it->first == key)
            ++base_it;
    }
    /// Overlay wins ties: a live overlay entry at the same key as `base_it` is an override.
    at_overlay = (overlay_it != overlay_end) && (base_it == base_end || overlay_it->first <= base_it->first);
}

RefCowMap::const_iterator & RefCowMap::const_iterator::operator++()
{
    if (at_overlay)
    {
        const String key = overlay_it->first;
        ++overlay_it;
        if (base_it != base_end && base_it->first == key)
            ++base_it;   /// this overlay entry shadowed a base row of the same key: consume it too
    }
    else
    {
        ++base_it;
    }
    normalize();
    return *this;
}

RefCowMap::const_iterator RefCowMap::begin() const
{
    const_iterator it;
    it.base_it = base->begin();
    it.base_end = base->end();
    it.overlay_it = overlay.begin();
    it.overlay_end = overlay.end();
    it.normalize();
    return it;
}

RefCowMap::const_iterator RefCowMap::end() const
{
    const_iterator it;
    it.base_it = base->end();
    it.base_end = base->end();
    it.overlay_it = overlay.end();
    it.overlay_end = overlay.end();
    it.at_overlay = false;
    return it;
}

RefCowMap::const_iterator RefCowMap::find(const String & key) const
{
    const auto ov = overlay.find(key);
    if (ov != overlay.end())
    {
        if (!ov->second.has_value())
            return end();   /// tombstoned: not present
        const_iterator it;
        it.base_it = base->lower_bound(key);   /// first base key >= this one: keeps the iterator mergeable
        it.base_end = base->end();
        it.overlay_it = ov;
        it.overlay_end = overlay.end();
        it.at_overlay = true;
        return it;
    }
    const auto b = base->find(key);
    if (b == base->end())
        return end();
    const_iterator it;
    it.base_it = b;
    it.base_end = base->end();
    it.overlay_it = overlay.lower_bound(key);   /// first overlay key >= this one: keeps the iterator mergeable
    it.overlay_end = overlay.end();
    it.at_overlay = false;
    return it;
}

const RefCommittedRow & RefCowMap::at(const String & key) const
{
    const auto it = find(key);
    if (it == end())
        throw std::out_of_range("RefCowMap::at: key not found: " + key);
    return it->second;
}

void RefCowMap::insertLive(const String & key, RefCommittedRow row)
{
    const auto ov = overlay.find(key);
    if (ov != overlay.end())
    {
        if (!ov->second.has_value())
            ++net_delta;   /// tombstoned (dead) -> live again
        ov->second = std::move(row);
    }
    else
    {
        if (!base->contains(key))
            ++net_delta;   /// brand new key, absent from base too
        overlay.emplace(key, std::move(row));
    }
}

std::pair<RefCowMap::iterator, bool> RefCowMap::emplace(String key, RefCommittedRow row)
{
    if (contains(key))
        return {find(key), false};
    insertLive(key, std::move(row));
    return {find(key), true};
}

std::pair<RefCowMap::iterator, bool> RefCowMap::insert_or_assign(String key, RefCommittedRow row)
{
    const bool was_present = contains(key);
    insertLive(key, std::move(row));
    return {find(key), !was_present};
}

size_t RefCowMap::erase(const String & key)
{
    const auto ov = overlay.find(key);
    if (ov != overlay.end())
    {
        if (!ov->second.has_value())
            return 0;   /// already tombstoned: no-op
        if (base->contains(key))
            ov->second.reset();   /// keep shadowing the base row
        else
            overlay.erase(ov);    /// pure-overlay key: nothing left to shadow
        --net_delta;
        return 1;
    }
    if (!base->contains(key))
        return 0;
    overlay.emplace(key, std::nullopt);   /// tombstone a base-only row
    --net_delta;
    return 1;
}

RefCowMap::iterator RefCowMap::erase(const_iterator pos)
{
    if (pos == end())
        return end();
    const String key = pos->first;
    ++pos;
    erase(key);
    return pos;
}

bool RefCowMap::operator==(const RefCowMap & other) const
{
    if (size() != other.size())
        return false;
    auto a = begin();
    auto b = other.begin();
    for (; a != end() && b != other.end(); ++a, ++b)
        if (a->first != b->first || !(a->second == b->second))
            return false;
    return a == end() && b == other.end();
}

void RefCowMap::materialize()
{
    if (overlay.empty())
        return;
    /// See the header for the full ownership-safety argument. The exception-coherence argument for the
    /// in-place path is here, next to the code it governs.
    ///
    /// Uniquely-owned base (the production flush case): fold each overlay entry into `*base` IN PLACE --
    /// O(overlay), no O(N) copy. The fold is INCREMENTALLY COHERENT: for every entry the base mutation
    /// (its ONLY throw point -- a `std::map` node insert or a `RefCommittedRow` copy under memory
    /// pressure) runs FIRST, and the matching `net_delta` adjustment and `overlay.erase` are both
    /// non-throwing and run only AFTER it succeeds. So at every point an exception can escape, the
    /// (base, overlay, net_delta) triple is exactly coherent: the merged view is unchanged, `size()` is
    /// exact, and a later `materialize` resumes cleanly from the un-erased overlay tail. This is a
    /// STRONGER guarantee than the copy path's (which only had to be strong for the shared holder): the
    /// state is coherent even mid-fold, which matters because `materialize` runs AFTER the transaction is
    /// durably PUT and applied -- an allocation throw here must never leave live bookkeeping wrong. (A
    /// throwing key-assign can leave a partially-updated base row, but its still-present overlay entry
    /// shadows that key in the merged view, so the partial row stays invisible until it is re-folded.)
    if (base.use_count() == 1)
    {
        ProfileEvents::increment(ProfileEvents::CASRefMaterializeInPlace);
        for (auto it = overlay.begin(); it != overlay.end(); )
        {
            if (it->second)
            {
                const bool inserted = base->insert_or_assign(it->first, *it->second).second;   /// throw point
                if (inserted)
                    --net_delta;   /// a key absent from base counted +1 in net_delta; now it lives in base
            }
            else
            {
                base->erase(it->first);   /// a tombstone only ever shadows a base member: non-throwing erase
                ++net_delta;              /// counted -1 in net_delta; now actually removed from base
            }
            it = overlay.erase(it);       /// non-throwing: retire this entry only after its base mutation stuck
        }
        return;   /// net_delta arithmetic above lands it back at 0 (base now holds the whole merged view)
    }
    /// A copy still shares this base, so fold into a FRESH one and swap -- the shared holder must stay
    /// byte-unchanged. Strong guarantee: a mid-fold throw discards `fresh` and leaves this container (and
    /// every sharer) exactly as it was.
    ProfileEvents::increment(ProfileEvents::CASRefMaterializeCopy);
    auto fresh = std::make_shared<Base>(*base);
    for (const auto & [key, maybe_row] : overlay)
    {
        if (maybe_row)
            (*fresh)[key] = *maybe_row;
        else
            fresh->erase(key);
    }
    base = std::move(fresh);
    overlay.clear();
    net_delta = 0;
}

}
