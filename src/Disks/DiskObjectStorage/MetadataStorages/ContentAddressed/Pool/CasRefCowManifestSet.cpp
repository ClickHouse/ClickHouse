#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasRefCowManifestSet.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace ProfileEvents
{
    extern const Event CASRefMaterializeInPlace;
    extern const Event CASRefMaterializeCopy;
}

namespace DB::Cas
{

bool RefCowManifestSet::contains(const ManifestRef & m) const
{
    const auto it = overlay.find(m);
    if (it != overlay.end())
        return it->second;
    return base->contains(m);
}

void RefCowManifestSet::insert(const ManifestRef & m)
{
    /// Unconditional membership guard, in EVERY build (not a `chassert`): a duplicate insert means the
    /// index has drifted from `committed`/`precommits`, and if it silently bumped `net_delta` the index
    /// would report a manifest present that a single `erase` could then hide while another owner still
    /// names it -- corrupting the add-precommit uniqueness invariant and GC's `+1/-1` edge accounting.
    /// Fail closed instead. The caller's own uniqueness check is what enforces the invariant; this is the
    /// last line that turns a maintaining-code bug into a caught exception rather than silent drift.
    if (contains(m))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefCowManifestSet: inserting a manifest that already has an owner -- the owned-manifest "
            "index has drifted from committed/precommits (a bug in the maintaining code)");
    const auto it = overlay.find(m);
    if (it != overlay.end())
        it->second = true;   /// was a tombstone shadowing a base member -- revive it
    else
        overlay.emplace(m, true);
    ++net_delta;
}

void RefCowManifestSet::erase(const ManifestRef & m)
{
    /// Same fail-closed rationale as `insert`: erasing an absent manifest would drift `net_delta` and,
    /// worse, could shadow a still-live owner. Throw in every build rather than silently corrupting.
    if (!contains(m))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefCowManifestSet: erasing a manifest with no current owner -- the owned-manifest index "
            "has drifted from committed/precommits (a bug in the maintaining code)");
    const auto it = overlay.find(m);
    if (it != overlay.end())
    {
        if (base->contains(m))
            it->second = false;   /// keep shadowing the base member
        else
            overlay.erase(it);    /// pure-overlay member: nothing left to shadow
    }
    else
    {
        overlay.emplace(m, false);   /// tombstone a base-only member
    }
    --net_delta;
}

void RefCowManifestSet::materialize()
{
    if (overlay.empty())
        return;
    /// Same two-path shape and exception-coherence contract as `RefCowMap::materialize` (see its
    /// comment for the full argument). Uniquely-owned base: fold each overlay entry into `*base` IN
    /// PLACE -- O(overlay), no O(N) `unordered_set` copy. Incrementally coherent: the base mutation (the
    /// only throw point -- an `unordered_set` insert's alloc/rehash, which is strong) runs first, then
    /// the non-throwing `net_delta` adjustment and `overlay.erase` commit the entry. Any escape leaves
    /// (base, overlay, net_delta) exactly coherent and a later `materialize` resumable.
    if (base.use_count() == 1)
    {
        ProfileEvents::increment(ProfileEvents::CASRefMaterializeInPlace);
        for (auto it = overlay.begin(); it != overlay.end(); )
        {
            if (it->second)
            {
                const bool inserted = base->insert(it->first).second;   /// throw point (alloc/rehash, strong)
                if (inserted)
                    --net_delta;   /// a member absent from base counted +1 in net_delta; now it lives in base
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
    /// A copy still shares this base, so fold into a FRESH one and swap -- strong guarantee, the shared
    /// holder stays byte-unchanged.
    ProfileEvents::increment(ProfileEvents::CASRefMaterializeCopy);
    auto fresh = std::make_shared<Base>(*base);
    for (const auto & [m, present] : overlay)
    {
        if (present)
            fresh->insert(m);
        else
            fresh->erase(m);
    }
    base = std::move(fresh);
    overlay.clear();
    net_delta = 0;
}

}
