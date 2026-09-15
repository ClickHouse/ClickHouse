#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasSentinelProbe.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCatalogFormat.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB::Cas
{

SentinelProbeResult probeSentinel(CasOperation & op, const String & key, const Retry & policy)
{
    return op.probeSentinel(key, policy);
}

namespace
{

/// Is `key` structurally-valid `_probe/` capability-battery debris — i.e. an object strictly under the
/// reserved `<prefix>/_probe/` subtree? `runCapabilityProbe` (invoked with `<prefix>/_probe/<u128hex>` by
/// `Pool::open`) is the ONLY writer under `_probe/`; a content-addressed pool NEVER stores durable
/// data/control state there, so the whole subtree is ephemeral capability-probe scratch that a crash or a
/// concurrent fresh opener may leave behind ([D2]). Ignoring it can therefore never strand real data —
/// `pool_prefix` is exclusively CAS-owned. The trailing `/` in `probe_root` keeps a mere sibling
/// look-alike (`<prefix>/_probe`, `<prefix>/_probelike/…`) OUT of the reserved subtree, so it is still
/// treated as genuine residual and fails the bootstrap closed.
bool isProbeSubtreeDebris(const String & probe_root, const String & key)
{
    return key.starts_with(probe_root);
}

}

BootstrapResidual probePoolBootstrapResidual(CasOperation & op, const Layout & layout)
{
    const String pool_meta_key = layout.poolMetaKey();
    const String catalog_key = layout.refCatalogKey();
    const String prefix = layout.poolPrefix() + "/";
    const String probe_root = layout.poolPrefix() + "/_probe/";

    /// `_pool_meta` present is decisive on its own, and one exact read of that key answers it without
    /// enumerating anything: a LIST page over a large pool is the most expensive request a store
    /// answers (it must enumerate and sort the prefix), and a store that is merely slow to LIST would
    /// otherwise refuse to reopen a pool it can read perfectly well. So the existing-pool case is
    /// settled by the read, and the LIST below runs only when the key is absent -- which is exactly
    /// the case that needs the absence-of-residue proof. A read that could not settle presence (a
    /// refusal, a deadline) is not a verdict; it falls through to the LIST, which may still see the
    /// key.
    try
    {
        if (op.read(pool_meta_key, Retry::standard()))
            return BootstrapResidual::PoolMetaPresent;
    }
    catch (...)
    {
        LOG_WARNING(getLogger("CasBootstrap"),
            "Pool prefix '{}': the exact read of '{}' could not settle whether the pool exists; "
            "falling back to the residual LIST: {}",
            prefix, pool_meta_key, getCurrentExceptionMessage(/*with_stacktrace=*/false));
    }

    /// The LIST answers one question: is there anything under the prefix besides the ignorable
    /// battery debris (and, as the one retryable exception, the canonical empty catalog)? The first
    /// residual key settles it, so the walk stops there. A probe-only or catalog-only prefix is walked
    /// to completion -- debris is normally a few keys but every interrupted open leaves one more, so
    /// it may span pages -- and the page is kept small because the enumeration cost of a large prefix
    /// is what a slow store cannot deliver within an attempt. `_pool_meta` is still recognised if the
    /// walk meets it (the exact read above may have been refused): it sorts before every key family
    /// CAS itself writes under `<prefix>/`, so on a lexicographic listing it is met before anything
    /// that could have stopped the walk. Foreign residue that sorts earlier, or a listing that is not
    /// lexicographic, can only make this refuse an existing pool, never bootstrap over one.
    constexpr size_t page_limit = 32;
    bool has_pool_meta = false;
    bool has_residual = false;
    bool has_catalog = false;
    try
    {
        op.forEachListedKey(prefix, [&](const ListedKey & listed) -> bool
        {
            if (listed.key == pool_meta_key)
            {
                has_pool_meta = true;
                return false;   /// decisive — the pool is authoritative; stop the walk
            }
            if (isProbeSubtreeDebris(probe_root, listed.key))
                return true;   /// crash leftover / concurrent opener's battery — ignore ([D2])
            if (listed.key == catalog_key)
            {
                has_catalog = true;
                return true;
            }
            has_residual = true;   /// a non-`_probe` object, and no `_pool_meta` seen — decisive too
            return false;
        }, Retry::standard(), page_limit);
    }
    catch (...)
    {
        /// The LIST failed: absence was never proven. Never mint a fresh identity under uncertainty —
        /// but log the swallowed error so the fail-closed startup refusal is diagnosable (the root cause
        /// must not vanish just because the verdict is "Indeterminate").
        LOG_WARNING(getLogger("CasBootstrap"),
            "Pool prefix '{}': the authoritative residual LIST failed; treating the bootstrap as "
            "Indeterminate (fail-closed, will refuse to mint _pool_meta): {}",
            prefix, getCurrentExceptionMessage(/*with_stacktrace=*/false));
        return BootstrapResidual::Indeterminate;
    }
    if (has_pool_meta)
        return BootstrapResidual::PoolMetaPresent;
    if (has_residual)
        return BootstrapResidual::ResidualWithoutMeta;
    if (!has_catalog)
        return BootstrapResidual::EmptyOrProbeOnly;

    /// LIST is only a discovery hint. Before treating catalog-only residue as retryable, exact-read
    /// the listed key and prove it is precisely the sole canonical empty authority. A missing object,
    /// malformed body, alternate encoding, or nonempty catalog is ordinary residual data, never a
    /// license to mint `_pool_meta`.
    try
    {
        const auto got = op.read(catalog_key, Retry::standard());
        if (!got)
            return BootstrapResidual::ResidualWithoutMeta;

        RefCatalog catalog = decodeRefCatalog(got->bytes);
        const String canonical_empty = encodeRefCatalog(RefCatalog{});
        if (catalog.entries.empty() && got->bytes == canonical_empty)
            return BootstrapResidual::CanonicalEmptyCatalogOnly;
        return BootstrapResidual::ResidualWithoutMeta;
    }
    catch (...)
    {
        LOG_WARNING(getLogger("CasBootstrap"),
            "Pool prefix '{}': could not prove listed catalog '{}' is canonical empty; refusing bootstrap: {}",
            prefix, catalog_key, getCurrentExceptionMessage(/*with_stacktrace=*/false));
        return BootstrapResidual::ResidualWithoutMeta;
    }
}

}
