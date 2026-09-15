#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CAS_DELETE_MARKER;
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

namespace
{

/// `op.remove`'s own delete-marker signal (`CAS_DELETE_MARKER`, thrown outside the attempt loop because
/// a versioned bucket answers this way every time) is re-signalled here as the operator-facing message
/// the probe has always thrown for it — everything else propagates unchanged. Every `remove` the battery
/// issues against a live incarnation goes through this: a store that ignores the delete precondition AND
/// mints delete markers must still be reported with this message, not the engine's terse one.
Removal removeOrReportDeleteMarker(CasOperation & op, const String & key, const Etag & seen)
{
    try
    {
        return op.remove(key, seen, Retry::standard());
    }
    catch (const DB::Exception & e)
    {
        if (e.code() != DB::ErrorCodes::CAS_DELETE_MARKER)
            throw;
        throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
            "CasProbe: remove succeeded but created a versioning delete marker — the bucket has "
            "object VERSIONING enabled, and a content-addressed pool cannot run on a versioned bucket: "
            "every GC delete would archive a noncurrent version instead of reclaiming storage (the bucket "
            "grows forever), and the constantly-rewritten ref objects would pile up versions on every "
            "commit. This is NOT ignorable and has no override. Use a bucket where versioning was NEVER "
            "enabled — note that merely SUSPENDING versioning is not enough (deletes on a "
            "versioning-suspended bucket still mint delete markers, so this probe will refuse again)");
    }
}

}

void runCapabilityProbe(CasOperation & op, const String & probe_prefix)
{
    // Sub-directory style ("probe_prefix/token") ensures that list(probe_prefix, …) works for both the
    // in-memory backend (prefix match) and the LocalObjectStorage backend (directory listing).
    const String key = probe_prefix + "/token";

    // Best-effort cleanup — runs at function exit regardless of outcome.
    auto cleanup = [&]() noexcept
    {
        // Skip the remove when HEAD says the key is already gone (the happy path: the battery's own
        // delete already ran). `Etag` can only be minted from an actual HEAD/read observation, so
        // an unconditional "delete with whatever precondition" this backend never saw is not
        // constructible here — the gate below is the only way to reach `remove` at all.
        try
        {
            const auto h = op.head(key, Retry::standard());
            if (h)
                op.remove(key, h->etag, Retry::standard());
        }
        catch (...) {} /// NOLINT(bugprone-empty-catch)
    };

    try
    {
        // ---- Step 1: create fresh -> Committed; read-after-write returns the bytes. ----
        Etag t1 = [&]
        {
            WriteResult r = op.create(key, "probe-v1", Retry::standard());
            if (!std::holds_alternative<Committed>(r))
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: create on a fresh key did not commit — backend is unexpectedly occupied or broken");
            return std::get<Committed>(r).etag;
        }();
        {
            const auto g = op.read(key, Retry::standard());
            if (!g || g->bytes != "probe-v1")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: read-after-write failed — create succeeded but the object is not readable");
        }

        // ---- Step 2: create the same key again -> Conflict; bytes intact. ----
        {
            WriteResult r = op.create(key, "should-not-land", Retry::standard());
            const auto * conflict = std::get_if<Conflict>(&r);
            if (!conflict)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: create on an existing key was not rejected (a conflict was expected) — "
                    "backend does not enforce conditional create");
            const auto * seen = std::get_if<Object>(&conflict->seen);
            if (!seen || seen->bytes != "probe-v1")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: create's conflict was reported but the original bytes were clobbered — "
                    "backend does not enforce conditional create");
        }

        // ---- Step 3: replace against the CURRENT incarnation (t1) -> Committed; incarnation changed;
        //              bytes replaced. Every "wrong incarnation" step below reuses THIS key's own prior
        //              incarnations (never a synthetic value) — an `Etag` is minted only from an
        //              actual backend observation, so there is no other way to name one that is
        //              guaranteed wrong yet dialect-valid. ----
        Etag t2 = [&]
        {
            WriteResult r = op.replace(key, "probe-v2", t1, Retry::standard());
            if (!std::holds_alternative<Committed>(r))
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: replace with the correct incarnation was rejected — backend does not accept a valid overwrite");
            Etag next = std::get<Committed>(r).etag;
            if (next == t1)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: replace succeeded but did not mint a new incarnation — an incarnation must "
                    "change on every write");
            return next;
        }();
        {
            const auto g = op.read(key, Retry::standard());
            if (!g || g->bytes != "probe-v2")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: replace succeeded but the new bytes are not readable");
        }

        // ---- Step 4: replace against t1, now STALE (the key committed to t2 in step 3) -> Conflict;
        //              bytes intact. ----
        {
            WriteResult r = op.replace(key, "clobbered", t1, Retry::standard());
            const auto * conflict = std::get_if<Conflict>(&r);
            if (!conflict)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: replace with a stale incarnation was not rejected (a conflict was expected) — "
                    "backend does not enforce conditional overwrite");
            const auto * seen = std::get_if<Object>(&conflict->seen);
            if (!seen || seen->bytes != "probe-v2")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: replace with a stale incarnation was 'rejected' but the original bytes were clobbered");
        }

        // ---- Step 5: remove with a STALE incarnation (t1) -> Mismatch; the object survives. A store
        //              that ignores this precondition removes the object here, so this path can reach a
        //              delete marker exactly like step 7's — route it through the same reporter. ----
        {
            const Removal d = removeOrReportDeleteMarker(op, key, t1);
            if (d != Removal::Mismatch)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: remove with a stale incarnation was not rejected (a mismatch was expected) — "
                    "backend does not enforce conditional deletes");
            const auto g = op.read(key, Retry::standard());
            if (!g)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: remove with a stale incarnation was rejected (correctly) but the object was deleted anyway — "
                    "backend does not enforce conditional deletes");
        }

        // ---- Step 6: list(probe_prefix) contains the probe key (list-after-write). ----
        {
            bool found = false;
            op.forEachListedKey(probe_prefix, [&](const ListedKey & listed) -> bool
            {
                if (listed.key != key)
                    return true;
                found = true;
                return false;
            }, Retry::standard());
            if (!found)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: list-after-write failed — the probe key '{}' is not visible in the listing under prefix '{}'",
                    key, probe_prefix);
        }

        // ---- Step 7: remove with the CORRECT incarnation (t2) -> Removed; no delete marker; object
        //              gone; list no longer contains the key. ----
        {
            const Removal d = removeOrReportDeleteMarker(op, key, t2);
            if (d != Removal::Removed)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: remove with the correct incarnation was not Removed — backend rejected a valid incarnation-exact delete");
            const auto g = op.read(key, Retry::standard());
            if (g)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: remove succeeded (Removed) but the object is still readable — backend delete is not effective");
            bool still_listed = false;
            op.forEachListedKey(probe_prefix, [&](const ListedKey & listed) -> bool
            {
                if (listed.key != key)
                    return true;
                still_listed = true;
                return false;
            }, Retry::standard());
            if (still_listed)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: list-after-delete failed — the deleted probe key '{}' is still visible in the listing under prefix '{}'",
                    key, probe_prefix);
        }
    }
    catch (...)
    {
        // Best-effort cleanup on failure path before re-throwing.
        cleanup();
        throw;
    }

    // Normal-exit cleanup (the key was already deleted in step 7). Call cleanup anyway to handle any
    // partial state edge cases — it is a no-op if the key is gone.
    cleanup();
}

}
