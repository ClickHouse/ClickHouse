#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasProbe.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

namespace DB::Cas
{

void runCapabilityProbe(Backend & backend, const String & probe_prefix)
{
    // Probe key used for the primary battery steps.
    // Sub-directory style ("probe_prefix/token") ensures that list(probe_prefix, …) works for both the
    // in-memory backend (prefix match) and the LocalObjectStorage backend (directory listing).
    const String key = probe_prefix + "/token";
    // Probe key used for the casPut chain.
    const String cas_key = probe_prefix + "/cas";

    // Best-effort cleanup — runs at function exit regardless of outcome.
    // We capture the keys we need to clean up.
    auto cleanup = [&]() noexcept
    {
        // Skip the delete when HEAD says the key is already gone (the happy path: step 8 deleted
        // it). A deleteExact with the absent HeadResult's EMPTY token is a malformed conditional
        // op — AWS S3 answers 400 InvalidArgument ("If-Match cannot be empty"), which lands as a
        // scary AWSClient <Error> log line on every mount even though the catch swallows it.
        for (const auto & k : {key, cas_key})
        {
            try
            {
                const auto h = backend.head(k);
                if (h.exists)
                    backend.deleteExact(k, h.token);
            }
            catch (...) {} /// NOLINT(bugprone-empty-catch)
        }
    };

    try
    {
        // ---- Step 0: store-level preconditions (backend-specific; throws = mount refused). ----
        backend.checkPoolPreconditions();

        // ---- Step 0b: conditional writes must use one underlying HTTP attempt. Transparent SDK
        // retries can outlive the writer's mount lease and hide whether a conditional operation
        // committed; CAS retries must instead be explicit and state-aware. Throws = mount refused.
        // Keep this separate from Step 0 so each precondition remains independently unit-testable. ----
        backend.checkConditionalWriteSingleAttemptSupport();

        // ---- Step 1: putIfAbsent fresh → Done; read-after-write returns the bytes. ----
        Token t1;
        {
            const auto res = backend.putIfAbsent(key, "probe-v1");
            t1 = res.token;
            if (res.outcome != PutOutcome::Done)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putIfAbsent on a fresh key returned PreconditionFailed — backend is unexpectedly occupied or broken");
        }
        {
            const auto g = backend.get(key);
            if (!g.has_value() || g->bytes != "probe-v1")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: read-after-write failed — putIfAbsent succeeded but the object is not readable");
        }

        // ---- Step 2: putIfAbsent same key → PreconditionFailed; bytes intact. ----
        {
            const auto outcome = backend.putIfAbsent(key, "should-not-land").outcome;
            if (outcome != PutOutcome::PreconditionFailed)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putIfAbsent on an existing key was not rejected (PreconditionFailed expected) — "
                    "backend does not enforce conditional create");
            const auto g = backend.get(key);
            if (!g.has_value() || g->bytes != "probe-v1")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putIfAbsent conflict was 'reported' but the original bytes were clobbered — "
                    "backend does not enforce conditional create");
        }

        // ---- Step 3: putOverwrite wrong token → PreconditionFailed; bytes intact. ----
        {
            /// Wrong-token values are NUMERIC on purpose: a generation-dialect backend (GCS)
            /// validates the If-Match FORMAT client-side and throws on a non-numeric value (an
            /// ETag-kind token leaking into a generation dialect) — the probe's synthetic wrong
            /// tokens must be format-valid for EVERY token kind, merely guaranteed-wrong. A huge
            /// numeric is a wrong ETag on AWS (412), a wrong generation on GCS (412), and a wrong
            /// sequence on the emulated backends (TokenMismatch).
            ///
            /// The TYPE must be the LIVE dialect (t1.type, just observed from this same backend),
            /// never a hardcoded TokenType::Emulated: a backend that mints a different dialect
            /// (e.g. Native/ETag) rejects a foreign-dialect token locally, before the wrong VALUE
            /// ever reaches the wire — which would make this check pass vacuously against a
            /// non-enforcing store instead of proving enforcement (codex-review-triage §3.18,
            /// Critical: the №19 local dialect guard must not defeat this probe).
            Token wrong_token{"900000000000000001", t1.type};
            const auto outcome = backend.putOverwrite(key, "clobbered", wrong_token).outcome;
            if (outcome != PutOutcome::PreconditionFailed)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putOverwrite with a wrong token was not rejected (PreconditionFailed expected) — "
                    "backend does not enforce conditional overwrite");
            const auto g = backend.get(key);
            if (!g.has_value() || g->bytes != "probe-v1")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putOverwrite with wrong token was 'rejected' but the original bytes were clobbered");
        }

        // ---- Step 4: putOverwrite correct token → Done; bytes replaced; token changed. ----
        Token t2;
        {
            const auto res = backend.putOverwrite(key, "probe-v2", t1);
            t2 = res.token;
            if (res.outcome != PutOutcome::Done)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putOverwrite with the correct token was rejected — backend does not accept valid overwrite");
            if (t2 == t1)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putOverwrite succeeded but did not mint a new token — tokens must change on every write");
            const auto g = backend.get(key);
            if (!g.has_value() || g->bytes != "probe-v2")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: putOverwrite succeeded but the new bytes are not readable");
        }

        // ---- Step 5: casPut chain. ----
        // 5a: create-if-absent (nullopt expected).
        Token ct1;
        {
            const auto res = backend.casPut(cas_key, "cas-s1", std::nullopt);
            ct1 = res.token;
            if (res.outcome != CasOutcome::Committed)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: casPut create-if-absent (nullopt expected) was not committed — "
                    "backend does not support CAS create-if-absent");
        }
        // 5b: conflict on existing (nullopt expected, but key exists).
        {
            const auto outcome = backend.casPut(cas_key, "cas-s1x", std::nullopt).outcome;
            if (outcome != CasOutcome::Conflict)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: casPut with nullopt expected against an existing key was not Conflict — "
                    "backend does not enforce create-if-absent semantics on casPut");
        }
        // 5c: conflict on stale token.
        {
            Token stale{"900000000000000002", ct1.type};   /// numeric + live dialect: see step 3
            const auto outcome = backend.casPut(cas_key, "cas-s1y", stale).outcome;
            if (outcome != CasOutcome::Conflict)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: casPut with a stale token was not Conflict — "
                    "backend does not enforce token-exact CAS");
        }
        // Bytes must still be the original.
        {
            const auto g = backend.get(cas_key);
            if (!g.has_value() || g->bytes != "cas-s1")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: casPut conflicts were reported but the original bytes were altered");
        }
        // 5d: commit on current token.
        {
            const auto res = backend.casPut(cas_key, "cas-s2", ct1);
            if (res.outcome != CasOutcome::Committed)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: casPut with the current token was not committed — "
                    "backend does not honor casPut with matching token");
            const auto g = backend.get(cas_key);
            if (!g.has_value() || g->bytes != "cas-s2")
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: casPut committed but new bytes are not readable");
        }

        // ---- Step 6: deleteExact wrong token → TokenMismatch AND the object still readable. ----
        {
            Token wrong_token{"900000000000000003", t2.type};   /// numeric + live dialect: see step 3
            const auto d = backend.deleteExact(key, wrong_token);
            if (d.kind != DeleteOutcome::Kind::TokenMismatch)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: deleteExact with a wrong token was not TokenMismatch — "
                    "delete with mismatching token was honored — backend does not enforce conditional deletes");
            const auto g = backend.get(key);
            if (!g.has_value())
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: deleteExact with a wrong token was rejected (correctly) but the object was deleted anyway — "
                    "backend does not enforce conditional deletes");
        }

        // ---- Step 7: list(probe_prefix) contains the probe key (list-after-write). ----
        {
            const auto page = backend.list(probe_prefix, "", 100);
            bool found = false;
            for (const auto & listed : page.keys)
            {
                if (listed.key == key)
                {
                    found = true;
                    break;
                }
            }
            if (!found)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: list-after-write failed — the probe key '{}' is not visible in the listing under prefix '{}'",
                    key, probe_prefix);
        }

        // ---- Step 8: deleteExact correct token → Deleted; object gone; no delete marker;
        //              list no longer contains the key. ----
        {
            const auto d = backend.deleteExact(key, t2);
            if (d.kind != DeleteOutcome::Kind::Deleted)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: deleteExact with the correct token was not Deleted — backend rejected a valid token-exact delete");
            if (d.created_delete_marker)
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: deleteExact succeeded but created a versioning delete marker — the bucket has "
                    "object VERSIONING enabled, and a content-addressed pool cannot run on a versioned bucket: "
                    "every GC delete would archive a noncurrent version instead of reclaiming storage (the bucket "
                    "grows forever), and the constantly-rewritten ref objects would pile up versions on every "
                    "commit. This is NOT ignorable and has no override. Use a bucket where versioning was NEVER "
                    "enabled — note that merely SUSPENDING versioning is not enough (deletes on a "
                    "versioning-suspended bucket still mint delete markers, so this probe will refuse again)");
            const auto g = backend.get(key);
            if (g.has_value())
                throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                    "CasProbe: deleteExact succeeded (Deleted) but the object is still readable — backend delete is not effective");
            // List-after-delete.
            const auto page = backend.list(probe_prefix, "", 100);
            for (const auto & listed : page.keys)
            {
                if (listed.key == key)
                    throw DB::Exception(DB::ErrorCodes::NOT_IMPLEMENTED,
                        "CasProbe: list-after-delete failed — the deleted probe key '{}' is still visible in the listing under prefix '{}'",
                        key, probe_prefix);
            }
        }

        // ---- Step 9: cleanup (best-effort; also deletes cas_key). ----
        // cas_key is still alive — clean it up via its current token.
        {
            const auto h = backend.head(cas_key);
            if (h.exists)
                backend.deleteExact(cas_key, h.token);
        }
    }
    catch (...)
    {
        // Best-effort cleanup on failure path before re-throwing.
        cleanup();
        throw;
    }

    // Normal-exit cleanup (cas_key was cleaned inside the try; key was deleted in step 8).
    // Call cleanup anyway to handle any partial state edge cases — it is a no-op if keys are gone.
    cleanup();
}

}
