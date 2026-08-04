#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasInstrumentedBackend.h>

namespace ProfileEvents
{
/// The CA per-namespace and per-operation events declared in `ProfileEvents.cpp`.
extern const Event CASBlobPut;
extern const Event CASBlobPutDeduplicated;
extern const Event CASBlobOverwrite;
extern const Event CASBlobCompareSwap;
extern const Event CASBlobCompareSwapConflict;
extern const Event CASBlobHead;
extern const Event CASBlobHeadMiss;
extern const Event CASBlobGet;
extern const Event CASBlobGetStream;
extern const Event CASBlobDelete;
extern const Event CASBlobList;

extern const Event CASManifestPut;
extern const Event CASManifestPutDeduplicated;
extern const Event CASManifestOverwrite;
extern const Event CASManifestCompareSwap;
extern const Event CASManifestCompareSwapConflict;
extern const Event CASManifestHead;
extern const Event CASManifestHeadMiss;
extern const Event CASManifestGet;
extern const Event CASManifestGetStream;
extern const Event CASManifestDelete;
extern const Event CASManifestList;

extern const Event CASRootPut;
extern const Event CASRootPutDeduplicated;
extern const Event CASRootOverwrite;
extern const Event CASRootCompareSwap;
extern const Event CASRootCompareSwapConflict;
extern const Event CASRootHead;
extern const Event CASRootHeadMiss;
extern const Event CASRootGet;
extern const Event CASRootGetStream;
extern const Event CASRootDelete;
extern const Event CASRootList;

extern const Event CASGCPut;
extern const Event CASGCPutDeduplicated;
extern const Event CASGCOverwrite;
extern const Event CASGCCompareSwap;
extern const Event CASGCCompareSwapConflict;
extern const Event CASGCHead;
extern const Event CASGCHeadMiss;
extern const Event CASGCGet;
extern const Event CASGCGetStream;
extern const Event CASGCDelete;
extern const Event CASGCList;

extern const Event CASServerPut;
extern const Event CASServerPutDeduplicated;
extern const Event CASServerOverwrite;
extern const Event CASServerCompareSwap;
extern const Event CASServerCompareSwapConflict;
extern const Event CASServerHead;
extern const Event CASServerHeadMiss;
extern const Event CASServerGet;
extern const Event CASServerGetStream;
extern const Event CASServerDelete;
extern const Event CASServerList;

extern const Event CASOtherPut;
extern const Event CASOtherPutDeduplicated;
extern const Event CASOtherOverwrite;
extern const Event CASOtherCompareSwap;
extern const Event CASOtherCompareSwapConflict;
extern const Event CASOtherHead;
extern const Event CASOtherHeadMiss;
extern const Event CASOtherGet;
extern const Event CASOtherGetStream;
extern const Event CASOtherDelete;
extern const Event CASOtherList;
}

namespace DB::Cas
{

/// Maps `(CasNs, CasOp)` to the corresponding `ProfileEvents::Event`. The table is row-major: the
/// outer index is the namespace and the inner index is the operation. Its rows and columns must stay
/// in lockstep with the `CasNs` and `CasOp` enum orderings.
static const ProfileEvents::Event cas_event_table[CAS_NS_COUNT][CAS_OP_COUNT] =
{
    /* Blob   */ {ProfileEvents::CASBlobPut, ProfileEvents::CASBlobPutDeduplicated, ProfileEvents::CASBlobOverwrite,
                  ProfileEvents::CASBlobCompareSwap, ProfileEvents::CASBlobCompareSwapConflict, ProfileEvents::CASBlobHead,
                  ProfileEvents::CASBlobHeadMiss, ProfileEvents::CASBlobGet, ProfileEvents::CASBlobGetStream,
                  ProfileEvents::CASBlobDelete, ProfileEvents::CASBlobList},
    /* Manifest */ {ProfileEvents::CASManifestPut, ProfileEvents::CASManifestPutDeduplicated, ProfileEvents::CASManifestOverwrite,
                  ProfileEvents::CASManifestCompareSwap, ProfileEvents::CASManifestCompareSwapConflict, ProfileEvents::CASManifestHead,
                  ProfileEvents::CASManifestHeadMiss, ProfileEvents::CASManifestGet, ProfileEvents::CASManifestGetStream,
                  ProfileEvents::CASManifestDelete, ProfileEvents::CASManifestList},
    /* Root   */ {ProfileEvents::CASRootPut, ProfileEvents::CASRootPutDeduplicated, ProfileEvents::CASRootOverwrite,
                  ProfileEvents::CASRootCompareSwap, ProfileEvents::CASRootCompareSwapConflict, ProfileEvents::CASRootHead,
                  ProfileEvents::CASRootHeadMiss, ProfileEvents::CASRootGet, ProfileEvents::CASRootGetStream,
                  ProfileEvents::CASRootDelete, ProfileEvents::CASRootList},
    /* Gc     */ {ProfileEvents::CASGCPut, ProfileEvents::CASGCPutDeduplicated, ProfileEvents::CASGCOverwrite,
                  ProfileEvents::CASGCCompareSwap, ProfileEvents::CASGCCompareSwapConflict, ProfileEvents::CASGCHead,
                  ProfileEvents::CASGCHeadMiss, ProfileEvents::CASGCGet, ProfileEvents::CASGCGetStream,
                  ProfileEvents::CASGCDelete, ProfileEvents::CASGCList},
    /* Server */ {ProfileEvents::CASServerPut, ProfileEvents::CASServerPutDeduplicated, ProfileEvents::CASServerOverwrite,
                  ProfileEvents::CASServerCompareSwap, ProfileEvents::CASServerCompareSwapConflict, ProfileEvents::CASServerHead,
                  ProfileEvents::CASServerHeadMiss, ProfileEvents::CASServerGet, ProfileEvents::CASServerGetStream,
                  ProfileEvents::CASServerDelete, ProfileEvents::CASServerList},
    /* Other  */ {ProfileEvents::CASOtherPut, ProfileEvents::CASOtherPutDeduplicated, ProfileEvents::CASOtherOverwrite,
                  ProfileEvents::CASOtherCompareSwap, ProfileEvents::CASOtherCompareSwapConflict, ProfileEvents::CASOtherHead,
                  ProfileEvents::CASOtherHeadMiss, ProfileEvents::CASOtherGet, ProfileEvents::CASOtherGetStream,
                  ProfileEvents::CASOtherDelete, ProfileEvents::CASOtherList},
};

CasNs classifyCasNs(const String & key)
{
    if (key.find("/blobs/") != String::npos)
        return CasNs::Blob;
    /// Ref streams and namespace-owned state live under `cas/ns/`; part manifests are under
    /// `cas/manifests/<namespace>/`. These paths must be classified before
    /// the generic `roots/` and `Other` cases, otherwise the ref and manifest operation counters
    /// silently accumulate in the wrong namespace.
    if (key.find("/cas/ns/") != String::npos)
        return CasNs::Root;
    if (key.find("/cas/manifests/") != String::npos)
        return CasNs::Manifest;
    if (key.find("/roots/") != String::npos)
        return CasNs::Root;
    if (key.find("/gc/") != String::npos)
        return CasNs::Gc;
    return CasNs::Other;
}

void incrementCasEvent(CasNs ns, CasOp op)
{
    ProfileEvents::increment(cas_event_table[static_cast<size_t>(ns)][static_cast<size_t>(op)]);
}

namespace
{

/// Wraps an inner `WriteSink`. The namespace is captured at creation because the key is not available
/// at `finalize`; the `Put` versus `PutDeduplicated` outcome is emitted only after the inner sink returns.
/// Buffer access and cancellation delegate verbatim, while exceptions from the inner sink propagate.
class InstrumentedWriteSink final : public WriteSink
{
public:
    InstrumentedWriteSink(WriteSinkPtr inner_, CasNs ns_) : inner(std::move(inner_)), ns(ns_) {}

    WriteBuffer & buffer() override { return inner->buffer(); }

    /// Finalize the inner upload first, then count its returned outcome. No event is emitted if the
    /// inner operation throws.
    PutResult finalize() override
    {
        PutResult result = inner->finalize();
        incrementCasEvent(ns, result.outcome == PutOutcome::Done ? CasOp::Put : CasOp::PutDeduplicated);
        return result;
    }

    void cancel() noexcept override { inner->cancel(); }

private:
    WriteSinkPtr inner;
    CasNs ns;
};

}

WriteSinkPtr InstrumentedBackend::putIfAbsentStream(const String & key, const ObjectMeta & meta)
{
    WriteSinkPtr sink = inner->putIfAbsentStream(key, meta);
    if (!sink)
        return sink;
    return std::make_unique<InstrumentedWriteSink>(std::move(sink), classifyCasNs(key));
}

}
