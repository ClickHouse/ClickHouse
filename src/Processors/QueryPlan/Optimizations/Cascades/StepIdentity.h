#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <base/extended_types.h>

#include <atomic>
#include <memory>
#include <string_view>

namespace DB
{

class ActionsDAG;
class IQueryPlanStep;
class WriteBuffer;
struct SerializedSetsRegistry;

/// Sink for the non-wire fields of a step that constrain execution but are not written by its
/// `serialize`. The framing is injective: every component writes its tag, a presence byte and,
/// when present, an explicit payload size before the payload bytes. So no reassignment of the
/// same values across tags, and no re-split of adjacent variable-length components, can produce
/// the same byte string. Tags are per-step-type constants, unique within the step; a step must
/// append the same tags in the same order on every call.
class CascadesIdentityExtras
{
public:
    CascadesIdentityExtras(WriteBuffer & out_, SerializedSetsRegistry & registry_);

    void addBool(UInt64 tag, bool value);
    void addVarUInt(UInt64 tag, UInt64 value);
    void addString(UInt64 tag, std::string_view value);
    void addStrings(UInt64 tag, const Names & value);
    void addSortDescription(UInt64 tag, const SortDescription & value);
    /// nullptr writes the absent marker.
    void addDAG(UInt64 tag, const ActionsDAG * dag);
    void addAbsent(UInt64 tag);

private:
    void addPayload(UInt64 tag, std::string_view payload);

    WriteBuffer & out;
    SerializedSetsRegistry & registry;
};

/// Canonical identity encoding: serialization name, output header, changed serialization settings,
/// wire `serialize` bytes, then the framed extras. The step description is deliberately excluded:
/// it is display-only. Caller guarantees `step.supportsCascadesIdentity()`.
/// This is the only encoding writer - both the hash and the byte-exact comparison go through it so
/// they cannot diverge.
void writeCascadesIdentityEncoding(const IQueryPlanStep & step, WriteBuffer & out);

/// Content-based cross-group identity of a step. Unlike `GroupExpression::structurallyEqualTo`,
/// which compares step name and description, this compares the encoding of the step's content.
struct StepIdentity
{
    UInt128 hash;
    /// The step the hash was computed from; a mismatch means the cache is stale.
    std::shared_ptr<const IQueryPlanStep> step;
};

/// Streams the encoding through SipHash without materializing the bytes.
UInt128 computeCascadesIdentityHash(const IQueryPlanStep & step);

/// Materializes both encodings and compares them byte for byte. Confirms a hash match; never a
/// substitute for it.
bool cascadesIdentityEncodingsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs);

/// Cost of the identity machinery, for the performance gate on memo-wide deduplication.
struct CascadesIdentityMetrics
{
    /// Completed encoding passes, whether they produced a hash or bytes.
    static std::atomic<UInt64> encoded_steps;
    /// Total bytes produced by those passes.
    static std::atomic<UInt64> encoded_bytes;
    /// Encodings recomputed only to confirm a hash match byte-exactly.
    static std::atomic<UInt64> exact_reencodes;

    static void reset();
};

}
