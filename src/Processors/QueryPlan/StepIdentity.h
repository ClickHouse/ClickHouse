#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>

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
/// Only the tags and their order are stable, not the values: a step may hold `mutable` state that
/// populates lazily (`ReadFromMergeTree` memoizes its index analysis and its analysis result while
/// being costed), so the same step can encode to different bytes before and after that. Equality is
/// therefore defined only over two live steps re-digested at compare time, which is what
/// `stepFullDigestsEqual` does; a cached fingerprint can go stale and only ever lose a merge.
/// Do not cache the digest bytes and compare them against a step digested at another time.
/// Same reason a fingerprint taken at insertion into a memo-wide index goes stale as lazy analysis
/// state populates: such an index must store the insertion-time fingerprint alongside the
/// expression, and must never look up or remove an expression by recomputing its current
/// fingerprint.
class StepDigestWriter
{
public:
    StepDigestWriter(WriteBuffer & out_, SerializedSetsRegistry & registry_);

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

/// Canonical full digest: serialization name, output header, changed serialization settings,
/// wire `serialize` bytes, then the framed extras. The step description is deliberately excluded:
/// it is display-only. Caller guarantees `step.supportsCascadesIdentity()`.
/// This is the only digest writer - both the fingerprint and the byte-exact comparison in
/// Optimizations/Cascades/StepIdentity.h go through it so they cannot diverge.
/// How to opt a step in, or classify a new field: Optimizations/Cascades/ARCHITECTURE.md,
/// "Step digests and cross-group identity".
void writeStepFullDigest(const IQueryPlanStep & step, WriteBuffer & out);

}
