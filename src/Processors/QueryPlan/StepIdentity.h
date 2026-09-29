#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>

#include <limits>
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
    /// Reserved tags, so that a witness digest can never collide with a content digest: every
    /// component of every digest is framed by a tag, a step's own tags are small per-step-type
    /// constants, and these two are out of that range. A step must not use them.
    static constexpr UInt64 WHOLE_OBJECT_WITNESS_TAG = std::numeric_limits<UInt64>::max();
    static constexpr UInt64 STEP_WIRE_ENCODING_TAG = std::numeric_limits<UInt64>::max() - 1;

    StepDigestWriter(WriteBuffer & out_, SerializedSetsRegistry & registry_);

    void addBool(UInt64 tag, bool value);
    void addVarUInt(UInt64 tag, UInt64 value);
    void addString(UInt64 tag, std::string_view value);
    void addStrings(UInt64 tag, const Names & value);
    void addSortDescription(UInt64 tag, const SortDescription & value);
    /// nullptr writes the absent marker.
    void addDAG(UInt64 tag, const ActionsDAG * dag);
    void addAbsent(UInt64 tag);

    /// Provenance witness: the address of an object the step owns stands in for content that has no
    /// canonical encoding. Equal address means literally the same object, hence equal content; a
    /// different address costs a merge and never produces a wrong one. See "Provenance witnesses" in
    /// Optimizations/Cascades/ARCHITECTURE.md. nullptr writes the absent marker.
    void addWitness(UInt64 tag, const void * ptr);

    /// The whole-object witness of the `IQueryPlanStep::writeFullDigest` default: pointer identity
    /// expressed inside the digest mechanism, for a step type that has no content digest yet and for
    /// an instance whose content digest is guarded off.
    void addWholeObjectWitness(const void * object);

    /// The step's wire encoding - changed serialization settings plus `serialize` bytes - as one
    /// framed component. Only a content `writeFullDigest` override calls it, and only on an instance
    /// it has established neither method throws for: this is the one place in the digest that can
    /// throw, which is why the guards live at the call site.
    void addStepWireEncoding(const IQueryPlanStep & step);

private:
    void addPayload(UInt64 tag, std::string_view payload);

    WriteBuffer & out;
    SerializedSetsRegistry & registry;
};

/// Canonical full digest: the shared preamble (serialization name, output header), then everything
/// the step writes through `writeFullDigest` - for a content step its wire encoding plus its framed
/// extras, for every other step one whole-object witness. The step description is deliberately
/// excluded: it is display-only. Total: defined for every step. The only per-step throwing component
/// (the wire encoding) is written only by a step that has established its own instance encodes, so
/// nothing under `writeFullDigest` throws. The preamble is outside those guards and does have one
/// throw path: `encodeDataType` raises `UNSUPPORTED_METHOD` for a type it cannot binary-encode. That
/// is unreachable in practice - such a type could not be a plan step's output header, since the same
/// encoding is what plan serialization uses - but it is not a guarantee this function makes.
/// Both the full fingerprint and the byte-exact full comparison in
/// Optimizations/Cascades/StepIdentity.h go through it so they cannot diverge.
/// How to give a step a content digest, or classify a new field:
/// Optimizations/Cascades/ARCHITECTURE.md, "Step digests and cross-group identity".
void writeStepFullDigest(const IQueryPlanStep & step, WriteBuffer & out);

/// Canonical logical digest: the same preamble (serialization name, output header), then the
/// relation-defining content the step writes through `writeLogicalDigest`. It embeds no wire
/// `serialize` bytes and no `serializeSettings`: those interleave relation-defining and physical
/// fields with no markers and cannot be filtered without parsing, so the step authors its whole
/// logical content through the framing writer instead. Caller guarantees `step.hasLogicalDigest()`.
/// Everything `StepDigestWriter` says about framing and about digest lifetime applies here too.
/// How a field is classified as relation-defining: Optimizations/Cascades/ARCHITECTURE.md,
/// "Step digests and cross-group identity".
void writeStepLogicalDigest(const IQueryPlanStep & step, WriteBuffer & out);

}
