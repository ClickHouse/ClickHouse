#pragma once

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

/// The outline section (Section A) of the v5 query-plan envelope: the stable, common per-node
/// data of every plan — tree shape, step names and format versions, descriptions, output headers,
/// changed settings, payload sizes — stored in front of the step payloads.
///
/// Properties this layout provides:
///  - a reader can validate that the whole plan is decodable (every step name and format version
///    known, every non-ignorable setting known, every set kind known) without touching a single
///    payload byte — see validateQueryPlanOutline;
///  - the plan shape can be rendered even when some steps are unknown or carry a newer format
///    version — see formatQueryPlanOutline;
///  - payloads become independently framed byte ranges, read one at a time in outline order.
///
/// The outline wire layout is frozen append-only: future additions go into each node's
/// extension_bytes bytes, which older readers skip, so shape rendering keeps working across plan
/// versions.
struct PlanOutline
{
    /// name + flags (bit 0: ignorable) + length-prefixed setting-field value bytes.
    using SettingEntry = QueryPlanSerializationSettings::SerializedEntry;

    struct Node
    {
        UInt64 child_count = 0;
        String step_name;                       /// QueryPlanStepRegistry key
        UInt64 step_format_version = 1;
        /// The oldest payload format able to prefix-read this node's payload. A reader that knows
        /// less than this rejects the plan rather than reading a restructured payload positionally.
        UInt64 payload_prefix_readable_from = 1;
        /// The oldest plan version able to read this node's content ("needed to read"), computed
        /// by the writer from the step's registry info, its value-dependent requirements, header
        /// types and settings. Lets a rejection name the blocking step.
        UInt64 min_reader_plan_version = 0;
        String step_description;
        SharedHeader header;                    /// nullptr for a step with no output header
        std::vector<SettingEntry> settings;
        UInt64 payload_size = 0;                /// size of this node's slice of the payload section
        String extension_bytes;                      /// empty in v5; skipped by readers that do not know it
    };

    /// Nodes in left-to-right post-order over the serialized tree (Delayed* steps elided, as in
    /// the plan walk), so every child precedes its parent and the root is the last node.
    std::vector<Node> nodes;

    struct SetEntry
    {
        FutureSet::Hash hash;
        UInt8 kind = 0;                         /// SetSerializationKind; unknown value = validation issue
        UInt64 payload_size = 0;
    };

    /// Sorted by the 128-bit hash.
    std::vector<SetEntry> sets;
};

/// Writes Section A: VarUInt outline_size, then the outline bytes.
void writeQueryPlanOutline(const PlanOutline & outline, WriteBuffer & out);

/// Reads Section A written by writeQueryPlanOutline. Bounded: never reads past outline_size,
/// rejects trailing bytes inside the frame and any size that exceeds the declared bounds.
/// `max_frame_bytes` caps the outline frame itself and every payload size it declares; pass the
/// size of the envelope holding them, since nothing inside it can be larger than that.
/// Frame-layer violations throw CANNOT_PARSE_QUERY_PLAN; errors from nested codecs (e.g. header
/// type decoding) keep their own codes and surface at the frame boundary.
PlanOutline readQueryPlanOutline(ReadBuffer & in, size_t max_type_complexity, UInt64 max_frame_bytes);

/// The tree rebuilt from the outline's child counts.
struct PlanOutlineShape
{
    /// Per node, its children left to right. Meaningful only when `ok()`.
    std::vector<std::vector<size_t>> children;
    std::vector<String> issues;

    bool ok() const { return issues.empty(); }
};

/// Rebuilds the tree from `child_count`. Nodes are in left-to-right post-order, so every child
/// precedes its parent: each node takes the `child_count` most recent subtrees that nothing has
/// claimed yet, and the single subtree left at the end is the root (the last node).
PlanOutlineShape reconstructOutlineShape(const PlanOutline & outline);

struct QueryPlanOutlineValidationResult
{
    /// Human-readable issues; empty means the reader is able to decode the whole plan.
    std::vector<String> issues;
    /// The tree the outline describes, so the caller does not rebuild what was checked here.
    /// Meaningful only when `ok()`.
    PlanOutlineShape shape;

    bool ok() const { return issues.empty(); }
    /// All issues joined into one message, so a mixed-version error reports everything at once.
    String describe() const;
};

/// Capability check against this binary's registry and settings: verifies the reader has the
/// handlers and metadata needed to decode the plan (step names, step format versions vs the
/// registry info, non-ignorable settings, set kinds, structural consistency), and cross-checks
/// the writer's declared per-node "needed to read" versions against this binary's registry info
/// (a writer that undercounted is reported instead of silently misexecuting on old readers).
/// It does not check that payload bytes are well-formed, nor that referenced tables exist.
/// Collects all issues over a syntactically parseable outline.
QueryPlanOutlineValidationResult validateQueryPlanOutline(
    const PlanOutline & outline, UInt64 head_min_reader_plan_version);

/// The oldest plan version whose readers understand this type's binary encoding. All current
/// encodings predate the outline format, so this returns the base version; a new `BinaryTypeIndex`
/// entry must add its introduced-at version here so plans using it demand new enough readers.
UInt64 minReaderVersionForType(const IDataType & type);

/// EXPLAIN-style rendering from the outline alone. Unknown steps render as placeholders with
/// their payload size; no payload is decoded and no catalog/storage work is done.
String formatQueryPlanOutline(const PlanOutline & outline);

/// Envelope sets channel (Section C): fills outline.sets (sorted by hash) and one payload per
/// entry. A subquery set's payload is a complete serialized plan (with its own leading version).
/// Also raises min_reader_plan_version with the sets' own requirements: tuple-set column types
/// and, for subquery sets, the nested plan's declared "needed to read" version (recursively, so
/// an old reader can never accept the outer stream and fail mid-set decode).
void serializeEnvelopeSets(
    SerializedSetsRegistry & registry,
    const QueryPlan::SerializationFlags & flags,
    PlanOutline & outline,
    std::vector<String> & payloads,
    UInt64 & min_reader_plan_version);

/// Reads Section C per the outline's set entries; every payload must consume its frame exactly.
QueryPlanAndSets deserializeEnvelopeSets(
    QueryPlan plan,
    DeserializedSetsRegistry & registry,
    const PlanOutline & outline,
    ReadBuffer & in,
    const QueryPlan::SerializationFlags & flags,
    const ContextPtr & context,
    size_t max_type_complexity);

}
