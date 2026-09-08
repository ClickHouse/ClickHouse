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

/// The outline: the front part of a serialized query plan, carrying the plan-level limits and the
/// data every step has in common - tree shape, step names and payload format versions, descriptions,
/// output headers, changed settings. Its front header holds the node and set counts, so a reader
/// knows how many payloads follow. Each payload then carries its own size inline, right before its
/// bytes, so the reader walks the plan one payload at a time.
///
/// Keeping all of that in front lets a reader:
///  - check it can decode the whole plan (every step name and payload format known, every setting
///    it has to act on known, every set kind known) before reading one payload byte, see
///    `validateQueryPlanOutline`;
///  - print the plan's shape even when some steps are unknown or newer, see
///    `formatQueryPlanOutline`;
///  - read one payload at a time instead of holding the whole plan.
///
/// A larger change picks a new `format_kind` in the head, which an older reader turns down on the
/// kind alone. Otherwise the deciding checks are the step names, settings and set kinds: a reader
/// runs a plan only when it knows every one, and refuses cleanly otherwise. The plan version is a
/// coarse gate on top of that - a reader refuses a version above the one it supports.
struct PlanOutline
{
    /// name + flags (bit 0: ignorable) + length-prefixed setting-field value bytes.
    using SettingEntry = QueryPlanSerializationSettings::SerializedEntry;

    /// Limits that belong to the plan as a whole, not to a step. Without them a plan fragment would
    /// run with default limits after it is read back.
    UInt64 max_threads = 0;
    bool concurrency_control = false;
    /// Step descriptions are debug text; they travel only when a writer asks for them, so a normal
    /// plan does not pay for them. A reader learns from this whether each node carries one.
    bool include_step_descriptions = false;

    struct Node
    {
        /// Indices of this node's input steps in `nodes`, left to right. Explicit edges rather than a
        /// count leave room for a step shared by several parents (a DAG), not only a tree.
        std::vector<UInt64> children;
        String step_name;                       /// QueryPlanStepRegistry key
        /// The payload format. Each step name owns exactly one payload layout, so this is always 1;
        /// a format change takes a new step name, not a higher number here. The reader refuses any
        /// other value. Kept as a reserved field on the wire.
        UInt64 step_format_version = 1;
        String step_description;
        SharedHeader header;                    /// nullptr for a step with no output header
        std::vector<SettingEntry> settings;
    };

    /// Every child comes before its parent and the root is last. `Delayed*` steps are skipped here,
    /// the same way the plan walk skips them.
    std::vector<Node> nodes;

    struct SetEntry
    {
        FutureSet::Hash hash;
        UInt8 kind = 0;                         /// SetSerializationKind; unknown value = validation issue
    };

    /// Sorted by the 128-bit hash.
    std::vector<SetEntry> sets;
};

/// Writes the outline: its size as a varint, then its bytes.
void writeQueryPlanOutline(const PlanOutline & outline, WriteBuffer & out);

/// Reads an outline written by `writeQueryPlanOutline`. Never reads past the size the outline
/// declares, and rejects both leftover bytes inside it and any size beyond the limits.
/// `max_frame_bytes` caps the outline frame; pass the plan-size limit, since the outline cannot be
/// larger. A broken layout throws `INCORRECT_DATA`; errors from what it decodes (a header type,
/// say) keep their own codes.
PlanOutline readQueryPlanOutline(ReadBuffer & in, size_t max_type_complexity, UInt64 max_frame_bytes);

/// The node and set counts, read from the front of the outline frame and nothing more: no step,
/// header or setting is decoded. A reader that only wants to take a plan off the stream (it is
/// draining, or the plan is one it cannot build) needs just these two counts to know how many sized
/// payload frames follow the outline. `max_frame_bytes` caps the outline frame the same way
/// `readQueryPlanOutline` does.
struct OutlineFrameCounts
{
    UInt64 node_count = 0;
    UInt64 set_count = 0;
};
OutlineFrameCounts readOutlineFrameCounts(ReadBuffer & in, UInt64 max_frame_bytes);

/// The tree rebuilt from the outline's child counts.
struct PlanOutlineShape
{
    /// Per node, its children left to right. Meaningful only when `ok()`.
    std::vector<std::vector<size_t>> children;
    std::vector<String> issues;

    bool ok() const { return issues.empty(); }
};

/// Rebuilds the shape from each node's explicit child indices. A child index must point to an
/// earlier node, so the graph is acyclic; the one node nothing points to is the root, and the writer
/// emits it last.
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

/// Checks that this server can decode the plan the outline describes: it knows every step name and
/// payload format, every setting it would have to act on, every set kind, and the tree shape holds
/// together. It does not look at payload bytes and does not check that tables exist.
/// Reports every problem it finds, not just the first.
QueryPlanOutlineValidationResult validateQueryPlanOutline(const PlanOutline & outline);

/// EXPLAIN-style rendering from the outline alone. Unknown steps render as placeholders marked
/// unknown; no payload is decoded and no catalog/storage work is done.
String formatQueryPlanOutline(const PlanOutline & outline);

/// Writes the sets the plan refers to: fills `outline.sets`, sorted by hash, and one payload per
/// entry. A subquery set's payload is a whole serialized plan, version and all.
void serializeFramedSets(
    SerializedSetsRegistry & registry,
    const QueryPlan::SerializationFlags & flags,
    PlanOutline & outline,
    std::vector<String> & payloads);

/// Reads the set payloads that follow the plan nodes, one sized frame each (a size then that many
/// bytes). Each set must consume exactly its own frame. `body_start` is `in.count()` at the start
/// of the plan body and `max_plan_bytes` the plan-size limit, so the running total stays bounded;
/// `frames_consumed` is the number of payload frames already read (the plan nodes), advanced here
/// as each set is read, so a caller draining after a failure knows how many frames are left.
QueryPlanAndSets deserializeFramedSets(
    QueryPlan plan,
    DeserializedSetsRegistry & registry,
    const PlanOutline & outline,
    ReadBuffer & in,
    const QueryPlan::SerializationFlags & flags,
    const ContextPtr & context,
    size_t max_type_complexity,
    UInt64 max_plan_bytes,
    size_t body_start,
    UInt64 & frames_consumed);

}
