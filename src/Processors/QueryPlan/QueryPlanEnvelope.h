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
/// data every step has in common - tree shape, step names and payload format versions, descriptions, output headers,
/// changed settings, payload sizes. The step payloads follow it, one sized byte range each.
///
/// Keeping all of that in front lets a reader:
///  - check it can decode the whole plan (every step name and payload format known, every setting
///    it has to act on known, every set kind known) before reading one payload byte, see
///    `validateQueryPlanOutline`;
///  - print the plan's shape even when some steps are unknown or newer, see
///    `formatQueryPlanOutline`;
///  - read one payload at a time instead of holding the whole plan.
///
/// The layout only ever grows at the end: new per-node data goes into `extension_bytes`, which
/// older readers skip, so both of those keep working for plans written by newer servers.
struct PlanOutline
{
    /// name + flags (bit 0: ignorable) + length-prefixed setting-field value bytes.
    using SettingEntry = QueryPlanSerializationSettings::SerializedEntry;

    /// Limits that belong to the plan as a whole, not to a step. Without them a plan fragment would
    /// run with default limits after it is read back.
    UInt64 max_threads = 0;
    bool concurrency_control = false;

    struct Node
    {
        UInt64 child_count = 0;
        String step_name;                       /// QueryPlanStepRegistry key
        UInt64 step_format_version = 1;
        /// The oldest payload format that can still be read from the front of this payload. A
        /// reader that knows only older formats refuses the plan instead of reading fields that
        /// have moved.
        UInt64 payload_prefix_readable_from = 1;
        /// The oldest plan version that can read this node, worked out by the writer from what the
        /// node actually carries: the step's registered requirements, whatever the step asked for
        /// while writing, its header types and its settings. Lets a rejection name the step that
        /// blocked the plan.
        UInt64 min_reader_plan_version = 0;
        String step_description;
        SharedHeader header;                    /// nullptr for a step with no output header
        std::vector<SettingEntry> settings;
        UInt64 payload_size = 0;                /// bytes this node takes in the payload part
        String extension_bytes;                 /// empty today; a reader skips what it does not know
    };

    /// Every child comes before its parent and the root is last, with siblings left to right.
    /// `Delayed*` steps are skipped here, the same way the plan walk skips them.
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

/// Writes the outline: its size as a varint, then its bytes.
void writeQueryPlanOutline(const PlanOutline & outline, WriteBuffer & out);

/// Reads an outline written by `writeQueryPlanOutline`. Never reads past the size the outline
/// declares, and rejects both leftover bytes inside it and any size beyond the limits.
/// `max_frame_bytes` limits the outline itself and every payload size it declares; pass the size
/// of the plan body holding them, since nothing inside it can be larger.
/// A broken layout throws `CANNOT_PARSE_QUERY_PLAN`; errors from what it decodes (a header type,
/// say) keep their own codes.
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

/// Checks that this server can decode the plan the outline describes: it knows every step name
/// and payload format, every setting it would have to act on, every set kind, and the tree shape
/// holds together. It also checks the writer's own claims about which reader version each node
/// needs, so a writer that understated one is reported instead of quietly running wrong on old
/// readers. It does not look at payload bytes and does not check that tables exist.
/// Reports every problem it finds, not just the first.
QueryPlanOutlineValidationResult validateQueryPlanOutline(
    const PlanOutline & outline, UInt64 head_min_reader_plan_version);

/// The oldest plan version whose readers understand this type's binary encoding. All current
/// encodings predate the outline format, so this returns the base version; a new `BinaryTypeIndex`
/// entry must add its introduced-at version here so plans using it demand new enough readers.
UInt64 minReaderVersionForType(const IDataType & type);

/// EXPLAIN-style rendering from the outline alone. Unknown steps render as placeholders with
/// their payload size; no payload is decoded and no catalog/storage work is done.
String formatQueryPlanOutline(const PlanOutline & outline);

/// Writes the sets the plan refers to: fills `outline.sets`, sorted by hash, and one payload per
/// entry. A subquery set's payload is a whole serialized plan, version and all.
/// Raises `min_reader_plan_version` with what the sets themselves need: the column types of a
/// tuple set, and for a subquery set the version its own plan needs. That recursion is what stops
/// an old reader from accepting the outer plan and only then failing on a set it cannot read.
void serializeEnvelopeSets(
    SerializedSetsRegistry & registry,
    const QueryPlan::SerializationFlags & flags,
    PlanOutline & outline,
    std::vector<String> & payloads,
    UInt64 & min_reader_plan_version);

/// Reads the set payloads the outline lists. Each one must consume exactly its own bytes.
QueryPlanAndSets deserializeEnvelopeSets(
    QueryPlan plan,
    DeserializedSetsRegistry & registry,
    const PlanOutline & outline,
    ReadBuffer & in,
    const QueryPlan::SerializationFlags & flags,
    const ContextPtr & context,
    size_t max_type_complexity);

}
