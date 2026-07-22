#pragma once

#include <Core/Block.h>
#include <Interpreters/PreparedSets.h>

namespace DB
{

class WriteBuffer;
class ReadBuffer;

/// The skeleton section (Section A) of the v4 query-plan envelope: the stable, common per-node
/// data of every plan — tree shape, step names and format versions, descriptions, output headers,
/// changed settings, payload sizes — stored in front of the step payloads.
///
/// Properties this layout provides:
///  - a reader can validate that the whole plan is decodable (every step name and format version
///    known, every non-ignorable setting known, every set kind known) without touching a single
///    payload byte — see validateQueryPlanSkeleton;
///  - the plan shape can be rendered even when some steps are unknown or carry a newer format
///    version — see formatQueryPlanSkeleton;
///  - payloads become independently addressable byte ranges (offsets are prefix sums of
///    payload_size in skeleton order).
///
/// The skeleton wire layout is frozen append-only: future additions go into each node's
/// node_extra bytes, which older readers skip, so shape rendering keeps working across plan
/// versions.
struct PlanSkeleton
{
    struct SettingEntry
    {
        String name;
        UInt8 flags = 0;    /// bit 0: ignorable — an old reader may skip this setting
        String value;       /// the setting-field binary encoding, exactly what writeChangedBinary emits per value

        static constexpr UInt8 FLAG_IGNORABLE = 1;
    };

    struct Node
    {
        UInt64 child_count = 0;
        String step_name;                       /// QueryPlanStepRegistry key
        UInt64 step_format_version = 1;
        bool has_output_header = false;
        String step_description;
        SharedHeader header;                    /// nullptr iff !has_output_header
        std::vector<SettingEntry> settings;
        UInt64 payload_size = 0;                /// size of this node's slice of the payload section
        String node_extra;                      /// empty in v4; skipped by readers that do not know it
    };

    /// Nodes in pre-order over the serialized tree (Delayed* steps elided, as in the plan walk).
    std::vector<Node> nodes;

    struct SetEntry
    {
        FutureSet::Hash hash;
        UInt8 kind = 0;                         /// SetSerializationKind; unknown value = validation issue
        UInt64 payload_size = 0;
    };

    /// In canonical order: sorted by the 128-bit hash.
    std::vector<SetEntry> sets;
};

/// Writes Section A: VarUInt skeleton_size, then the skeleton bytes.
void writeQueryPlanSkeleton(const PlanSkeleton & skeleton, WriteBuffer & out);

/// Reads Section A written by writeQueryPlanSkeleton. Bounded: never reads past skeleton_size,
/// rejects trailing bytes inside the frame and any size that exceeds the declared bounds.
/// Frame-layer violations throw CANNOT_PARSE_QUERY_PLAN; errors from nested codecs (e.g. header
/// type decoding) keep their own codes and surface at the frame boundary.
PlanSkeleton readQueryPlanSkeleton(ReadBuffer & in, size_t max_type_complexity);

struct QueryPlanSkeletonValidationResult
{
    /// Human-readable issues; empty means the reader is able to decode the whole plan.
    std::vector<String> issues;

    bool ok() const { return issues.empty(); }
    /// All issues joined into one message, so a mixed-version error reports everything at once.
    String describe() const;
};

/// Capability check against this binary's registry and settings: proves the reader can decode
/// the plan (names, step format versions vs the manifest, non-ignorable settings, set kinds,
/// structural consistency). It does not prove payload bytes are well-formed, nor that referenced
/// tables exist. Collects all issues over a syntactically parseable skeleton.
QueryPlanSkeletonValidationResult validateQueryPlanSkeleton(const PlanSkeleton & skeleton, UInt64 plan_version);

/// EXPLAIN-style rendering from the skeleton alone. Unknown steps render as placeholders with
/// their payload size; no payload is decoded and no catalog/storage work is done.
String formatQueryPlanSkeleton(const PlanSkeleton & skeleton);

}
