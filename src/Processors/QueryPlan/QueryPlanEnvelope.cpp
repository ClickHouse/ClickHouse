#include <Processors/QueryPlan/QueryPlanEnvelope.h>

#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetSerialization.h>

#include <Common/Exception.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_PARSE_QUERY_PLAN;
}

namespace
{

/// Sanity caps for a hostile skeleton. The skeleton carries names, headers and settings, not
/// data, so these are far above anything a real plan produces.
constexpr UInt64 MAX_SKELETON_BYTES = 64ULL << 20;
constexpr UInt64 MAX_SKELETON_NODES = 1ULL << 20;
constexpr UInt64 MAX_SKELETON_FIELD_BYTES = 16ULL << 20;
constexpr UInt64 MAX_SKELETON_SETTINGS_PER_NODE = 64 * 1024;
constexpr UInt64 MAX_SKELETON_DECLARED_PAYLOAD_BYTES = 2ULL << 30;

void writeSkeletonBody(const PlanSkeleton & skeleton, WriteBuffer & out)
{
    writeVarUInt(skeleton.nodes.size(), out);
    for (const auto & node : skeleton.nodes)
    {
        writeVarUInt(node.child_count, out);
        writeStringBinary(node.step_name, out);
        writeVarUInt(node.step_format_version, out);

        UInt8 node_flags = node.has_output_header ? 1 : 0;
        writeIntBinary(node_flags, out);

        writeStringBinary(node.step_description, out);

        if (node.has_output_header)
            serializeQueryPlanHeader(*node.header, out);

        writeVarUInt(node.settings.size(), out);
        for (const auto & setting : node.settings)
        {
            writeStringBinary(setting.name, out);
            writeIntBinary(setting.flags, out);
            writeVarUInt(setting.value.size(), out);
            out.write(setting.value.data(), setting.value.size());
        }

        writeVarUInt(node.payload_size, out);

        writeVarUInt(node.extension_bytes.size(), out);
        out.write(node.extension_bytes.data(), node.extension_bytes.size());
    }

    writeVarUInt(skeleton.sets.size(), out);
    for (const auto & set : skeleton.sets)
    {
        writeBinary(set.hash, out);
        writeIntBinary(set.kind, out);
        writeVarUInt(set.payload_size, out);
    }
}

UInt64 readCappedVarUInt(ReadBuffer & in, UInt64 cap, const char * what)
{
    UInt64 value = 0;
    readVarUInt(value, in);
    if (value > cap)
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan skeleton declares {} {} which exceeds the limit of {}", value, what, cap);
    return value;
}

String readCappedSizedBytes(ReadBuffer & in, UInt64 cap, const char * what)
{
    UInt64 size = readCappedVarUInt(in, cap, what);
    String bytes;
    bytes.resize(size);
    in.readStrict(bytes.data(), size);
    return bytes;
}

PlanSkeleton readSkeletonBody(ReadBuffer & in, size_t max_type_complexity)
{
    PlanSkeleton skeleton;

    UInt64 node_count = readCappedVarUInt(in, MAX_SKELETON_NODES, "plan nodes");
    skeleton.nodes.reserve(node_count);

    for (UInt64 i = 0; i < node_count; ++i)
    {
        PlanSkeleton::Node node;

        node.child_count = readCappedVarUInt(in, MAX_SKELETON_NODES, "node children");
        readStringBinary(node.step_name, in, MAX_SKELETON_FIELD_BYTES);
        readVarUInt(node.step_format_version, in);

        UInt8 node_flags = 0;
        readIntBinary(node_flags, in);
        node.has_output_header = node_flags & 1;

        readStringBinary(node.step_description, in, MAX_SKELETON_FIELD_BYTES);

        if (node.has_output_header)
            node.header = std::make_shared<const Block>(deserializeQueryPlanHeader(in, max_type_complexity));

        UInt64 settings_count = readCappedVarUInt(in, MAX_SKELETON_SETTINGS_PER_NODE, "settings");
        node.settings.reserve(settings_count);
        for (UInt64 s = 0; s < settings_count; ++s)
        {
            PlanSkeleton::SettingEntry entry;
            readStringBinary(entry.name, in, MAX_SKELETON_FIELD_BYTES);
            readIntBinary(entry.flags, in);
            entry.value = readCappedSizedBytes(in, MAX_SKELETON_FIELD_BYTES, "setting value bytes");
            node.settings.push_back(std::move(entry));
        }

        node.payload_size = readCappedVarUInt(in, MAX_SKELETON_DECLARED_PAYLOAD_BYTES, "step payload bytes");

        /// Future skeleton layouts append data here; a v4 reader skips it, which is what keeps
        /// shape rendering working for plans of newer versions.
        node.extension_bytes = readCappedSizedBytes(in, MAX_SKELETON_FIELD_BYTES, "node extra bytes");

        skeleton.nodes.push_back(std::move(node));
    }

    UInt64 set_count = readCappedVarUInt(in, MAX_SKELETON_NODES, "sets");
    skeleton.sets.reserve(set_count);
    for (UInt64 i = 0; i < set_count; ++i)
    {
        PlanSkeleton::SetEntry entry;
        readBinary(entry.hash, in);
        readIntBinary(entry.kind, in);
        entry.payload_size = readCappedVarUInt(in, MAX_SKELETON_DECLARED_PAYLOAD_BYTES, "set payload bytes");
        skeleton.sets.push_back(entry);
    }

    return skeleton;
}

}

void writeQueryPlanSkeleton(const PlanSkeleton & skeleton, WriteBuffer & out)
{
    WriteBufferFromOwnString body;
    writeSkeletonBody(skeleton, body);
    body.finalize();

    writeVarUInt(body.str().size(), out);
    out.write(body.str().data(), body.str().size());
}

PlanSkeleton readQueryPlanSkeleton(ReadBuffer & in, size_t max_type_complexity)
{
    UInt64 skeleton_size = readCappedVarUInt(in, MAX_SKELETON_BYTES, "skeleton bytes");

    /// Copy the frame and parse from memory: parsing can then never read past the declared size,
    /// and trailing bytes inside the frame are detectable.
    String skeleton_bytes;
    skeleton_bytes.resize(skeleton_size);
    in.readStrict(skeleton_bytes.data(), skeleton_size);

    ReadBufferFromMemory body(skeleton_bytes.data(), skeleton_bytes.size());
    try
    {
        auto skeleton = readSkeletonBody(body, max_type_complexity);

        if (!body.eof())
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                "Query plan skeleton has {} trailing bytes inside its frame", body.available());

        return skeleton;
    }
    catch (Exception & e)
    {
        /// A frame that ends mid-field is a malformed frame, not an I/O condition of the outer
        /// stream. Nested codec errors (e.g. type decoding) keep their own codes.
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                "Query plan skeleton is truncated: {}", e.message());
        throw;
    }
}

String QueryPlanSkeletonValidationResult::describe() const
{
    return fmt::format("{}", fmt::join(issues, "; "));
}

QueryPlanSkeletonValidationResult validateQueryPlanSkeleton(const PlanSkeleton & skeleton, UInt64 plan_version)
{
    QueryPlanSkeletonValidationResult result;
    const auto & registry = QueryPlanStepRegistry::instance();

    if (skeleton.nodes.empty())
    {
        result.issues.push_back("plan has no nodes");
        return result;
    }

    /// Pre-order structural walk: each node consumes one pending slot and opens child_count new
    /// ones. The counts are consistent iff no node arrives with nothing pending and nothing stays
    /// pending at the end.
    UInt64 pending_child_slots = 1;
    for (size_t i = 0; i < skeleton.nodes.size(); ++i)
    {
        const auto & node = skeleton.nodes[i];

        if (pending_child_slots == 0)
        {
            result.issues.push_back(fmt::format("node #{} ('{}') is not reachable from the root", i, node.step_name));
            break;
        }
        pending_child_slots -= 1;
        pending_child_slots += node.child_count;

        if (!registry.hasStep(node.step_name))
        {
            result.issues.push_back(fmt::format("unknown step '{}'", node.step_name));
        }
        else if (const auto * info = registry.getStepSerializationInfo(node.step_name))
        {
            /// A step format version that requires a newer plan version than the envelope carries
            /// is a writer violation. Versions above this binary's known maximum are legitimate
            /// ignorable extensions (prefix-read at payload time) as long as the mapping allows.
            for (const auto & [step_version, min_plan_version] : info->min_plan_version_for_step_version)
                if (node.step_format_version >= step_version && plan_version < min_plan_version)
                    result.issues.push_back(fmt::format(
                        "step '{}' format version {} requires plan version at least {}, envelope has {}",
                        node.step_name, node.step_format_version, min_plan_version, plan_version));
        }

        if (node.step_format_version == 0)
            result.issues.push_back(fmt::format("step '{}' has format version 0", node.step_name));

        /// Parents read their input headers from their children, so only the root may lack one.
        if (i != 0 && !node.has_output_header)
            result.issues.push_back(fmt::format("non-root step '{}' (node #{}) has no output header", node.step_name, i));

        for (const auto & setting : node.settings)
            if (!QueryPlanSerializationSettings::hasSetting(setting.name)
                && !(setting.flags & PlanSkeleton::SettingEntry::FLAG_IGNORABLE))
                result.issues.push_back(fmt::format("unknown plan setting '{}' (not marked ignorable)", setting.name));
    }

    if (pending_child_slots != 0)
        result.issues.push_back(fmt::format("plan tree is incomplete: {} declared children have no nodes", pending_child_slots));

    for (size_t i = 0; i < skeleton.sets.size(); ++i)
    {
        const auto & set = skeleton.sets[i];

        if (set.kind < UInt8(SetSerializationKind::StorageSet) || set.kind > UInt8(SetSerializationKind::SubqueryPlan))
            result.issues.push_back(fmt::format("unknown set kind {}", UInt32(set.kind)));

        if (i > 0)
        {
            const auto & prev = skeleton.sets[i - 1].hash;
            const auto & curr = set.hash;
            if (!(std::tie(prev.high64, prev.low64) < std::tie(curr.high64, curr.low64)))
                result.issues.push_back(fmt::format("set entries are not sorted by hash at index {}", i));
        }
    }

    return result;
}

String formatQueryPlanSkeleton(const PlanSkeleton & skeleton)
{
    const auto & registry = QueryPlanStepRegistry::instance();

    WriteBufferFromOwnString out;

    /// Depth per node from the pre-order child counts.
    std::vector<UInt64> remaining_children_stack;
    for (const auto & node : skeleton.nodes)
    {
        size_t depth = remaining_children_stack.size();
        if (!remaining_children_stack.empty())
            --remaining_children_stack.back();

        for (size_t i = 0; i < depth; ++i)
            writeString("  ", out);

        writeString(node.step_name, out);
        if (node.step_format_version != 1)
            writeString(fmt::format(" (format v{})", node.step_format_version), out);
        if (!registry.hasStep(node.step_name))
            writeString(fmt::format(" <unknown step, {} payload bytes>", node.payload_size), out);
        if (!node.step_description.empty())
            writeString(fmt::format(" ({})", node.step_description), out);

        if (node.has_output_header && node.header)
        {
            writeString(" -> ", out);
            bool first = true;
            for (const auto & column : *node.header)
            {
                if (!first)
                    writeString(", ", out);
                first = false;
                writeString(column.name, out);
                writeChar(' ', out);
                writeString(column.type->getName(), out);
            }
        }
        writeChar('\n', out);

        while (!remaining_children_stack.empty() && remaining_children_stack.back() == 0)
            remaining_children_stack.pop_back();
        if (node.child_count > 0)
            remaining_children_stack.push_back(node.child_count);
    }

    if (!skeleton.sets.empty())
        writeString(fmt::format("Sets: {}\n", skeleton.sets.size()), out);

    out.finalize();
    return out.str();
}

}
