#include <Processors/QueryPlan/QueryPlanEnvelope.h>

#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/SetSerialization.h>

#include <Core/ProtocolDefines.h>

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

/// Sanity caps for a hostile outline. The outline carries names, headers and settings, not
/// data, so these are far above anything a real plan produces.
constexpr UInt64 MAX_OUTLINE_BYTES = 64ULL << 20;
constexpr UInt64 MAX_OUTLINE_NODES = 1ULL << 20;
constexpr UInt64 MAX_OUTLINE_FIELD_BYTES = 16ULL << 20;
constexpr UInt64 MAX_OUTLINE_SETTINGS_PER_NODE = 64 * 1024;
constexpr UInt64 MAX_OUTLINE_DECLARED_PAYLOAD_BYTES = 2ULL << 30;

void writeOutlineBody(const PlanOutline & outline, WriteBuffer & out)
{
    writeVarUInt(outline.nodes.size(), out);
    for (const auto & node : outline.nodes)
    {
        writeVarUInt(node.child_count, out);
        writeStringBinary(node.step_name, out);
        writeVarUInt(node.step_format_version, out);
        writeVarUInt(node.min_reader_plan_version, out);

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

    writeVarUInt(outline.sets.size(), out);
    for (const auto & set : outline.sets)
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
            "Query plan outline declares {} {} which exceeds the limit of {}", value, what, cap);
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

PlanOutline readOutlineBody(ReadBuffer & in, size_t max_type_complexity)
{
    PlanOutline outline;

    UInt64 node_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "plan nodes");
    outline.nodes.reserve(node_count);

    for (UInt64 i = 0; i < node_count; ++i)
    {
        PlanOutline::Node node;

        node.child_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "node children");
        readStringBinary(node.step_name, in, MAX_OUTLINE_FIELD_BYTES);
        readVarUInt(node.step_format_version, in);
        readVarUInt(node.min_reader_plan_version, in);

        UInt8 node_flags = 0;
        readIntBinary(node_flags, in);
        node.has_output_header = node_flags & 1;

        readStringBinary(node.step_description, in, MAX_OUTLINE_FIELD_BYTES);

        if (node.has_output_header)
            node.header = std::make_shared<const Block>(deserializeQueryPlanHeader(in, max_type_complexity));

        UInt64 settings_count = readCappedVarUInt(in, MAX_OUTLINE_SETTINGS_PER_NODE, "settings");
        node.settings.reserve(settings_count);
        for (UInt64 s = 0; s < settings_count; ++s)
        {
            PlanOutline::SettingEntry entry;
            readStringBinary(entry.name, in, MAX_OUTLINE_FIELD_BYTES);
            readIntBinary(entry.flags, in);
            entry.value = readCappedSizedBytes(in, MAX_OUTLINE_FIELD_BYTES, "setting value bytes");
            node.settings.push_back(std::move(entry));
        }

        node.payload_size = readCappedVarUInt(in, MAX_OUTLINE_DECLARED_PAYLOAD_BYTES, "step payload bytes");

        /// Future outline layouts append data here; a v4 reader skips it, which is what keeps
        /// shape rendering working for plans of newer versions.
        node.extension_bytes = readCappedSizedBytes(in, MAX_OUTLINE_FIELD_BYTES, "node extra bytes");

        outline.nodes.push_back(std::move(node));
    }

    UInt64 set_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "sets");
    outline.sets.reserve(set_count);
    for (UInt64 i = 0; i < set_count; ++i)
    {
        PlanOutline::SetEntry entry;
        readBinary(entry.hash, in);
        readIntBinary(entry.kind, in);
        entry.payload_size = readCappedVarUInt(in, MAX_OUTLINE_DECLARED_PAYLOAD_BYTES, "set payload bytes");
        outline.sets.push_back(entry);
    }

    return outline;
}

}

void writeQueryPlanOutline(const PlanOutline & outline, WriteBuffer & out)
{
    WriteBufferFromOwnString body;
    writeOutlineBody(outline, body);
    body.finalize();

    writeVarUInt(body.str().size(), out);
    out.write(body.str().data(), body.str().size());
}

PlanOutline readQueryPlanOutline(ReadBuffer & in, size_t max_type_complexity)
{
    UInt64 outline_size = readCappedVarUInt(in, MAX_OUTLINE_BYTES, "outline bytes");

    /// Copy the frame and parse from memory: parsing can then never read past the declared size,
    /// and trailing bytes inside the frame are detectable.
    String outline_bytes;
    outline_bytes.resize(outline_size);
    in.readStrict(outline_bytes.data(), outline_size);

    ReadBufferFromMemory body(outline_bytes.data(), outline_bytes.size());
    try
    {
        auto outline = readOutlineBody(body, max_type_complexity);

        if (!body.eof())
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                "Query plan outline has {} trailing bytes inside its frame", body.available());

        return outline;
    }
    catch (Exception & e)
    {
        /// A frame that ends mid-field is a malformed frame, not an I/O condition of the outer
        /// stream. Nested codec errors (e.g. type decoding) keep their own codes.
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                "Query plan outline is truncated: {}", e.message());
        throw;
    }
}

String QueryPlanOutlineValidationResult::describe() const
{
    return fmt::format("{}", fmt::join(issues, "; "));
}

QueryPlanOutlineValidationResult validateQueryPlanOutline(
    const PlanOutline & outline, UInt64 plan_version, UInt64 head_min_reader_plan_version)
{
    QueryPlanOutlineValidationResult result;
    const auto & registry = QueryPlanStepRegistry::instance();

    if (outline.nodes.empty())
    {
        result.issues.push_back("plan has no nodes");
        return result;
    }

    /// Pre-order structural walk: each node consumes one pending slot and opens child_count new
    /// ones. The counts are consistent iff no node arrives with nothing pending and nothing stays
    /// pending at the end.
    UInt64 pending_child_slots = 1;
    for (size_t i = 0; i < outline.nodes.size(); ++i)
    {
        const auto & node = outline.nodes[i];

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

        /// Writer-honesty cross-checks: a node's declared "needed to read" version must cover the
        /// static requirements this binary knows about, and the head value must cover every node.
        /// A writer that undercounted would otherwise make old readers silently misexecute.
        if (const auto * info = registry.getStepSerializationInfo(node.step_name))
        {
            UInt64 static_requirement = info->introduced_in_plan_version;
            for (const auto & [step_version, min_plan_version] : info->min_plan_version_for_step_version)
                if (node.step_format_version >= step_version)
                    static_requirement = std::max(static_requirement, min_plan_version);
            if (static_requirement > node.min_reader_plan_version)
                result.issues.push_back(fmt::format(
                    "step '{}' (node #{}) declares reader version {} but its registry info requires {}",
                    node.step_name, i, node.min_reader_plan_version, static_requirement));
        }
        if (node.min_reader_plan_version > head_min_reader_plan_version)
            result.issues.push_back(fmt::format(
                "step '{}' (node #{}) requires reader version {} above the plan's declared {}",
                node.step_name, i, node.min_reader_plan_version, head_min_reader_plan_version));

        /// Parents read their input headers from their children, so only the root may lack one.
        if (i != 0 && !node.has_output_header)
            result.issues.push_back(fmt::format("non-root step '{}' (node #{}) has no output header", node.step_name, i));

        for (const auto & setting : node.settings)
            if (!QueryPlanSerializationSettings::hasSetting(setting.name)
                && !(setting.flags & PlanOutline::SettingEntry::FLAG_IGNORABLE))
                result.issues.push_back(fmt::format("unknown plan setting '{}' (not marked ignorable)", setting.name));
    }

    if (pending_child_slots != 0)
        result.issues.push_back(fmt::format("plan tree is incomplete: {} declared children have no nodes", pending_child_slots));

    for (size_t i = 0; i < outline.sets.size(); ++i)
    {
        const auto & set = outline.sets[i];

        if (set.kind < UInt8(SetSerializationKind::StorageSet) || set.kind > UInt8(SetSerializationKind::SubqueryPlan))
            result.issues.push_back(fmt::format("unknown set kind {}", UInt32(set.kind)));

        if (i > 0)
        {
            const auto & prev = outline.sets[i - 1].hash;
            const auto & curr = set.hash;
            if (!(std::tie(prev.high64, prev.low64) < std::tie(curr.high64, curr.low64)))
                result.issues.push_back(fmt::format("set entries are not sorted by hash at index {}", i));
        }
    }

    return result;
}

UInt64 minReaderVersionForType(const IDataType &)
{
    /// Every type encoding in existence predates the outline format. A new `BinaryTypeIndex`
    /// entry must return its introduced-at version here, otherwise old readers would fail while
    /// decoding a header or set payload instead of rejecting the plan up front.
    return DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;
}

String formatQueryPlanOutline(const PlanOutline & outline)
{
    const auto & registry = QueryPlanStepRegistry::instance();

    WriteBufferFromOwnString out;

    /// Depth per node from the pre-order child counts.
    std::vector<UInt64> remaining_children_stack;
    for (const auto & node : outline.nodes)
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

    if (!outline.sets.empty())
        writeString(fmt::format("Sets: {}\n", outline.sets.size()), out);

    out.finalize();
    return out.str();
}

}
