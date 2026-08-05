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

void writeOutlineBody(const PlanOutline & outline, WriteBuffer & out)
{
    writeVarUInt(outline.nodes.size(), out);
    for (const auto & node : outline.nodes)
    {
        writeVarUInt(node.child_count, out);
        writeStringBinary(node.step_name, out);
        writeVarUInt(node.step_format_version, out);
        writeVarUInt(node.payload_prefix_readable_from, out);
        writeVarUInt(node.min_reader_plan_version, out);

        UInt8 node_flags = node.header ? 1 : 0;
        writeIntBinary(node_flags, out);

        writeStringBinary(node.step_description, out);

        if (node.header)
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

PlanOutline readOutlineBody(ReadBuffer & in, size_t max_type_complexity, UInt64 max_frame_bytes)
{
    PlanOutline outline;

    /// Counts come from the peer: the vectors grow as elements are read, so a frame that ends
    /// early only pays for what it delivered.
    UInt64 node_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "plan nodes");

    for (UInt64 i = 0; i < node_count; ++i)
    {
        PlanOutline::Node node;

        node.child_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "node children");
        readStringBinary(node.step_name, in, MAX_OUTLINE_FIELD_BYTES);
        readVarUInt(node.step_format_version, in);
        readVarUInt(node.payload_prefix_readable_from, in);
        readVarUInt(node.min_reader_plan_version, in);

        UInt8 node_flags = 0;
        readIntBinary(node_flags, in);
        /// Only bit 0 is assigned. Ignorable additions go in `extension_bytes`, so a bit set here
        /// means something this reader would have to act on and cannot.
        if (node_flags & ~UInt8(1))
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                "Query plan node carries unknown flags {:#x}", UInt32(node_flags));
        readStringBinary(node.step_description, in, MAX_OUTLINE_FIELD_BYTES);

        if (node_flags & 1)
            node.header = std::make_shared<const Block>(deserializeQueryPlanHeader(in, max_type_complexity));

        UInt64 settings_count = readCappedVarUInt(in, MAX_OUTLINE_SETTINGS_PER_NODE, "settings");
        for (UInt64 s = 0; s < settings_count; ++s)
        {
            PlanOutline::SettingEntry entry;
            readStringBinary(entry.name, in, MAX_OUTLINE_FIELD_BYTES);
            readIntBinary(entry.flags, in);
            if (entry.flags & ~PlanOutline::SettingEntry::FLAG_IGNORABLE)
                throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                    "Query plan setting '{}' carries unknown flags {:#x}", entry.name, UInt32(entry.flags));
            entry.value = readCappedSizedBytes(in, MAX_OUTLINE_FIELD_BYTES, "setting value bytes");
            node.settings.push_back(std::move(entry));
        }

        node.payload_size = readCappedVarUInt(in, max_frame_bytes, "step payload bytes");

        /// Future outline layouts append data here; a v5 reader skips it, which is what keeps
        /// shape rendering working for plans of newer versions.
        node.extension_bytes = readCappedSizedBytes(in, MAX_OUTLINE_FIELD_BYTES, "node extra bytes");

        outline.nodes.push_back(std::move(node));
    }

    UInt64 set_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "sets");
    for (UInt64 i = 0; i < set_count; ++i)
    {
        PlanOutline::SetEntry entry;
        readBinary(entry.hash, in);
        readIntBinary(entry.kind, in);
        entry.payload_size = readCappedVarUInt(in, max_frame_bytes, "set payload bytes");
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

PlanOutline readQueryPlanOutline(ReadBuffer & in, size_t max_type_complexity, UInt64 max_frame_bytes)
{
    /// Capped by the envelope as well as by the absolute limit: the outline lives inside the
    /// envelope, so a frame larger than that must be rejected before anything is allocated or read,
    /// or a small declared envelope could still make the reader take bytes belonging to the
    /// protocol after it.
    UInt64 outline_size = readCappedVarUInt(in, std::min(MAX_OUTLINE_BYTES, max_frame_bytes), "outline bytes");

    /// Copy the frame and parse from memory: parsing can then never read past the declared size,
    /// and trailing bytes inside the frame are detectable.
    String outline_bytes;
    outline_bytes.resize(outline_size);
    in.readStrict(outline_bytes.data(), outline_size);

    ReadBufferFromMemory body(outline_bytes.data(), outline_bytes.size());
    try
    {
        auto outline = readOutlineBody(body, max_type_complexity, max_frame_bytes);

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

PlanOutlineShape reconstructOutlineShape(const PlanOutline & outline)
{
    PlanOutlineShape shape;
    const size_t node_count = outline.nodes.size();
    shape.children.resize(node_count);

    /// Subtrees that no parent has claimed yet, in the order they were completed.
    std::vector<size_t> unclaimed;
    unclaimed.reserve(node_count);

    for (size_t i = 0; i < node_count; ++i)
    {
        const UInt64 child_count = outline.nodes[i].child_count;
        if (child_count > unclaimed.size())
        {
            shape.issues.push_back(fmt::format(
                "node #{} ('{}') declares {} children but only {} subtrees precede it",
                i, outline.nodes[i].step_name, child_count, unclaimed.size()));
            return shape;
        }

        /// Popping walks the children right to left, so fill the list from the back.
        auto & children = shape.children[i];
        children.resize(child_count);
        for (size_t position = child_count; position-- > 0;)
        {
            children[position] = unclaimed.back();
            unclaimed.pop_back();
        }

        unclaimed.push_back(i);
    }

    if (unclaimed.size() != 1)
        shape.issues.push_back(fmt::format(
            "plan tree does not have a single root: {} subtrees are unattached", unclaimed.size()));

    return shape;
}

QueryPlanOutlineValidationResult validateQueryPlanOutline(
    const PlanOutline & outline, UInt64 head_min_reader_plan_version)
{
    QueryPlanOutlineValidationResult result;
    const auto & registry = QueryPlanStepRegistry::instance();

    if (outline.nodes.empty())
    {
        result.issues.push_back("plan has no nodes");
        return result;
    }

    /// Structural issues are collected first but do not stop the metadata scan below: a caller
    /// upgrading versions wants every unknown step and setting named, not just the first problem.
    result.shape = reconstructOutlineShape(outline);
    for (const auto & issue : result.shape.issues)
        result.issues.push_back(issue);

    const size_t root_index = outline.nodes.size() - 1;

    for (size_t i = 0; i < outline.nodes.size(); ++i)
    {
        const auto & node = outline.nodes[i];

        const auto * info = registry.getStepSerializationInfo(node.step_name);
        if (!info)
            result.issues.push_back(fmt::format("unknown step '{}'", node.step_name));

        if (node.step_format_version == 0)
            result.issues.push_back(fmt::format("step '{}' has format version 0", node.step_name));

        /// A payload newer than this binary knows is only readable when the writer says the part we
        /// understand still comes first. Restructured payloads say otherwise and are refused here,
        /// before any payload byte is decoded.
        if (node.payload_prefix_readable_from == 0 || node.payload_prefix_readable_from > node.step_format_version)
            result.issues.push_back(fmt::format(
                "step '{}' (node #{}) says its payload format {} is readable from format {}, which is "
                "not a format it could have",
                node.step_name, i, node.step_format_version, node.payload_prefix_readable_from));

        if (info)
        {
            const UInt64 known_formats = info->maxFormatVersion();
            if (node.step_format_version > known_formats)
            {
                if (node.payload_prefix_readable_from > known_formats)
                    result.issues.push_back(fmt::format(
                        "step '{}' (node #{}) has payload format {} readable only by format {} and up, "
                        "this server knows up to {}",
                        node.step_name, i, node.step_format_version, node.payload_prefix_readable_from, known_formats));
            }
            /// For a format this server knows, the writer's claim is checkable against the step's
            /// own history: a writer understating it would have old readers accept bytes they must
            /// not read positionally.
            else if (node.payload_prefix_readable_from != info->prefixReadableFrom(node.step_format_version))
                result.issues.push_back(fmt::format(
                    "step '{}' (node #{}) says payload format {} is readable from format {} but this "
                    "server's history of that step says {}",
                    node.step_name, i, node.step_format_version, node.payload_prefix_readable_from,
                    info->prefixReadableFrom(node.step_format_version)));

            /// Writer-honesty cross-check: a node's declared "needed to read" version must cover the
            /// requirements this binary knows about. A writer that undercounted would otherwise make
            /// old readers silently misexecute.
            const UInt64 static_requirement = info->minPlanVersionForFormat(node.step_format_version);
            if (static_requirement > node.min_reader_plan_version)
                result.issues.push_back(fmt::format(
                    "step '{}' (node #{}) declares reader version {} but its registry info requires {}",
                    node.step_name, i, node.min_reader_plan_version, static_requirement));
        }

        /// The head value must cover every node, or a reader would accept a plan on a promise the
        /// nodes do not keep.
        if (node.min_reader_plan_version > head_min_reader_plan_version)
            result.issues.push_back(fmt::format(
                "step '{}' (node #{}) requires reader version {} above the plan's declared {}",
                node.step_name, i, node.min_reader_plan_version, head_min_reader_plan_version));

        /// Parents read their input headers from their children, so only the root may lack one.
        /// In post-order the root is the last node.
        if (i != root_index && !node.header)
            result.issues.push_back(fmt::format("non-root step '{}' (node #{}) has no output header", node.step_name, i));

        for (const auto & setting : node.settings)
            if (!QueryPlanSerializationSettings::hasSetting(setting.name)
                && !(setting.flags & PlanOutline::SettingEntry::FLAG_IGNORABLE))
                result.issues.push_back(fmt::format("unknown plan setting '{}' (not marked ignorable)", setting.name));
    }

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

    /// Nodes are stored children-first, so depth cannot be counted while scanning them: rebuild
    /// the tree and walk it from the root instead. A malformed tree still renders, flat.
    auto shape = reconstructOutlineShape(outline);

    std::vector<std::pair<size_t, size_t>> to_print; /// (node index, depth)
    if (shape.ok() && !outline.nodes.empty())
    {
        std::vector<std::pair<size_t, size_t>> stack{{outline.nodes.size() - 1, 0}};
        while (!stack.empty())
        {
            auto [index, depth] = stack.back();
            stack.pop_back();
            to_print.emplace_back(index, depth);

            const auto & children = shape.children[index];
            for (size_t position = children.size(); position-- > 0;)
                stack.emplace_back(children[position], depth + 1);
        }
    }
    else
    {
        for (size_t i = 0; i < outline.nodes.size(); ++i)
            to_print.emplace_back(i, 0);
    }

    for (const auto & [index, depth] : to_print)
    {
        const auto & node = outline.nodes[index];

        for (size_t i = 0; i < depth; ++i)
            writeString("  ", out);

        writeString(node.step_name, out);
        if (node.step_format_version != 1)
            writeString(fmt::format(" (format v{})", node.step_format_version), out);
        if (!registry.hasStep(node.step_name))
            writeString(fmt::format(" <unknown step, {} payload bytes>", node.payload_size), out);
        if (!node.step_description.empty())
            writeString(fmt::format(" ({})", node.step_description), out);

        if (node.header)
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
    }

    if (!outline.sets.empty())
        writeString(fmt::format("Sets: {}\n", outline.sets.size()), out);

    out.finalize();
    return out.str();
}

}
