#include <Processors/QueryPlan/QueryPlanOutline.h>
#include <limits>

#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>

#include <IO/LimitReadBuffer.h>
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
    extern const int INCORRECT_DATA;
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
    writeVarUInt(outline.max_threads, out);
    writeBinary(outline.concurrency_control, out);
    writeBinary(outline.include_step_descriptions, out);

    /// Both counts sit in the front header, so a receiver that only wants to skip the plan off the
    /// stream reads them here and then jumps the node records by their lengths and the payloads by
    /// their inline sizes, without walking any node record. Payload sizes are not in the outline;
    /// each payload carries its own size inline (see `serializeFramedToChunks`).
    writeVarUInt(outline.nodes.size(), out);
    writeVarUInt(outline.sets.size(), out);

    for (const auto & node : outline.nodes)
    {
        /// Each node record is length-prefixed. A reader that knows fewer fields reads the ones it
        /// knows and skips the rest of the record by this length; a result-changing addition takes a
        /// new step name, so the trailing bytes are always safe to skip.
        WriteBufferFromOwnString record;
        writeVarUInt(node.children.size(), record);
        for (UInt64 child : node.children)
            writeVarUInt(child, record);
        writeStringBinary(node.step_name, record);
        writeVarUInt(node.step_format_version, record);

        UInt8 node_flags = node.header ? 1 : 0;
        writeIntBinary(node_flags, record);

        if (outline.include_step_descriptions)
            writeStringBinary(node.step_description, record);

        if (node.header)
            serializeQueryPlanHeader(*node.header, record);

        writeVarUInt(node.settings.size(), record);
        for (const auto & setting : node.settings)
        {
            writeStringBinary(setting.name, record);
            writeIntBinary(setting.flags, record);
            writeVarUInt(setting.value.size(), record);
            record.write(setting.value.data(), setting.value.size());
        }
        record.finalize();

        writeVarUInt(record.str().size(), out);
        out.write(record.str().data(), record.str().size());
    }

    for (const auto & set : outline.sets)
    {
        writeBinary(set.hash, out);
        writeIntBinary(set.kind, out);
    }
}

UInt64 readCappedVarUInt(ReadBuffer & in, UInt64 cap, const char * what)
{
    UInt64 value = 0;
    readVarUInt(value, in);
    if (value > cap)
        throw Exception(ErrorCodes::INCORRECT_DATA,
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

/// Reads the front header - the plan-level fields and the node and set counts - into `outline`.
/// This is all a reader needs to know how many payload frames follow, without touching a step,
/// header or setting.
OutlineFrameCounts readOutlineFrontHeader(ReadBuffer & in, PlanOutline & outline)
{
    readVarUInt(outline.max_threads, in);
    readBinary(outline.concurrency_control, in);
    readBinary(outline.include_step_descriptions, in);

    OutlineFrameCounts counts;
    counts.node_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "plan nodes");
    counts.set_count = readCappedVarUInt(in, MAX_OUTLINE_NODES, "sets");
    return counts;
}

PlanOutline readOutlineBody(ReadBuffer & in, size_t max_type_complexity, UInt64 max_frame_bytes)
{
    PlanOutline outline;

    /// The counts come from the peer, so the vectors grow as elements are read: a frame that ends
    /// early only pays for what it delivered.
    const auto [node_count, set_count] = readOutlineFrontHeader(in, outline);

    for (UInt64 i = 0; i < node_count; ++i)
    {
        PlanOutline::Node node;

        /// The record is length-prefixed and read through a bounded buffer: the reader takes the
        /// fields it knows and skips any trailing bytes of a record a newer writer grew.
        UInt64 record_size = readCappedVarUInt(in, max_frame_bytes, "node record bytes");
        LimitReadBuffer record(in, {.read_no_more = record_size});

        /// A node cannot have more children than there are nodes, and each index is one of them.
        UInt64 child_count = readCappedVarUInt(record, node_count, "node children");
        for (UInt64 c = 0; c < child_count; ++c)
            node.children.push_back(readCappedVarUInt(record, node_count, "child index"));
        readStringBinary(node.step_name, record, MAX_OUTLINE_FIELD_BYTES);
        readVarUInt(node.step_format_version, record);

        UInt8 node_flags = 0;
        readIntBinary(node_flags, record);
        /// Only bit 0 is assigned; a spare bit set here means something this reader would have to
        /// act on and cannot.
        if (node_flags & ~UInt8(1))
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Query plan node carries unknown flags {:#x}", UInt32(node_flags));
        if (outline.include_step_descriptions)
            readStringBinary(node.step_description, record, MAX_OUTLINE_FIELD_BYTES);

        if (node_flags & 1)
            node.header = std::make_shared<const Block>(deserializeQueryPlanHeader(record, max_type_complexity));

        UInt64 settings_count = readCappedVarUInt(record, MAX_OUTLINE_SETTINGS_PER_NODE, "settings");
        for (UInt64 s = 0; s < settings_count; ++s)
        {
            PlanOutline::SettingEntry entry;
            readStringBinary(entry.name, record, MAX_OUTLINE_FIELD_BYTES);
            readIntBinary(entry.flags, record);
            if (entry.flags & ~PlanOutline::SettingEntry::FLAG_IGNORABLE)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Query plan setting '{}' carries unknown flags {:#x}", entry.name, UInt32(entry.flags));
            entry.value = readCappedSizedBytes(record, MAX_OUTLINE_FIELD_BYTES, "setting value bytes");
            node.settings.push_back(std::move(entry));
        }

        /// Trailing bytes of a record grown by a newer writer are ignorable and skipped; a
        /// result-changing change would arrive under a new step name, not as record bytes.
        record.ignoreAll();

        outline.nodes.push_back(std::move(node));
    }

    for (UInt64 i = 0; i < set_count; ++i)
    {
        PlanOutline::SetEntry entry;
        readBinary(entry.hash, in);
        readIntBinary(entry.kind, in);
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
    /// Limited by the plan body as well as by the absolute limit: the outline sits inside the body,
    /// so an outline larger than the body has to be rejected before anything is read or allocated.
    /// Otherwise a plan declaring a small body could still make the reader take bytes that belong
    /// to the protocol after it.
    UInt64 outline_size = readCappedVarUInt(in, std::min(MAX_OUTLINE_BYTES, max_frame_bytes), "outline bytes");

    /// Read straight from the stream, bounded to the frame: parsing then never reads past the declared
    /// size, and bytes left inside the frame are detectable, without copying the frame into memory first.
    LimitReadBuffer body(in, {.read_no_more = outline_size});
    try
    {
        auto outline = readOutlineBody(body, max_type_complexity, max_frame_bytes);

        if (body.bytesUntilLimit() != 0)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Query plan outline has {} trailing bytes inside its frame", body.bytesUntilLimit());

        return outline;
    }
    catch (Exception & e)
    {
        /// A frame that ends mid-field is a malformed frame, not an I/O condition of the outer
        /// stream. Nested codec errors (e.g. type decoding) keep their own codes.
        if (e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Query plan outline is truncated: {}", e.message());
        throw;
    }
}

OutlineFrameCounts readOutlineFrameCounts(ReadBuffer & in, UInt64 max_frame_bytes)
{
    UInt64 outline_size = readCappedVarUInt(in, std::min(MAX_OUTLINE_BYTES, max_frame_bytes), "outline bytes");

    LimitReadBuffer body(in, {.read_no_more = outline_size});
    PlanOutline scratch;
    auto counts = readOutlineFrontHeader(body, scratch);
    /// The counts are at the very front of the outline; the rest of the frame is skipped whole,
    /// so a plan this server cannot decode is still measured for the drain.
    body.ignoreAll();
    return counts;
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

    if (node_count == 0)
    {
        shape.issues.push_back("plan has no nodes");
        return shape;
    }

    std::vector<bool> referenced(node_count, false);
    for (size_t i = 0; i < node_count; ++i)
    {
        for (UInt64 child : outline.nodes[i].children)
        {
            /// A child is written before its parent, so its index points back. This keeps the graph
            /// acyclic and lets a reader build each node once its children exist.
            if (child >= i)
            {
                shape.issues.push_back(fmt::format(
                    "node #{} ('{}') has child index {} that does not precede it",
                    i, outline.nodes[i].step_name, child));
                return shape;
            }
            referenced[child] = true;
        }
        shape.children[i].assign(outline.nodes[i].children.begin(), outline.nodes[i].children.end());
    }

    /// The root is the one node nothing points to, and the writer emits it last.
    for (size_t i = 0; i + 1 < node_count; ++i)
        if (!referenced[i])
            shape.issues.push_back(fmt::format(
                "node #{} ('{}') is not an input of any step", i, outline.nodes[i].step_name));
    if (referenced[node_count - 1])
        shape.issues.push_back("the last plan node is an input of another step, so the plan has no single root");

    return shape;
}

QueryPlanOutlineValidationResult validateQueryPlanOutline(const PlanOutline & outline)
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

        /// Each step name owns exactly one payload layout, numbered 1; any other value is a payload
        /// this server cannot read, so the plan is refused rather than misread.
        if (node.step_format_version != 1)
            result.issues.push_back(fmt::format(
                "step '{}' (node #{}) has unknown payload format version {}",
                node.step_name, i, node.step_format_version));

        if (info)
        {
            /// A step is built from its children's headers, so a node whose child count does not
            /// match the input count the step reads would make the factory read a header that is
            /// not there. Caught here, before the step is built.
            if (info->input_count != std::numeric_limits<size_t>::max() && node.children.size() != info->input_count)
                result.issues.push_back(fmt::format(
                    "step '{}' (node #{}) has {} inputs but reads {}",
                    node.step_name, i, node.children.size(), info->input_count));
        }

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
        if (!registry.hasStep(node.step_name))
            writeString(" <unknown step>", out);
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
