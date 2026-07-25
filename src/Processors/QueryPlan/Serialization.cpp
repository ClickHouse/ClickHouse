#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanEnvelope.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>

#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>

#include <stack>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_serialized_query_plan_size;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_PARSE_QUERY_PLAN;
}

void serializeQueryPlanHeader(const Block & header, WriteBuffer & out)
{
    /// Write only names and types.
    /// Constants should be filled by step.

    writeVarUInt(header.columns(), out);
    for (const auto & column : header)
    {
        writeStringBinary(column.name, out);
        encodeDataType(column.type, out);
    }
}

/// Sanity caps for hostile input; far above any real plan. Checked before allocation.
static constexpr UInt64 MAX_QUERY_PLAN_HEADER_COLUMNS = 1'000'000;
static constexpr UInt64 MAX_QUERY_PLAN_STRING_BYTES = 16ULL << 20;

Block deserializeQueryPlanHeader(ReadBuffer & in, size_t max_type_complexity)
{
    UInt64 num_columns = 0;
    readVarUInt(num_columns, in);
    if (num_columns > MAX_QUERY_PLAN_HEADER_COLUMNS)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Serialized query plan header declares {} columns which exceeds the limit of {}",
            num_columns, MAX_QUERY_PLAN_HEADER_COLUMNS);

    ColumnsWithTypeAndName columns(num_columns);

    for (auto & column : columns)
    {
        readStringBinary(column.name, in, MAX_QUERY_PLAN_STRING_BYTES);
        column.type = decodeDataType(in, max_type_complexity);
    }

    /// Fill columns in header. Some steps expect them to be not empty.
    for (auto & column : columns)
        column.column = column.type->createColumn();

    return Block(std::move(columns));
}

/// The version a plan is written with for a peer that supports up to `max_supported_version`.
/// v4+ peers accept streams by the content's needed-to-read version, so they all get the writer's
/// own version (one byte string serves a whole mixed v4+ fleet); only pre-outline peers need the
/// stream clamped down to what they can parse.
static UInt64 effectiveSerializationVersion(size_t max_supported_version)
{
    if (max_supported_version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
        return DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    return std::min<UInt64>(max_supported_version, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
}

void QueryPlan::serialize(WriteBuffer & out, size_t max_supported_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version);

    SerializationFlags flags;
    flags.version = version;

    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
    {
        auto chunks = serializeEnvelopeToChunks(flags);
        /// Each chunk is released once it has been written, so the plan is not held twice.
        for (auto & chunk : chunks)
        {
            out.write(chunk.data(), chunk.size());
            String{}.swap(chunk);
        }
        return;
    }

    writeVarUInt(version, out);
    serialize(out, flags);
}

void QueryPlan::serialize(WriteBuffer & out, const SerializationFlags & flags) const
{
    checkInitialized();

    SerializedSetsRegistry registry;

    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});
    while (!stack.empty())
    {
        auto & frame = stack.top();
        auto * node = frame.node;

        if (typeid_cast<DelayedCreatingSetsStep *>(node->step.get())
            || typeid_cast<DelayedMaterializingCTEsStep *>(node->step.get()))
        {
            frame.node = node->children.front();
            continue;
        }

        if (frame.next_child == 0)
        {
            writeVarUInt(node->children.size(), out);
        }

        if (frame.next_child < node->children.size())
        {
            stack.push(Frame{.node = node->children[frame.next_child]});
            ++frame.next_child;
            continue;
        }

        stack.pop();

        writeStringBinary(node->step->getSerializationName(), out);
        writeStringBinary(node->step->getStepDescription(), out);

        if (node->step->hasOutputHeader())
            serializeQueryPlanHeader(*node->step->getOutputHeader(), out);
        else
            serializeQueryPlanHeader({}, out);

        QueryPlanSerializationSettings settings;
        node->step->serializeSettings(settings);

        settings.writeChangedBinary(out);

        IQueryPlanStep::Serialization ctx{out, registry};
        ctx.version = flags.version;
        node->step->serialize(ctx);
    }

    serializeSets(registry, out, flags);
}

QueryPlan::SerializedChunks QueryPlan::serializeEnvelopeToChunks(const SerializationFlags & flags) const
{
    checkInitialized();

    SerializedSetsRegistry registry;
    const auto & step_registry = QueryPlanStepRegistry::instance();

    PlanOutline outline;
    std::vector<String> payloads;
    UInt64 min_reader_plan_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;

    /// Left-to-right post-order: children are emitted before their parent, so a reader builds each
    /// step as its payload arrives and never has to hold the whole envelope. Delayed* steps are
    /// elided as in the legacy walk.
    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
    };

    std::stack<Frame> stack;
    stack.push(Frame{.node = root});
    while (!stack.empty())
    {
        auto & frame = stack.top();

        while (typeid_cast<DelayedCreatingSetsStep *>(frame.node->step.get())
            || typeid_cast<DelayedMaterializingCTEsStep *>(frame.node->step.get()))
            frame.node = frame.node->children.front();

        Node * node = frame.node;

        if (frame.next_child < node->children.size())
        {
            Node * child = node->children[frame.next_child];
            ++frame.next_child;
            stack.push(Frame{.node = child});
            continue;
        }

        stack.pop();

        PlanOutline::Node outline_node;
        outline_node.child_count = node->children.size();
        outline_node.step_name = node->step->getSerializationName();
        outline_node.step_description = node->step->getStepDescription();

        const auto * info = step_registry.getStepSerializationInfo(outline_node.step_name);
        outline_node.has_output_header = node->step->hasOutputHeader();
        if (outline_node.has_output_header)
            outline_node.header = node->step->getOutputHeader();

        QueryPlanSerializationSettings settings;
        node->step->serializeSettings(settings);
        outline_node.settings = settings.getChangedEntries();

        WriteBufferFromOwnString payload;
        IQueryPlanStep::Serialization ctx{payload, registry};
        ctx.version = flags.version;
        ctx.step_format_version = info ? info->max_step_format_version : 1;
        node->step->serialize(ctx);
        payload.finalize();

        /// The step may have emitted an older payload form and lowered the context value; the
        /// outline must advertise the format of the bytes actually written.
        outline_node.step_format_version = ctx.step_format_version;
        outline_node.payload_size = payload.str().size();

        /// "Needed to read" for this node: the step's registry requirements for the format
        /// actually written, its value-dependent requirements, and the header's type encodings.
        UInt64 node_min_reader = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;
        if (info)
        {
            node_min_reader = std::max(node_min_reader, info->introduced_in_plan_version);
            for (const auto & [step_version, min_plan_version] : info->min_plan_version_for_step_version)
                if (ctx.step_format_version >= step_version)
                    node_min_reader = std::max(node_min_reader, min_plan_version);
        }
        node_min_reader = std::max(node_min_reader, ctx.min_reader_version);
        if (outline_node.header)
            for (const auto & column : *outline_node.header)
                node_min_reader = std::max(node_min_reader, minReaderVersionForType(*column.type));
        for (const auto & entry : outline_node.settings)
            node_min_reader = std::max(node_min_reader, QueryPlanSerializationSettings::minReaderVersionForEntry(entry));

        outline_node.min_reader_plan_version = node_min_reader;
        min_reader_plan_version = std::max(min_reader_plan_version, node_min_reader);

        outline.nodes.push_back(std::move(outline_node));
        /// `payload` is finalized and goes out of scope here, so its bytes move rather than copy.
        payloads.push_back(std::move(payload.str()));
    }

    std::vector<String> set_payloads;
    serializeEnvelopeSets(registry, flags, outline, set_payloads, min_reader_plan_version);

    /// An honest writer never requires a reader newer than the version it writes: features are
    /// gated on `ctx.version`, so a higher requirement here is a missing writer-side gate.
    chassert(min_reader_plan_version <= flags.version);

    /// The payload sizes are known, so the envelope size the reader needs up front can be summed
    /// without joining the payloads together.
    WriteBufferFromOwnString outline_bytes;
    writeQueryPlanOutline(outline, outline_bytes);
    outline_bytes.finalize();

    size_t envelope_size = outline_bytes.str().size();
    for (const auto & payload : payloads)
        envelope_size += payload.size();
    for (const auto & payload : set_payloads)
        envelope_size += payload.size();

    /// The head carries the stream version, the "needed to read" version, the envelope size and
    /// the outline; every payload then stays the chunk it was serialized into.
    SerializedChunks chunks;
    chunks.reserve(1 + payloads.size() + set_payloads.size());

    WriteBufferFromOwnString head;
    writeVarUInt(flags.version, head);
    writeVarUInt(min_reader_plan_version, head);
    writeVarUInt(envelope_size, head);
    head.write(outline_bytes.str().data(), outline_bytes.str().size());
    head.finalize();
    chunks.push_back(std::move(head.str()));

    for (auto & payload : payloads)
        chunks.push_back(std::move(payload));
    for (auto & payload : set_payloads)
        chunks.push_back(std::move(payload));

    return chunks;
}

/// Bounds the memory one plan can take before anything is buffered. A server setting, not a query
/// one: a query setting arrives from the sender, who could then pick its own limit.
static UInt64 readEnvelopeSize(ReadBuffer & in, const ContextPtr & context)
{
    const UInt64 max_envelope_bytes = context->getServerSettings()[ServerSetting::max_serialized_query_plan_size];

    UInt64 envelope_size = 0;
    readVarUInt(envelope_size, in);
    if (envelope_size > max_envelope_bytes)
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan envelope declares {} bytes which exceeds `max_serialized_query_plan_size` = {}",
            envelope_size, max_envelope_bytes);

    return envelope_size;
}

/// Takes the whole envelope off the stream without decoding any of it. The frame is self-delimiting,
/// so a plan that will be discarded costs no allocation and no step construction.
static void skipQueryPlanEnvelope(ReadBuffer & in, const ContextPtr & context)
{
    const UInt64 envelope_size = readEnvelopeSize(in, context);
    try
    {
        in.ignore(envelope_size);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format("while skipping a query plan envelope of {} bytes", envelope_size));
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN, "Query plan envelope is truncated: {}", e.message());
    }
}

QueryPlanAndSets QueryPlan::deserializeEnvelope(ReadBuffer & in, const ContextPtr & context, const SerializationFlags & flags, size_t max_type_complexity, UInt64 min_reader_plan_version)
{
    const UInt64 envelope_size = readEnvelopeSize(in, context);
    const size_t envelope_start = in.count();

    auto outline = readQueryPlanOutline(in, max_type_complexity, envelope_size);

    size_t consumed = in.count() - envelope_start;
    if (consumed > envelope_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan outline of {} bytes does not fit the envelope of {}", consumed, envelope_size);

    /// One pass over the outline reports every problem at once (unknown steps, unsupported step
    /// versions, unknown settings) instead of failing on the first byte deep inside a payload.
    auto validation = validateQueryPlanOutline(outline, flags.version, min_reader_plan_version);
    if (!validation.ok())
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan cannot be deserialized: {}", validation.describe());

    const size_t node_count = outline.nodes.size();

    /// Validation already reported any structural issue; rebuilding here must therefore succeed.
    auto shape = reconstructOutlineShape(outline);
    if (!shape.ok())
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan cannot be deserialized: {}", shape.issues.front());
    const auto & children_indices = shape.children;

    /// Every declared frame is checked against the envelope before a single step is built, so a
    /// plan whose sizes do not add up is rejected without constructing anything. Subtracting from
    /// the budget keeps a hostile size from overflowing.
    {
        UInt64 budget = envelope_size - consumed;
        for (const auto & outline_node : outline.nodes)
        {
            if (outline_node.payload_size > budget)
                throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                    "Query plan payload of step '{}' extends past the envelope", outline_node.step_name);
            budget -= outline_node.payload_size;
        }
        for (const auto & set : outline.sets)
        {
            if (set.payload_size > budget)
                throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                    "Serialized set {}_{} extends past the envelope", set.hash.low64, set.hash.high64);
            budget -= set.payload_size;
        }
        if (budget != 0)
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                "Query plan envelope has {} bytes that no frame accounts for", budget);
    }

    QueryPlanStepRegistry & step_registry = QueryPlanStepRegistry::instance();
    DeserializedSetsRegistry sets_registry;

    QueryPlan plan;
    std::vector<Node *> nodes_by_index(node_count);

    /// Nodes arrive children-first, so a forward walk always has the children of the node it is
    /// building. Parents receive the constructed children's output headers (the same contract as
    /// the legacy stream: serialized headers do not carry constants, steps refill them, and e.g.
    /// `UnionStep` depends on child header constness).
    String payload_bytes;
    for (size_t idx = 0; idx < node_count; ++idx)
    {
        const auto & outline_node = outline.nodes[idx];

        std::vector<Node *> children;
        SharedHeaders input_headers;
        children.reserve(children_indices[idx].size());
        input_headers.reserve(children_indices[idx].size());
        for (size_t child_index : children_indices[idx])
        {
            children.push_back(nodes_by_index[child_index]);
            input_headers.push_back(nodes_by_index[child_index]->step->getOutputHeader());
        }

        SharedHeader output_header = outline_node.has_output_header
            ? outline_node.header
            : std::make_shared<const Block>();

        QueryPlanSerializationSettings settings;
        settings.applyEntries(outline_node.settings);

        /// One frame at a time: the buffer is reused, so only the largest payload is ever held.
        payload_bytes.resize(outline_node.payload_size);
        try
        {
            in.readStrict(payload_bytes.data(), payload_bytes.size());
        }
        catch (Exception & e)
        {
            e.addMessage(fmt::format("while reading the payload of step '{}' (node #{}, {} bytes)",
                outline_node.step_name, idx, outline_node.payload_size));
            throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN, "Query plan envelope is truncated: {}", e.message());
        }

        ReadBufferFromMemory payload(payload_bytes.data(), payload_bytes.size());
        IQueryPlanStep::Deserialization ctx{
            payload, sets_registry, {}, context, input_headers, output_header, settings,
            max_type_complexity, flags.version, flags.skip_data, outline_node.step_format_version};
        auto step = step_registry.createStep(outline_node.step_name, ctx);

        if (step->hasOutputHeader())
        {
            assertCompatibleHeader(
                *step->getOutputHeader(), *output_header,
                fmt::format("deserialization of query plan {} step", outline_node.step_name));
        }
        else if (output_header->columns())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Deserialized step {} has no output stream, but deserialized header is not empty : {}",
                outline_node.step_name, output_header->dumpStructure());

        /// A tail is only legitimate when the writer used a payload format this binary does not
        /// know: every change to a payload, an ignorable append included, bumps
        /// `step_format_version`. At a format we do know, leftover bytes mean a corrupt stream or
        /// a writer bug, and accepting them would let a malformed plan run.
        if (!payload.eof())
        {
            const auto * info = step_registry.getStepSerializationInfo(outline_node.step_name);
            UInt64 known_format_version = info ? info->max_step_format_version : 1;
            if (outline_node.step_format_version <= known_format_version)
                throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                    "Step {} left {} of its {} payload bytes unread at step format version {}, "
                    "which this server knows in full",
                    outline_node.step_name, payload.available(), outline_node.payload_size,
                    outline_node.step_format_version);
        }

        auto & node = plan.nodes.emplace_back(std::move(step), std::move(children));
        nodes_by_index[idx] = &node;

        for (const auto & storage : ctx.storage_holders)
            plan.addStorageHolder(storage);
    }

    /// Children-first order puts the root last.
    plan.root = nodes_by_index[node_count - 1];

    auto res = deserializeEnvelopeSets(
        std::move(plan), sets_registry, outline, in, flags, context, max_type_complexity);

    /// The budget above proved the declared frames fill the envelope exactly; this proves the
    /// reader consumed exactly those frames and nothing else.
    const size_t total_consumed = in.count() - envelope_start;
    if (total_consumed != envelope_size)
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan envelope declared {} bytes but {} were read", envelope_size, total_consumed);

    return res;
}

void QueryPlan::ensureSerialized(size_t max_supported_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version);

    std::lock_guard lock(serialized_plans.mutex);
    if (serialized_plans.plans.contains(version))
        return;  // Already serialized for this version

    /// The entry is published only once it is complete, so a concurrent sender either does not
    /// see it and waits here, or gets the whole plan. Nothing is cached if serializing throws.
    SerializationFlags flags;
    flags.version = version;

    SerializedChunks chunks;
    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
    {
        chunks = serializeEnvelopeToChunks(flags);
    }
    else
    {
        /// A legacy stream is written in one pass, so it is cached as a single chunk.
        WriteBufferFromOwnString buffer;
        writeVarUInt(version, buffer);
        serialize(buffer, flags);
        buffer.finalize();
        chunks.push_back(std::move(buffer.str()));
    }

    serialized_plans.plans.emplace(version, std::make_shared<const SerializedChunks>(std::move(chunks)));
}

void QueryPlan::writeSerializedTo(WriteBuffer & out, size_t max_supported_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version);

    std::shared_ptr<const SerializedChunks> chunks;
    {
        std::lock_guard lock(serialized_plans.mutex);
        auto it = serialized_plans.plans.find(version);
        if (it == serialized_plans.plans.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Query plan is not serialized for version {}. Call ensureSerialized() first.", version);
        chunks = it->second;
    }

    /// Written outside the lock: the entry is immutable once published, and a slow peer must not
    /// hold up the other senders.
    for (const auto & chunk : *chunks)
        out.write(chunk.data(), chunk.size());
}

bool QueryPlan::isSerialized(size_t max_supported_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version);

    std::lock_guard lock(serialized_plans.mutex);
    return serialized_plans.plans.contains(version);
}

QueryPlanAndSets QueryPlan::deserialize(ReadBuffer & in, const ContextPtr & context, size_t max_type_complexity, bool skip_data)
{
    UInt64 version = 0;
    readVarUInt(version, in);

    SerializationFlags flags{.version = version, .skip_data = skip_data};

    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
    {
        /// Acceptance is decided by the content's "needed to read" version, not by the writer's
        /// version: a newer writer's plan is readable as long as everything above this reader's
        /// knowledge is ignorable, which the writer's fold guarantees for min_reader <= supported.
        UInt64 min_reader_plan_version = 0;
        readVarUInt(min_reader_plan_version, in);

        /// The plan is only being drained off the connection, so the envelope frame is consumed
        /// without parsing it: no steps are built and no set data is decoded. A plan too new to
        /// read is drained the same way, which leaves the connection usable.
        if (flags.skip_data)
        {
            skipQueryPlanEnvelope(in, context);
            return {};
        }

        if (min_reader_plan_version > DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The query plan requires serialization version {} while this server supports up to {}",
                min_reader_plan_version, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

        return deserializeEnvelope(in, context, flags, max_type_complexity, min_reader_plan_version);
    }

    if (version > DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Query plan serialization version {} is not supported. The last supported version is {}",
            version, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    /// A legacy stream declares no size, so `max_serialized_query_plan_size` does not apply here:
    /// the reader consumes the plan field by field as it arrives, with per-field caps, rather than
    /// buffering it whole. Only a peer older than v4 sends one.
    return deserialize(in, context, flags, max_type_complexity);
}

QueryPlanAndSets QueryPlan::deserialize(ReadBuffer & in, const ContextPtr & context, const SerializationFlags & flags, size_t max_type_complexity)
{
    QueryPlanStepRegistry & step_registry = QueryPlanStepRegistry::instance();

    DeserializedSetsRegistry sets_registry;

    using NodePtr = Node *;
    struct Frame
    {
        NodePtr & to_fill;
        size_t next_child = 0;
        std::vector<Node *> children = {};
    };

    std::stack<Frame> stack;

    QueryPlan plan;
    stack.push(Frame{.to_fill = plan.root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (frame.next_child == 0)
        {
            UInt64 num_children = 0;
            readVarUInt(num_children, in);
            frame.children.resize(num_children);
        }

        if (frame.next_child < frame.children.size())
        {
            stack.push(Frame{.to_fill = frame.children[frame.next_child]});
            ++frame.next_child;
            continue;
        }

        std::string step_name;
        std::string step_description;
        readStringBinary(step_name, in, MAX_QUERY_PLAN_STRING_BYTES);
        readStringBinary(step_description, in, MAX_QUERY_PLAN_STRING_BYTES);

        auto output_header  = std::make_shared<const Block>(deserializeQueryPlanHeader(in, max_type_complexity));

        QueryPlanSerializationSettings settings;
        settings.readBinary(in);

        SharedHeaders input_headers;
        input_headers.reserve(frame.children.size());
        for (const auto & child : frame.children)
            input_headers.push_back(child->step->getOutputHeader());

        IQueryPlanStep::Deserialization ctx{
            in, sets_registry, {}, context, input_headers, output_header, settings, max_type_complexity, flags.version, flags.skip_data};
        auto step = step_registry.createStep(step_name, ctx);

        if (step->hasOutputHeader())
        {
            assertCompatibleHeader(
                *step->getOutputHeader(), *output_header, fmt::format("deserialization of query plan {} step", step_name));
        }
        else if (output_header->columns())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Deserialized step {} has no output stream, but deserialized header is not empty : {}",
                step_name, output_header->dumpStructure());

        auto & node = plan.nodes.emplace_back(std::move(step), std::move(frame.children));
        frame.to_fill = &node;

        for (const auto & storage : ctx.storage_holders)
            plan.addStorageHolder(storage);

        stack.pop();
    }

    return deserializeSets(std::move(plan), sets_registry, in, flags, context, max_type_complexity);
}

}
