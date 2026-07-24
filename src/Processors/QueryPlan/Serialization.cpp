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

#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>

#include <stack>

namespace DB
{

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
    writeVarUInt(version, out);

    SerializationFlags flags;
    flags.version = version;

    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
        serializeEnvelope(out, flags);
    else
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

/// Sanity cap for a hostile envelope size; far above any real plan.
static constexpr UInt64 MAX_QUERY_PLAN_ENVELOPE_BYTES = 2ULL << 30;

void QueryPlan::serializeEnvelope(WriteBuffer & out, const SerializationFlags & flags) const
{
    checkInitialized();

    SerializedSetsRegistry registry;
    const auto & step_registry = QueryPlanStepRegistry::instance();

    PlanOutline outline;
    std::vector<String> payloads;
    UInt64 min_reader_plan_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;

    /// Pre-order walk (children pushed in reverse); Delayed* steps are elided as in the legacy walk.
    std::stack<Node *> stack;
    stack.push(root);
    while (!stack.empty())
    {
        Node * node = stack.top();
        stack.pop();

        while (typeid_cast<DelayedCreatingSetsStep *>(node->step.get())
            || typeid_cast<DelayedMaterializingCTEsStep *>(node->step.get()))
            node = node->children.front();

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
        payloads.push_back(payload.str());

        for (auto it = node->children.rbegin(); it != node->children.rend(); ++it)
            stack.push(*it);
    }

    std::vector<String> set_payloads;
    serializeEnvelopeSets(registry, flags, outline, set_payloads, min_reader_plan_version);

    /// An honest writer never requires a reader newer than the version it writes: features are
    /// gated on `ctx.version`, so a higher requirement here is a missing writer-side gate.
    chassert(min_reader_plan_version <= flags.version);

    WriteBufferFromOwnString envelope;
    writeQueryPlanOutline(outline, envelope);
    for (const auto & payload : payloads)
        envelope.write(payload.data(), payload.size());
    for (const auto & payload : set_payloads)
        envelope.write(payload.data(), payload.size());
    envelope.finalize();

    writeVarUInt(min_reader_plan_version, out);
    writeVarUInt(envelope.str().size(), out);
    out.write(envelope.str().data(), envelope.str().size());
}

QueryPlanAndSets QueryPlan::deserializeEnvelope(ReadBuffer & in, const ContextPtr & context, const SerializationFlags & flags, size_t max_type_complexity, UInt64 min_reader_plan_version)
{
    UInt64 envelope_size = 0;
    readVarUInt(envelope_size, in);
    if (envelope_size > MAX_QUERY_PLAN_ENVELOPE_BYTES)
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan envelope declares {} bytes which exceeds the limit of {}",
            envelope_size, MAX_QUERY_PLAN_ENVELOPE_BYTES);

    String envelope;
    envelope.resize(envelope_size);
    try
    {
        in.readStrict(envelope.data(), envelope.size());
    }
    catch (Exception & e)
    {
        /// A stream that ends before the declared envelope size is a malformed plan, not an I/O
        /// condition of the connection.
        e.addMessage(fmt::format("while reading a query plan envelope of {} bytes", envelope.size()));
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN, "Query plan envelope is truncated: {}", e.message());
    }

    ReadBufferFromMemory envelope_buffer(envelope.data(), envelope.size());
    auto outline = readQueryPlanOutline(envelope_buffer, max_type_complexity);

    /// One pass over the outline reports every problem at once (unknown steps, unsupported step
    /// versions, unknown settings) instead of failing on the first byte deep inside a payload.
    auto validation = validateQueryPlanOutline(outline, flags.version, min_reader_plan_version);
    if (!validation.ok())
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan cannot be deserialized: {}", validation.describe());

    size_t node_count = outline.nodes.size();

    /// Payload offsets in the envelope (Section B starts where the outline ended) and the
    /// children lists reconstructed from the pre-order child counts.
    std::vector<size_t> payload_offsets(node_count);
    std::vector<std::vector<size_t>> children_indices(node_count);
    size_t offset = envelope_buffer.count();
    {
        std::vector<std::pair<size_t, UInt64>> parents; /// (node index, remaining child slots)
        for (size_t i = 0; i < node_count; ++i)
        {
            const auto & outline_node = outline.nodes[i];

            payload_offsets[i] = offset;
            /// Subtraction, not addition: `offset + payload_size` could wrap around on a hostile size.
            if (outline_node.payload_size > envelope.size() - offset)
                throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
                    "Query plan payload of step '{}' extends past the envelope", outline_node.step_name);
            offset += outline_node.payload_size;

            if (i > 0)
            {
                children_indices[parents.back().first].push_back(i);
                if (--parents.back().second == 0)
                    parents.pop_back();
            }
            if (outline_node.child_count > 0)
                parents.emplace_back(i, outline_node.child_count);
        }
    }

    QueryPlanStepRegistry & step_registry = QueryPlanStepRegistry::instance();
    DeserializedSetsRegistry sets_registry;

    QueryPlan plan;
    std::vector<Node *> nodes_by_index(node_count);

    /// In pre-order all descendants of a node follow it, so iterating backwards constructs every
    /// child before its parent. Parents receive the constructed children's output headers (the
    /// same contract as the legacy stream: serialized headers do not carry constants, steps
    /// refill them, and e.g. `UnionStep` depends on child header constness).
    for (size_t idx = node_count; idx-- > 0;)
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

        ReadBufferFromMemory payload(envelope.data() + payload_offsets[idx], outline_node.payload_size);
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

        /// A payload tail the step did not consume is a newer, ignorable extension of its format;
        /// the frame makes it skippable by construction.

        auto & node = plan.nodes.emplace_back(std::move(step), std::move(children));
        nodes_by_index[idx] = &node;

        for (const auto & storage : ctx.storage_holders)
            plan.addStorageHolder(storage);
    }

    plan.root = nodes_by_index[0];

    ReadBufferFromMemory sets_buffer(envelope.data() + offset, envelope.size() - offset);
    auto res = deserializeEnvelopeSets(
        std::move(plan), sets_registry, outline, sets_buffer, flags, context, max_type_complexity);

    if (!sets_buffer.eof())
        throw Exception(ErrorCodes::CANNOT_PARSE_QUERY_PLAN,
            "Query plan envelope has {} trailing bytes after the sets section", sets_buffer.available());

    return res;
}

void QueryPlan::ensureSerialized(size_t max_supported_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version);
    auto & buffer = serialized_plans[version];
    if (buffer)
        return;  // Already serialized for this version

    buffer = std::make_unique<WriteBufferFromOwnString>();
    serialize(*buffer, version);
    buffer->finalize();
}

std::string_view QueryPlan::getSerializedData(size_t max_supported_version) const
{
    auto it = serialized_plans.find(effectiveSerializationVersion(max_supported_version));
    if (it == serialized_plans.end() || !it->second)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Query plan is not serialized for version {}. Call ensureSerialized() first.",
            effectiveSerializationVersion(max_supported_version));

    return it->second->stringView();
}

bool QueryPlan::isSerialized(size_t max_supported_version) const
{
    auto it = serialized_plans.find(effectiveSerializationVersion(max_supported_version));
    return it != serialized_plans.end() && it->second != nullptr;
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
