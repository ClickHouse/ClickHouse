#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanOutline.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/MaterializingCTEStep.h>

#include <IO/LimitReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>

#include <base/scope_guard.h>

#include <stack>

namespace DB
{

namespace ServerSetting
{
    extern const ServerSettingsUInt64 max_query_plan_serialization_version;
    extern const ServerSettingsUInt64 max_serialized_query_plan_size;
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int LOGICAL_ERROR;
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
static constexpr UInt64 MAX_LEGACY_PLAN_CHILDREN = 1ULL << 20;

static bool haveSameSerializedHeader(const Block & lhs, const Block & rhs)
{
    WriteBufferFromOwnString lhs_buf;
    WriteBufferFromOwnString rhs_buf;
    serializeQueryPlanHeader(lhs, lhs_buf);
    serializeQueryPlanHeader(rhs, rhs_buf);
    return lhs_buf.stringView() == rhs_buf.stringView();
}

Block deserializeQueryPlanHeader(ReadBuffer & in, size_t max_type_complexity)
{
    UInt64 num_columns = 0;
    readVarUInt(num_columns, in);
    if (num_columns > MAX_QUERY_PLAN_HEADER_COLUMNS)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Serialized query plan header declares {} columns which exceeds the limit of {}",
            num_columns, MAX_QUERY_PLAN_HEADER_COLUMNS);

    /// The count comes from the peer: columns are built as they are read, so a header that ends
    /// early only pays for what it delivered.
    ColumnsWithTypeAndName columns;

    for (UInt64 i = 0; i < num_columns; ++i)
    {
        ColumnWithTypeAndName column;
        readStringBinary(column.name, in, MAX_QUERY_PLAN_STRING_BYTES);
        column.type = decodeDataType(in, max_type_complexity);
        columns.push_back(std::move(column));
    }

    /// Fill columns in header. Some steps expect them to be not empty.
    for (auto & column : columns)
        column.column = column.type->createColumn();

    return Block(std::move(columns));
}

/// The version to write: what the query asked for, or the server default, never above what this
/// binary can write. `requested_version` of 0 means the query did not ask.
static UInt64 writerSerializationVersion(UInt64 requested_version)
{
    UInt64 writer_version = requested_version != 0 ? requested_version : DBMS_DEFAULT_QUERY_PLAN_SERIALIZATION_VERSION;

    const UInt64 supported_version = QueryPlanStepRegistry::instance().supportedVersion();
    if (writer_version > supported_version)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Query plan serialization version {} was requested but this server writes up to {}",
            writer_version, supported_version);

    /// An operator can hold every writer at an older version while a fleet is mixed, so a plan the
    /// not-yet-upgraded servers cannot read is never written, whatever a query asks for. A missing
    /// global context means there is no configuration to read, as in unit tests.
    if (auto global_context = Context::getGlobalContextInstance())
    {
        UInt64 ceiling = global_context->getServerSettings()[ServerSetting::max_query_plan_serialization_version];
        if (ceiling != 0)
        {
            /// Lowering the default silently is the ceiling doing its job, but a query that named a
            /// version has to hear that it did not get it.
            if (requested_version != 0 && requested_version > ceiling)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Query plan serialization version {} was requested but this server is held at {} "
                    "by `max_query_plan_serialization_version`", requested_version, ceiling);

            writer_version = std::min(writer_version, ceiling);
        }
    }

    return writer_version;
}

/// The version a plan is written with for a peer that supports up to `max_supported_version`: the
/// version the query or server chose, clamped to what the peer can read. A peer refuses a version
/// above the one it supports, so the writer never emits one above the peer's maximum, whether the
/// peer is on the framed format or the older one.
static UInt64 effectiveSerializationVersion(size_t max_supported_version, UInt64 requested_version)
{
    return std::min<UInt64>(max_supported_version, writerSerializationVersion(requested_version));
}

void QueryPlan::serialize(WriteBuffer & out, size_t max_supported_version, UInt64 requested_version) const
{
    SerializationFlags flags;
    flags.version = effectiveSerializationVersion(max_supported_version, requested_version);
    serializeWithFlags(out, flags);
}

void QueryPlan::serializeForDistributedTask(
    WriteBuffer & out, size_t max_supported_version, const SizeLimits & sets_transfer_limits, UInt64 requested_version) const
{
    SerializationFlags flags;
    flags.version = effectiveSerializationVersion(max_supported_version, requested_version);
    flags.sets_must_be_ready = true;
    flags.sets_transfer_limits = sets_transfer_limits;
    serializeWithFlags(out, flags);
}

void QueryPlan::serializeWithFlags(WriteBuffer & out, const SerializationFlags & flags) const
{
    if (flags.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
    {
        auto chunks = serializeFramedToChunks(flags);
        /// Each chunk is released once it has been written, so the plan is not held twice.
        for (auto & chunk : chunks)
        {
            out.write(chunk.data(), chunk.size());
            String{}.swap(chunk);
        }
        return;
    }

    /// The older layout has no place for the plan-level limits before version 10, and a plan that
    /// silently lost them would run with defaults on the other side.
    if (flags.version < DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXECUTION_LIMITS && (max_threads || concurrency_control))
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Cannot serialize a query plan with execution limits for serialization version {}; version {} or newer is required",
            flags.version,
            DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXECUTION_LIMITS);

    writeVarUInt(flags.version, out);
    serialize(out, flags);
}

void QueryPlan::serialize(WriteBuffer & out, const SerializationFlags & flags) const
{
    checkInitialized();

    if (flags.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXECUTION_LIMITS)
    {
        writeVarUInt(max_threads, out);
        writeBinary(concurrency_control, out);
    }

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
        node->step->serializeSettings(settings, flags.version);

        settings.writeChangedBinary(out);

        IQueryPlanStep::Serialization ctx{out, registry};
        ctx.version = flags.version;
        node->step->serialize(ctx);
    }

    serializeSets(registry, out, flags);
}

QueryPlan::SerializedChunks QueryPlan::serializeFramedToChunks(const SerializationFlags & flags) const
{
    checkInitialized();

    SerializedSetsRegistry registry;

    PlanOutline outline;
    outline.max_threads = max_threads;
    outline.concurrency_control = concurrency_control;
    outline.include_step_descriptions = flags.with_step_descriptions;
    std::vector<String> payloads;

    /// Children are written before their parent, siblings left to right, so a reader builds each
    /// step as its payload arrives instead of holding the whole plan. `Delayed*` steps are skipped
    /// here, the same way the older walk skips them.
    struct Frame
    {
        Node * node = {};
        size_t next_child = 0;
        /// Indices of this node's children in `outline.nodes`, filled as each child is emitted.
        std::vector<UInt64> child_indices = {};
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

        std::vector<UInt64> child_indices = std::move(frame.child_indices);
        stack.pop();

        PlanOutline::Node outline_node;
        outline_node.children = std::move(child_indices);
        outline_node.step_name = node->step->getSerializationName();
        if (flags.with_step_descriptions)
            outline_node.step_description = node->step->getStepDescription();

        if (node->step->hasOutputHeader())
            outline_node.header = node->step->getOutputHeader();

        QueryPlanSerializationSettings settings;
        node->step->serializeSettings(settings, flags.version);
        outline_node.settings = settings.getChangedEntries();

        WriteBufferFromOwnString payload;
        IQueryPlanStep::Serialization ctx{payload, registry};
        ctx.version = flags.version;
        node->step->serialize(ctx);
        payload.finalize();

        /// Each step name owns one payload layout, so the outline always names format 1; the reader
        /// refuses any other value. A step that changed its wire content would take a new name, not a
        /// higher number here.
        if (ctx.step_format_version != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Step {} wrote payload format {} but only format 1 is defined",
                outline_node.step_name, ctx.step_format_version);

        outline_node.step_format_version = ctx.step_format_version;

        const UInt64 node_index = outline.nodes.size();
        outline.nodes.push_back(std::move(outline_node));
        if (!stack.empty())
            stack.top().child_indices.push_back(node_index);
        /// `payload` is finalized and goes out of scope here, so its bytes move rather than copy.
        payloads.push_back(std::move(payload.str()));
    }

    std::vector<String> set_payloads;
    serializeFramedSets(registry, flags, outline, set_payloads);

    WriteBufferFromOwnString outline_bytes;
    writeQueryPlanOutline(outline, outline_bytes);
    outline_bytes.finalize();

    /// The head carries the stream version and the body layout, and is followed by the outline. Each
    /// payload then keeps the chunk it was serialized into, with its size written just before it so
    /// the reader can walk the payloads one at a time.
    SerializedChunks chunks;
    chunks.reserve(1 + 2 * (payloads.size() + set_payloads.size()));

    WriteBufferFromOwnString head;
    writeVarUInt(flags.version, head);
    writeVarUInt(UInt64(DBMS_QUERY_PLAN_FORMAT_KIND_OUTLINE), head);
    head.write(outline_bytes.str().data(), outline_bytes.str().size());
    head.finalize();
    chunks.push_back(std::move(head.str()));

    auto push_sized = [&chunks](String & payload)
    {
        WriteBufferFromOwnString size_bytes;
        writeVarUInt(payload.size(), size_bytes);
        size_bytes.finalize();
        chunks.push_back(std::move(size_bytes.str()));
        chunks.push_back(std::move(payload));
    };

    for (auto & payload : payloads)
        push_sized(payload);
    for (auto & payload : set_payloads)
        push_sized(payload);

    return chunks;
}

/// Skips `frame_count` sized payload frames on `in`: each is a varint size then that many bytes.
/// Reads and discards, allocating nothing for the sizes the frames declare, so it reads only what
/// is on the wire. Called only at a frame boundary, so it stays aligned frame to frame.
static void skipSizedFrames(ReadBuffer & in, UInt64 frame_count)
{
    for (UInt64 i = 0; i < frame_count; ++i)
    {
        UInt64 size = 0;
        readVarUInt(size, in);
        in.ignore(size);
    }
}

/// Takes an outline-format body off the stream without building anything: reads the outline for its
/// node and set counts, then skips that many sized payload frames. Leaves the stream at whatever
/// follows the plan, so a refused plan costs the query and not the connection. It reads only what is
/// on the wire; a stream that ends mid-plan makes the read throw.
static void drainOutlineBody(ReadBuffer & in, UInt64 max_plan_bytes)
{
    auto counts = readOutlineFrameCounts(in, max_plan_bytes);
    skipSizedFrames(in, counts.node_count + counts.set_count);
}

QueryPlanAndSets QueryPlan::deserializeFramedBody(
    ReadBuffer & in, const ContextPtr & context, const SerializationFlags & flags,
    size_t max_type_complexity, UInt64 max_plan_bytes)
{
    const size_t body_start = in.count();
    auto budget_left = [&]() -> UInt64
    {
        const size_t used = in.count() - body_start;
        return used >= max_plan_bytes ? 0 : max_plan_bytes - used;
    };

    /// The whole plan body must fit the plan-size limit, and the outline is bounded to it as well.
    auto outline = readQueryPlanOutline(in, max_type_complexity, budget_left());

    /// One pass over the outline reports every problem at once (unknown steps, unknown settings,
    /// unknown set kinds) instead of failing on the first byte deep inside a payload.
    auto validation = validateQueryPlanOutline(outline);

    const size_t node_count = outline.nodes.size();
    const UInt64 frames_total = node_count + outline.sets.size();
    UInt64 frames_consumed = 0;

    /// Any failure below leaves the frames it did not reach on the stream. They are skipped here so
    /// the connection is left at the next packet, then the original error is re-thrown.
    try
    {
        if (!validation.ok())
            throw Exception(ErrorCodes::INCORRECT_DATA,
                "Query plan cannot be deserialized: {}", validation.describe());

        const auto & children_indices = validation.shape.children;

        QueryPlanStepRegistry & step_registry = QueryPlanStepRegistry::instance();
        DeserializedSetsRegistry sets_registry;

        QueryPlan plan;
        plan.max_threads = outline.max_threads;
        plan.concurrency_control = outline.concurrency_control;
        std::vector<Node *> nodes_by_index(node_count);

        /// Children arrive before their parent, so a forward walk always has the children of the node
        /// it is building. A parent gets the output headers of the children as they were built, which
        /// is what the older stream did too: headers on the wire drop constants, steps refill them, and
        /// `UnionStep` for one looks at whether a child's header columns are constant.
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

            SharedHeader output_header = outline_node.header
                ? outline_node.header
                : std::make_shared<const Block>();

            QueryPlanSerializationSettings settings;
            settings.applyEntries(outline_node.settings);

            /// The payload size is read inline, right before the payload. Reading it at a frame
            /// boundary keeps the stream aligned for the drain if a later step fails.
            UInt64 payload_size = 0;
            readVarUInt(payload_size, in);

            /// The payload is read straight from the stream, bounded to its own frame: the step
            /// cannot read past it, and a codec sizes its allocations by the bytes the frame still
            /// holds. Whatever happens, the scope guard steps over the frame and counts it, so a
            /// failure still leaves the stream at a frame boundary for the drain above.
            {
                LimitReadBuffer payload(in, {.read_no_more = payload_size});
                SCOPE_EXIT({
                    try { payload.ignoreAll(); } catch (...) {} // Ok: best-effort step over the frame; a stream ending here is taken by the drain below // NOLINT(bugprone-empty-catch)
                    ++frames_consumed;
                });

                /// The plan as a whole must fit the size limit. A payload that pushes it past the
                /// limit is not built; the guard takes it off the stream and the drain takes the
                /// rest, so an over-limit plan is refused without losing the connection.
                if (payload_size > budget_left())
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Query plan payload of step '{}' pushes the plan past `max_serialized_query_plan_size`",
                        outline_node.step_name);

                IQueryPlanStep::Deserialization ctx{
                    payload, sets_registry, {}, context, input_headers, output_header, settings,
                    max_type_complexity, flags.version, flags.skip_data, outline_node.step_format_version};
                auto step = step_registry.createStep(outline_node.step_name, ctx);

                if (step->hasOutputHeader())
                {
                    /// Headers that encode to the same bytes cannot differ in anything that came off
                    /// the wire; the encoding leaves out the aggregate state variant.
                    if (!isCompatibleHeader(*step->getOutputHeader(), *output_header)
                        && !haveSameSerializedHeader(*step->getOutputHeader(), *output_header))
                        assertCompatibleHeader(
                            *step->getOutputHeader(), *output_header,
                            fmt::format("deserialization of query plan {} step", outline_node.step_name));
                }
                else if (output_header->columns())
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Deserialized step {} has no output stream, but deserialized header is not empty : {}",
                        outline_node.step_name, output_header->dumpStructure());

                /// The step must consume its whole frame. Each step name owns one payload layout, so
                /// leftover bytes mean a corrupt stream or a writer bug; accepting them would let a
                /// malformed plan run.
                const size_t leftover = payload.bytesUntilLimit();
                if (leftover != 0)
                    throw Exception(ErrorCodes::INCORRECT_DATA,
                        "Step {} left {} of its {} payload bytes unread",
                        outline_node.step_name, leftover, payload_size);

                auto & node = plan.nodes.emplace_back(std::move(step), std::move(children));
                nodes_by_index[idx] = &node;

                for (const auto & storage : ctx.storage_holders)
                    plan.addStorageHolder(storage);
            }
        }

        /// Children-first order puts the root last.
        plan.root = nodes_by_index[node_count - 1];

        return deserializeFramedSets(
            std::move(plan), sets_registry, outline, in, flags, context, max_type_complexity,
            max_plan_bytes, body_start, frames_consumed);
    }
    catch (...)
    {
        try
        {
            skipSizedFrames(in, frames_total - frames_consumed);
        }
        catch (...) // Ok: the drain is best-effort; the original error re-thrown below is what matters // NOLINT(bugprone-empty-catch)
        {
        }
        throw;
    }
}

void QueryPlan::ensureSerialized(size_t max_supported_version, UInt64 requested_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version, requested_version);

    std::lock_guard lock(serialized_plans.mutex);
    if (serialized_plans.chunks)
    {
        if (serialized_plans.version == version)
            return;  // Already serialized at this version

        /// The plan is kept at one version and every peer is sent those bytes. A peer that needs a
        /// different version is refused rather than served a second serialization. The writer
        /// pre-serializes at its own version first, so the kept version is the writer's and only a
        /// peer too old for the framed format lands here.
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "The query plan is serialized at version {} and cannot also be written at version {}; "
            "a peer that cannot read the version this server writes is refused",
            serialized_plans.version, version);
    }

    /// The bytes are published only once complete, so a concurrent sender either does not see them
    /// and waits here, or gets the whole plan. Nothing is kept if serializing throws.
    SerializationFlags flags;
    flags.version = version;

    SerializedChunks chunks;
    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
    {
        chunks = serializeFramedToChunks(flags);
    }
    else
    {
        /// The older layout is written in one pass, so it is kept as a single chunk.
        WriteBufferFromOwnString buffer;
        serializeWithFlags(buffer, flags);
        buffer.finalize();
        chunks.push_back(std::move(buffer.str()));
    }

    serialized_plans.version = version;
    serialized_plans.chunks = std::make_shared<const SerializedChunks>(std::move(chunks));
}

void QueryPlan::writeSerializedTo(WriteBuffer & out, size_t max_supported_version, UInt64 requested_version) const
{
    UInt64 version = effectiveSerializationVersion(max_supported_version, requested_version);

    std::shared_ptr<const SerializedChunks> chunks;
    {
        std::lock_guard lock(serialized_plans.mutex);
        if (!serialized_plans.chunks)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Query plan is not serialized. Call ensureSerialized() first.");
        if (serialized_plans.version != version)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The query plan is serialized at version {} but this peer needs version {}; "
                "a peer that cannot read the version this server writes is refused",
                serialized_plans.version, version);
        chunks = serialized_plans.chunks;
    }

    /// Written outside the lock: the entry is immutable once published, and a slow peer must not
    /// hold up the other senders.
    for (const auto & chunk : *chunks)
        out.write(chunk.data(), chunk.size());
}

QueryPlanAndSets QueryPlan::deserialize(ReadBuffer & in, const ContextPtr & context, size_t max_type_complexity, bool skip_data)
{
    UInt64 version = 0;
    readVarUInt(version, in);

    SerializationFlags flags{.version = version, .skip_data = skip_data};

    if (version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE)
    {
        /// The head is the same two fields in every body layout, so a reader always finds the body
        /// even when the rest of the stream means nothing to it.
        UInt64 format_kind = 0;
        readVarUInt(format_kind, in);

        const UInt64 max_plan_bytes = context->getServerSettings()[ServerSetting::max_serialized_query_plan_size];

        /// An unknown body layout cannot be walked, so its extent is unknown and it cannot be taken
        /// off the stream: the connection has to close.
        if (format_kind != DBMS_QUERY_PLAN_FORMAT_KIND_OUTLINE)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "The query plan uses body format {} which this server does not know", format_kind);

        /// The plan is only being drained off the connection: no steps are built and no set data
        /// is decoded, so a plan this server could never build is still taken off the stream.
        if (flags.skip_data)
        {
            drainOutlineBody(in, max_plan_bytes);
            return {};
        }

        /// The version is a coarse gate: a version above the one this server supports is refused. The
        /// body of a known kind is still walkable, so it is drained first and the connection is kept;
        /// a drain that throws means the stream ended mid-plan, and the refusal is the error to report.
        const UInt64 supported_version = QueryPlanStepRegistry::instance().supportedVersion();
        if (version > supported_version)
        {
            try
            {
                drainOutlineBody(in, max_plan_bytes);
            }
            catch (...) // Ok: the drain is best-effort; the refusal thrown below is the reported error // NOLINT(bugprone-empty-catch)
            {
            }
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "Query plan serialization version {} is not supported. The last supported version is {}",
                version, supported_version);
        }

        /// `deserializeFramedBody` takes its own unread frames off the stream before it throws, so
        /// whatever it turns the plan down for, the connection is left at the next packet.
        return deserializeFramedBody(in, context, flags, max_type_complexity, max_plan_bytes);
    }

    if (version > QueryPlanStepRegistry::instance().supportedVersion())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Query plan serialization version {} is not supported. The last supported version is {}",
            version, QueryPlanStepRegistry::instance().supportedVersion());

    /// A legacy stream declares no size, so `max_serialized_query_plan_size` does not apply here:
    /// the reader consumes the plan field by field as it arrives, with per-field caps, rather than
    /// buffering it whole. Only a peer below the framed format sends one.
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
    if (flags.version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXECUTION_LIMITS)
    {
        readVarUInt(plan.max_threads, in);
        readBinary(plan.concurrency_control, in);
    }

    stack.push(Frame{.to_fill = plan.root});

    while (!stack.empty())
    {
        auto & frame = stack.top();
        if (frame.next_child == 0)
        {
            UInt64 num_children = 0;
            readVarUInt(num_children, in);
            /// Without a cap the count alone would size the vector, so a few bytes could ask for an
            /// arbitrary allocation. The legacy stream declares no size to measure against.
            if (num_children > MAX_LEGACY_PLAN_CHILDREN)
                throw Exception(ErrorCodes::INCORRECT_DATA,
                    "Serialized query plan node declares {} children which exceeds the limit of {}",
                    num_children, MAX_LEGACY_PLAN_CHILDREN);
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
            /// Headers encoding to the same bytes are indistinguishable to this serializer, so their
            /// difference cannot have come off the wire. The encoding omits the aggregate state variant.
            if (!isCompatibleHeader(*step->getOutputHeader(), *output_header)
                && !haveSameSerializedHeader(*step->getOutputHeader(), *output_header))
            {
                assertCompatibleHeader(
                    *step->getOutputHeader(), *output_header, fmt::format("deserialization of query plan {} step", step_name));
            }
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
