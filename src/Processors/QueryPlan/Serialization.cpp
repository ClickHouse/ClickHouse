#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>

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
}

static void serializeHeader(const Block & header, WriteBuffer & out)
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

static Block deserializeHeader(ReadBuffer & in)
{
    UInt64 num_columns;
    readVarUInt(num_columns, in);

    ColumnsWithTypeAndName columns(num_columns);

    for (auto & column : columns)
    {
        readStringBinary(column.name, in);
        column.type = decodeDataType(in);
    }

    /// Fill columns in header. Some steps expect them to be not empty.
    for (auto & column : columns)
        column.column = column.type->createColumn();

    return Block(std::move(columns));
}

/// Nothing is here for now
struct QueryPlan::SerializationFlags
{
};

void QueryPlan::serialize(WriteBuffer & out, size_t max_supported_version) const
{
    UInt64 version = std::min<UInt64>(max_supported_version, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    writeVarUInt(version, out);

    SerializationFlags flags;
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

        if (typeid_cast<DelayedCreatingSetsStep *>(node->step.get()))
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
            serializeHeader(*node->step->getOutputHeader(), out);
        else
            serializeHeader({}, out);

        QueryPlanSerializationSettings settings;
        node->step->serializeSettings(settings);

        settings.writeChangedBinary(out);

        IQueryPlanStep::Serialization ctx{out, registry};
        node->step->serialize(ctx);
    }

    serializeSets(registry, out, flags);
}

void QueryPlan::ensureSerialized(size_t max_supported_version) const
{
    if (serialized_plan)
        return;  // Already serialized

    serialized_plan = std::make_unique<WriteBufferFromOwnString>();
    serialize(*serialized_plan, max_supported_version);
    serialized_plan->finalize();
}

std::string_view QueryPlan::getSerializedData() const
{
    if (!serialized_plan)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Query plan is not serialized. Call ensureSerialized() first.");

    return serialized_plan->stringView();
}

bool QueryPlan::isSerialized() const
{
    return serialized_plan != nullptr;
}

QueryPlanAndSets QueryPlan::deserialize(ReadBuffer & in, const ContextPtr & context)
{
    UInt64 version;
    readVarUInt(version, in);

    if (version > DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "Query plan serialization version {} is not supported. The last supported version is {}",
            version, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    SerializationFlags flags;
    return deserialize(in, context, flags);
}

QueryPlanAndSets QueryPlan::deserialize(ReadBuffer & in, const ContextPtr & context, const SerializationFlags & flags)
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
            UInt64 num_children;
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
        readStringBinary(step_name, in);
        readStringBinary(step_description, in);

        auto output_header  = std::make_shared<const Block>(deserializeHeader(in));

        QueryPlanSerializationSettings settings;
        settings.readBinary(in);

        SharedHeaders input_headers;
        input_headers.reserve(frame.children.size());
        for (const auto & child : frame.children)
            input_headers.push_back(child->step->getOutputHeader());

        IQueryPlanStep::Deserialization ctx{in, sets_registry, context, input_headers, output_header, settings};
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

        stack.pop();
    }

    return deserializeSets(std::move(plan), sets_registry, in, flags, context);
}

}
