#include <Processors/QueryPlan/StepIdentity.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>

namespace DB
{

StepDigestWriter::StepDigestWriter(WriteBuffer & out_, SerializedSetsRegistry & registry_)
    : out(out_)
    , registry(registry_)
{
}

void StepDigestWriter::addPayload(UInt64 tag, std::string_view payload)
{
    writeVarUInt(tag, out);
    writeBinary(static_cast<UInt8>(1), out);
    writeVarUInt(payload.size(), out);
    out.write(payload.data(), payload.size());
}

void StepDigestWriter::addAbsent(UInt64 tag)
{
    writeVarUInt(tag, out);
    writeBinary(static_cast<UInt8>(0), out);
}

void StepDigestWriter::addBool(UInt64 tag, bool value)
{
    const char byte = value ? 1 : 0;
    addPayload(tag, std::string_view(&byte, 1));
}

void StepDigestWriter::addVarUInt(UInt64 tag, UInt64 value)
{
    WriteBufferFromOwnString payload;
    writeVarUInt(value, payload);
    addPayload(tag, payload.str());
}

void StepDigestWriter::addString(UInt64 tag, std::string_view value)
{
    addPayload(tag, value);
}

void StepDigestWriter::addStrings(UInt64 tag, const Names & value)
{
    WriteBufferFromOwnString payload;
    writeVarUInt(value.size(), payload);
    for (const auto & name : value)
        writeStringBinary(name, payload);
    addPayload(tag, payload.str());
}

void StepDigestWriter::addSortDescription(UInt64 tag, const SortDescription & value)
{
    WriteBufferFromOwnString payload;
    serializeSortDescription(value, payload);
    addPayload(tag, payload.str());
}

void StepDigestWriter::addWitness(UInt64 tag, const void * ptr)
{
    if (!ptr)
        addAbsent(tag);
    else
        addVarUInt(tag, static_cast<UInt64>(reinterpret_cast<uintptr_t>(ptr)));
}

void StepDigestWriter::addWholeObjectWitness(const void * object)
{
    addWitness(WHOLE_OBJECT_WITNESS_TAG, object);
}

void StepDigestWriter::addStepWireEncoding(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString payload;

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    settings.writeChangedBinary(payload);

    /// `for_cache_key` must stay false on both the context and the registry: the cache-key mode
    /// drops `AggregatingStep::final` and runtime-filter ids, which are identity-relevant here.
    /// The registry is the writer's, so set ids stay in encounter order across the wire bytes and
    /// the extras that follow.
    IQueryPlanStep::Serialization ctx{
        .out = payload, .registry = registry, .for_cache_key = false, .version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION};
    step.serialize(ctx);

    addPayload(STEP_WIRE_ENCODING_TAG, payload.str());
}

void StepDigestWriter::addDAG(UInt64 tag, const ActionsDAG * dag)
{
    if (!dag)
    {
        addAbsent(tag);
        return;
    }

    WriteBufferFromOwnString payload;
    dag->serialize(payload, registry);
    addPayload(tag, payload.str());
}

/// Shared by both digests: the step class and the schema half of the relation it produces.
static void writeStepDigestPreamble(const IQueryPlanStep & step, WriteBuffer & out)
{
    writeStringBinary(step.getSerializationName(), out);

    if (step.hasOutputHeader())
        serializeQueryPlanStepHeader(*step.getOutputHeader(), out);
    else
        serializeQueryPlanStepHeader({}, out);
}

void writeStepFullDigest(const IQueryPlanStep & step, WriteBuffer & out)
{
    writeStepDigestPreamble(step, out);

    /// A fresh registry per digest: set ids are assigned in encounter order, so two independently
    /// built steps holding equal sets encode equally.
    SerializedSetsRegistry registry;
    StepDigestWriter writer(out, registry);
    step.writeFullDigest(writer);
}

void writeStepLogicalDigest(const IQueryPlanStep & step, WriteBuffer & out)
{
    writeStepDigestPreamble(step, out);

    /// A fresh registry per digest, as in the full digest: set ids are assigned in encounter order,
    /// so two independently built steps holding equal sets encode equally. `for_cache_key` stays
    /// false here too, so a runtime-filter id inside a DAG is part of the digest - fail-closed.
    SerializedSetsRegistry registry;
    StepDigestWriter writer(out, registry);
    step.writeLogicalDigest(writer);
}

}
