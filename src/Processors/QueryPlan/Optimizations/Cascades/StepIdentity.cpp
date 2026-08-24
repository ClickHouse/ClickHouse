#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>

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
#include <Common/SipHash.h>

namespace DB
{

namespace
{

/// Feeds the encoding straight into SipHash. The block size only affects buffering, not the hash.
class SipHashWriteBuffer final : public WriteBuffer
{
public:
    SipHashWriteBuffer()
        : WriteBuffer(buffer, sizeof(buffer))
    {
    }

    ~SipHashWriteBuffer() override { cancel(); }

    UInt128 getHash()
    {
        finalize();
        return hash.get128();
    }

private:
    void nextImpl() override { hash.update(working_buffer.begin(), offset()); }

    SipHash hash;
    char buffer[4096];
};

}

CascadesIdentityExtras::CascadesIdentityExtras(WriteBuffer & out_, SerializedSetsRegistry & registry_)
    : out(out_)
    , registry(registry_)
{
}

void CascadesIdentityExtras::addPayload(UInt64 tag, std::string_view payload)
{
    writeVarUInt(tag, out);
    writeBinary(static_cast<UInt8>(1), out);
    writeVarUInt(payload.size(), out);
    out.write(payload.data(), payload.size());
}

void CascadesIdentityExtras::addAbsent(UInt64 tag)
{
    writeVarUInt(tag, out);
    writeBinary(static_cast<UInt8>(0), out);
}

void CascadesIdentityExtras::addBool(UInt64 tag, bool value)
{
    const char byte = value ? 1 : 0;
    addPayload(tag, std::string_view(&byte, 1));
}

void CascadesIdentityExtras::addVarUInt(UInt64 tag, UInt64 value)
{
    WriteBufferFromOwnString payload;
    writeVarUInt(value, payload);
    addPayload(tag, payload.str());
}

void CascadesIdentityExtras::addString(UInt64 tag, std::string_view value)
{
    addPayload(tag, value);
}

void CascadesIdentityExtras::addStrings(UInt64 tag, const Names & value)
{
    WriteBufferFromOwnString payload;
    writeVarUInt(value.size(), payload);
    for (const auto & name : value)
        writeStringBinary(name, payload);
    addPayload(tag, payload.str());
}

void CascadesIdentityExtras::addSortDescription(UInt64 tag, const SortDescription & value)
{
    WriteBufferFromOwnString payload;
    serializeSortDescription(value, payload);
    addPayload(tag, payload.str());
}

void CascadesIdentityExtras::addDAG(UInt64 tag, const ActionsDAG * dag)
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

void writeCascadesIdentityEncoding(const IQueryPlanStep & step, WriteBuffer & out)
{
    writeStringBinary(step.getSerializationName(), out);

    if (step.hasOutputHeader())
        serializeQueryPlanStepHeader(*step.getOutputHeader(), out);
    else
        serializeQueryPlanStepHeader({}, out);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    settings.writeChangedBinary(out);

    /// `for_cache_key` must stay false on both the context and the registry: the cache-key mode
    /// drops `AggregatingStep::final` and runtime-filter ids, which are identity-relevant here.
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{
        .out = out, .registry = registry, .for_cache_key = false, .version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION};
    step.serialize(ctx);

    CascadesIdentityExtras extras(out, registry);
    step.appendCascadesIdentityExtras(extras);
}

UInt128 computeCascadesIdentityHash(const IQueryPlanStep & step)
{
    SipHashWriteBuffer buffer;
    writeCascadesIdentityEncoding(step, buffer);
    auto hash = buffer.getHash();

    CascadesIdentityMetrics::encoded_steps.fetch_add(1, std::memory_order_relaxed);
    CascadesIdentityMetrics::encoded_bytes.fetch_add(buffer.count(), std::memory_order_relaxed);
    return hash;
}

bool cascadesIdentityEncodingsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs)
{
    WriteBufferFromOwnString lhs_out;
    writeCascadesIdentityEncoding(lhs, lhs_out);

    WriteBufferFromOwnString rhs_out;
    writeCascadesIdentityEncoding(rhs, rhs_out);

    const auto lhs_bytes = std::string_view(lhs_out.str());
    const auto rhs_bytes = std::string_view(rhs_out.str());

    CascadesIdentityMetrics::encoded_steps.fetch_add(2, std::memory_order_relaxed);
    CascadesIdentityMetrics::encoded_bytes.fetch_add(lhs_bytes.size() + rhs_bytes.size(), std::memory_order_relaxed);
    CascadesIdentityMetrics::exact_reencodes.fetch_add(2, std::memory_order_relaxed);

    return lhs_bytes == rhs_bytes;
}

std::atomic<UInt64> CascadesIdentityMetrics::encoded_steps = 0;
std::atomic<UInt64> CascadesIdentityMetrics::encoded_bytes = 0;
std::atomic<UInt64> CascadesIdentityMetrics::exact_reencodes = 0;

void CascadesIdentityMetrics::reset()
{
    encoded_steps.store(0, std::memory_order_relaxed);
    encoded_bytes.store(0, std::memory_order_relaxed);
    exact_reencodes.store(0, std::memory_order_relaxed);
}

}
