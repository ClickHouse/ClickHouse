#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/StepIdentity.h>
#include <Common/SipHash.h>

namespace DB
{

namespace
{

/// Feeds the encoding straight into SipHash. The block size only affects buffering, not the hash.
class SipHashWriteBuffer final : public WriteBuffer
{
public:
    /// NOLINTNEXTLINE(cppcoreguidelines-pro-type-member-init,hicpp-member-init) - `buffer` is scratch space, hashed only up to what was written
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
