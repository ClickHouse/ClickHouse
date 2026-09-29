#include <Processors/QueryPlan/Optimizations/Cascades/StepIdentity.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StepDigestCounters.h>
#include <Processors/QueryPlan/StepIdentity.h>
#include <Common/ProfileEvents.h>
#include <Common/SipHash.h>

namespace ProfileEvents
{
    extern const Event CascadesStepDigests;
    extern const Event CascadesStepDigestBytes;
    extern const Event CascadesStepDigestConfirmations;
}

namespace DB
{

namespace
{

/// Feeds the digest straight into SipHash. The block size only affects buffering, not the hash.
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

using StepDigestWriteFunction = void (*)(const IQueryPlanStep &, WriteBuffer &);

/// The full and the logical pass differ only in which writer they run; the counters and the
/// ProfileEvents deliberately aggregate both, since both cost the optimizer the same way.
UInt128 computeFingerprint(const IQueryPlanStep & step, StepDigestWriteFunction write_digest)
{
    SipHashWriteBuffer buffer;
    write_digest(step, buffer);
    auto hash = buffer.getHash();

    if (auto * counters = CurrentStepDigestCounters::get())
    {
        counters->digests_written += 1;
        counters->digest_bytes_written += buffer.count();
    }
    ProfileEvents::increment(ProfileEvents::CascadesStepDigests);
    ProfileEvents::increment(ProfileEvents::CascadesStepDigestBytes, buffer.count());
    return hash;
}

bool digestsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs, StepDigestWriteFunction write_digest)
{
    WriteBufferFromOwnString lhs_out;
    write_digest(lhs, lhs_out);

    WriteBufferFromOwnString rhs_out;
    write_digest(rhs, rhs_out);

    const auto lhs_bytes = std::string_view(lhs_out.str());
    const auto rhs_bytes = std::string_view(rhs_out.str());

    if (auto * counters = CurrentStepDigestCounters::get())
    {
        counters->digests_written += 2;
        counters->digest_bytes_written += lhs_bytes.size() + rhs_bytes.size();
        counters->digest_confirmations += 2;
    }
    ProfileEvents::increment(ProfileEvents::CascadesStepDigests, 2);
    ProfileEvents::increment(ProfileEvents::CascadesStepDigestBytes, lhs_bytes.size() + rhs_bytes.size());
    ProfileEvents::increment(ProfileEvents::CascadesStepDigestConfirmations, 2);

    return lhs_bytes == rhs_bytes;
}

}

UInt128 computeStepFullFingerprint(const IQueryPlanStep & step)
{
    return computeFingerprint(step, writeStepFullDigest);
}

bool stepFullDigestsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs)
{
    return digestsEqual(lhs, rhs, writeStepFullDigest);
}

UInt128 computeStepLogicalFingerprint(const IQueryPlanStep & step)
{
    return computeFingerprint(step, writeStepLogicalDigest);
}

bool stepLogicalDigestsEqual(const IQueryPlanStep & lhs, const IQueryPlanStep & rhs)
{
    return digestsEqual(lhs, rhs, writeStepLogicalDigest);
}

}
