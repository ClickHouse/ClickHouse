#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasRequestBudget.h>

#include <limits>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace DB::Cas
{

uint64_t CasRequestBudget::attemptEnvelopeMs() const
{
    /// The TCP connect and the TLS handshake each get one connect interval from Poco, so an HTTPS
    /// attempt may spend two caps before any request I/O. Scheme-agnostic on purpose: conservative for
    /// plain HTTP, exact for HTTPS.
    const uint64_t cap = connect_timeout_cap_ms.value_or(0);
    const uint64_t connects = cap > std::numeric_limits<uint64_t>::max() / 2 ? std::numeric_limits<uint64_t>::max() : 2 * cap;
    return attempt_timeout_ms > std::numeric_limits<uint64_t>::max() - connects
        ? std::numeric_limits<uint64_t>::max()
        : attempt_timeout_ms + connects;
}

void validateCasRequestBudget(const CasRequestBudget & budget, uint64_t mount_lease_ttl_ms,
                              uint64_t mount_renew_period_ms, bool background_renewal)
{
    if (budget.attempt_timeout_ms == 0)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: attempt_timeout_ms must be at least 1; a zero would reserve nothing "
            "while the request keeps the storage's own timeout");
    const uint64_t envelope = budget.attemptEnvelopeMs();
    /// Subtractions against the unsigned TTL: the sums could wrap for absurd values and read as small.
    const bool one_envelope_fits = envelope < mount_lease_ttl_ms
        && budget.lease_safety_margin_ms < mount_lease_ttl_ms - envelope;
    if (!one_envelope_fits)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "CAS request budget rejected: the attempt envelope ({} ms = attempt_timeout_ms {} + connect cap {}) "
            "plus lease_safety_margin_ms ({}) must be strictly less than the mount lease TTL ({} ms). "
            "A writable mount refuses to open with this budget.",
            envelope, budget.attempt_timeout_ms, budget.connect_timeout_cap_ms.value_or(0), budget.lease_safety_margin_ms, mount_lease_ttl_ms);
    if (background_renewal)
    {
        /// A renewal is a write: two envelopes (the attempt and its settlement read) after one period.
        /// Saturating doubling first (matching the production horizon checks' own arithmetic), then
        /// subtraction-based comparisons against the TTL -- no truncating division, so this enforces
        /// exactly the inequality the exception message states, not an off-by-one-tighter one.
        const uint64_t two_envelope = envelope > std::numeric_limits<uint64_t>::max() / 2
            ? std::numeric_limits<uint64_t>::max() : 2 * envelope;
        const bool cadence_fits = mount_renew_period_ms < mount_lease_ttl_ms
            && two_envelope < mount_lease_ttl_ms - mount_renew_period_ms
            && budget.lease_safety_margin_ms < mount_lease_ttl_ms - mount_renew_period_ms - two_envelope;
        if (!cadence_fits)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "CAS mount renewal cadence rejected: mount_renew_period_ms ({}) + 2 × attempt envelope ({} ms) + "
                "lease_safety_margin_ms ({}) must be strictly less than the mount lease TTL ({} ms)",
                mount_renew_period_ms, envelope, budget.lease_safety_margin_ms, mount_lease_ttl_ms);
    }
    LOG_INFO(getLogger("CasRequestBudget"),
        "CAS request budget in effect: attempt_timeout_ms={} connect_timeout_cap_ms={} envelope_ms={} lease_safety_margin_ms={} "
        "(mount_lease_ttl_ms={} mount_renew_period_ms={})",
        budget.attempt_timeout_ms, budget.connect_timeout_cap_ms.value_or(0), envelope, budget.lease_safety_margin_ms,
        mount_lease_ttl_ms, mount_renew_period_ms);
}

}
