#pragma once
#include <cstddef>

namespace DB::Cas
{

/// A capability token: holding one proves the holder is `CasRequests`. Not copyable, not
/// constructible outside that one friend, and carries no data besides the engine's own attempt
/// count -- its main job is to gate access at compile time to the backend entry points that must
/// not be called except through the contract.
class TransportAccess
{
    friend class CasRequests;
    explicit TransportAccess(size_t attempt_no_) : attempt_no(attempt_no_) {}
    size_t attempt_no;

public:
    TransportAccess(const TransportAccess &) = delete;
    TransportAccess & operator=(const TransportAccess &) = delete;
    /// The engine's 1-based count of physical attempts of the logical call this request belongs to,
    /// for the transport to number its request with. Only "1 versus more than 1" is relied upon.
    size_t attemptNo() const { return attempt_no; }
};

}
