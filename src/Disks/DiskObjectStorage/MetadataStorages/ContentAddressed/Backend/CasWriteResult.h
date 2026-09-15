#pragma once
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Backend/CasEtag.h>
#include <Common/Exception.h>
#include <base/defines.h>

#include <fmt/format.h>

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <variant>

namespace DB::ErrorCodes
{
    extern const int ABORTED;
}

namespace DB::Cas
{

/// Declared (and defined) in `CasRequests.h`/`.cpp`. Forward-declared narrowly here, rather than
/// including that header, because `CasRequests.h` itself includes this one for `WriteResult` --
/// including it back would be circular.
[[noreturn]] void throwCasWriteRetryLater(const String & why);
[[noreturn]] void throwCasTransientUnavailable(const String & subject, const String & condition);

struct Object { String bytes; Etag etag; };
struct Meta   { uint64_t size; Etag etag; };
enum class Removal : uint8_t { Removed, Gone, Mismatch };

/// What a write attempt observed of the key's current state before giving up, so a caller (or the
/// message built by `orThrow`) can report exactly what was seen instead of just that something failed.
struct NotObserved {};
struct ProvenAbsent {};
using Observation = std::variant<NotObserved, ProvenAbsent, Meta, Object>;

/// A durable write landed: `etag` names the incarnation it created (or, for a retried write
/// resolved by a read, the incarnation already present), `attempts_sent` counts the HTTP attempts this
/// call made, and `resolved_by_read` is true when the commit was proven by a read rather than by the
/// attempt's own response.
struct Committed { Etag etag; uint32_t attempts_sent; bool resolved_by_read; };
/// The write was never attempted or never needed -- e.g. `putIfAbsent` finding the key already
/// present under the caller's intended content. `seen` is whatever the resolve read observed.
struct Declined  { Observation seen; };
/// A competing write won: the key's current state does not match what this call expected.
/// `attempts_sent` counts the HTTP attempts this call made, the same count `Committed` and `GaveUp`
/// carry: an operator's attempt counters sum over ALL the endings of a write, and losing the key is
/// one an operator wants counted rather than dropped. `any_ambiguous` is true when an attempt of the
/// inner write ended without proof of whether it applied before the resolve read settled the race: a
/// caller outside the engine paces such a conflict on the growing schedule, as the engine's own
/// loops do, and a clean lost race on the flat one. `attempts_sent` cannot stand in for it: a
/// throttled first attempt whose resolve read finds the key moved is one attempt, ambiguous.
struct Conflict  { Observation seen; uint32_t attempts_sent = 0; bool any_ambiguous = false; };
/// The store itself refused the request (not a lost precondition) -- `store_error` is a ClickHouse
/// error code and `message` explains it. `attempts_sent` is the same count `Conflict` carries, for
/// the same reason.
struct Refused   { int store_error; String message; uint32_t attempts_sent = 0; };
/// No attempt landed and none can be proven safe to keep making.
struct GaveUp
{
    enum class Why : uint8_t { Deadline, FenceLost, Unresolved };
    enum class Source : uint8_t { Policy, Lease };
    Why why; Source deadline_source; bool sent_any; Observation last_seen;
    /// The HTTP attempts this call made, the same count `Committed` carries. Operator counters -- the
    /// mount renewal's attempt and retry counters among them -- have to count the attempts of a write
    /// that GAVE UP as well as of one that committed, and `sent_any` cannot say how many. It stays
    /// beside this because the readers that only branch on "was anything sent" branch on it by name.
    uint32_t attempts_sent = 0;
};
using WriteResult = std::variant<Committed, Declined, Conflict, Refused, GaveUp>;

namespace detail
{

/// Helper for std::visit with multiple lambdas; no shared one exists in the tree yet.
template <typename... Ts>
struct Overload : Ts...
{
    using Ts::operator()...;
};
template <typename... Ts>
Overload(Ts...) -> Overload<Ts...>;

inline String renderObservation(const Observation & seen)
{
    return std::visit(Overload{
        [](const NotObserved &) -> String { return "nothing observed"; },
        [](const ProvenAbsent &) -> String { return "absent"; },
        [](const Meta &) -> String { return "present (meta)"; },
        [](const Object & o) -> String { return "present (" + o.etag.render() + ")"; }}, seen);
}

}

/// Collapse a `WriteResult` into the incarnation a caller can act on: `nullopt` for a declined write
/// (nothing changed, nothing to report), the committed incarnation otherwise -- or throw, mapping
/// every non-success alternative to the error class its meaning already implies. `what` names the
/// call for the thrown message.
inline std::optional<Etag> orThrow(WriteResult && result, std::string_view what)
{
    using detail::Overload;
    using detail::renderObservation;
    return std::visit(Overload{
        [](Committed & c) -> std::optional<Etag> { return std::move(c.etag); },
        [](Declined &) -> std::optional<Etag> { return std::nullopt; },
        [&](Conflict & c) -> std::optional<Etag>
        {
            throw Exception(ErrorCodes::ABORTED, "{}: conflict, observed {}", what, renderObservation(c.seen));
        },
        [&](Refused & r) -> std::optional<Etag>
        {
            throw Exception(r.store_error, "{}: the store refused the write: {}", what, r.message);
        },
        [&](GaveUp & g) -> std::optional<Etag>
        {
            switch (g.why)
            {
                case GaveUp::Why::FenceLost:
                    throwCasTransientUnavailable(String(what), "mount fence tripped: the durable write is refused because this node no longer holds the mount incarnation it was admitted under");
                case GaveUp::Why::Deadline:
                    throwCasWriteRetryLater(fmt::format("{}: gave up at the {} deadline after {} attempt(s)", what, g.deadline_source == GaveUp::Source::Lease ? "lease" : "policy", g.sent_any ? "one or more" : "zero"));
                case GaveUp::Why::Unresolved:
                    throwCasWriteRetryLater(fmt::format("{}: the write is unresolved (sent, resolve read found {})", what, renderObservation(g.last_seen)));
            }
            UNREACHABLE();
        }}, result);
}

}
