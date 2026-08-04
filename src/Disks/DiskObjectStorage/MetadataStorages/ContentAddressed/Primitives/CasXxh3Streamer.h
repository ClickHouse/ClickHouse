#pragma once

/// Isolated include wrapper + tiny helper API for the xxHash XXH3-128 hash used by `CasBlobHashingWriteBuffer`.
///
/// Two problems this header contains in one place:
///   1. In the `dbms` target a plain `#include <xxhash.h>` resolves to lz4's bundled copy
///      (`contrib/lz4/lib` is a higher-priority `-I` than the `-isystem contrib/xxHash`), and that copy
///      provides only XXH32/64 — NOT the XXH3 API. So xxHash is referenced by an explicit repo-relative
///      path (from this file's directory up to the repo root) which unambiguously picks the full
///      standalone `contrib/xxHash` that has XXH3.
///   2. `XXH_INLINE_ALL` makes xxHash a header-only static-inline implementation whose vendored C is not
///      clean under the CAS `-Werror -Weverything` flags, and whose inline functions carry an "unused"
///      attribute that trips `-Wused-but-marked-unused` at every call site. `#pragma clang system_header`
///      marks the rest of THIS header (and everything it includes, plus the helper calls below) as a
///      system header, so ALL of those warnings are suppressed here — without disabling warnings for any
///      real `CasBlobHashingWriteBuffer` code, which only ever touches the clean `DB::Cas` helpers defined below.
///
/// The explicit include is a build-selection detail: it does not change the XXH3-128 algorithm or
/// the digest representation exposed to CAS. Keeping the dependency and warning suppression here
/// prevents callers from depending on either vendored implementation directly.
#pragma clang system_header

#include <base/types.h>
#include <cstddef>

#define XXH_INLINE_ALL
#include "../../../../../../contrib/xxHash/xxhash.h"

namespace DB::Cas
{

/// Owns one streaming XXH3-128 state and exposes only the operations needed by `CasBlobHashingWriteBuffer`.
/// The state is allocated by the constructor and released by the destructor; the wrapper is
/// deliberately non-copyable because copying an xxHash state would make ownership and continuation
/// semantics ambiguous. Callers normally check `valid` immediately after construction, then feed
/// the complete byte sequence with `update` before retrieving the digest with `digest`.
///
/// All raw xxHash symbols are confined to this system header, so callers see no xxHash warnings.
class Xxh3Streamer
{
public:
    /// Allocates and resets a fresh state. `valid` reports allocation failure; callers must not call
    /// `update` or `digest` on an invalid wrapper.
    Xxh3Streamer() : state(XXH3_createState()) { XXH3_128bits_reset(state); }

    /// Releases the state owned by this wrapper. It does not perform or publish a digest.
    ~Xxh3Streamer() { XXH3_freeState(state); }

    Xxh3Streamer(const Xxh3Streamer &) = delete;
    Xxh3Streamer & operator=(const Xxh3Streamer &) = delete;

    /// Returns whether the constructor obtained an xxHash state successfully.
    bool valid() const { return state != nullptr; }

    /// Adds the next byte range to the running digest. The range must remain readable for the
    /// duration of the call; it is consumed immediately and is not retained by the wrapper.
    void update(const void * data, size_t len) { XXH3_128bits_update(state, data, len); }

    /// Writes the current 128-bit digest into its low and high 64-bit halves. This does not reset
    /// the state, so it can be used for inspection before the stream is destroyed; callers should
    /// finish all `update` calls before relying on the result.
    void digest(UInt64 & low, UInt64 & high) const
    {
        const XXH128_hash_t d = XXH3_128bits_digest(state);
        low = d.low64;
        high = d.high64;
    }

private:
    XXH3_state_t * state;
};

/// Hashes one byte range with XXH3-128 and writes the digest into its low and high 64-bit halves.
/// The input is consumed during the call and no state is retained. This is the one-shot counterpart
/// to `Xxh3Streamer` and is used as the reference path for the streaming CAS hash.
inline void xxh3_128_oneshot(const void * data, size_t len, UInt64 & low, UInt64 & high)
{
    const XXH128_hash_t d = XXH3_128bits(data, len);
    low = d.low64;
    high = d.high64;
}

}
