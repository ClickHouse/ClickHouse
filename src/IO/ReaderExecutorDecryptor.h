#pragma once

#include "config.h"

#if USE_SSL

#include <IO/ChainedBuffers.h>
#include <IO/FileEncryptionCommon.h>
#include <Common/VectorWithMemoryTracking.h>
#include <base/types.h>

#include <functional>

namespace DB
{

/// Decrypts the payload served by `ReaderExecutor`.
///
/// Holds the immutable per-layer decryption configuration (algorithm, key,
/// init vector) parsed once from the encryption headers, and decrypts
/// ciphertext in place. `decrypt` is reentrant: it builds a fresh stack
/// `FileEncryption::Encryptor` per layer per call, so there is no shared mutable
/// state and it may be called concurrently from several threads. Per-call
/// construction is cheap because the EVP context is allocated per call inside
/// `Encryptor` anyway.
///
/// Copyable: it carries only the parsed configuration, no encryptor state.
class ReaderExecutorDecryptor
{
public:
    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer. Call `parseHeaders` once after all layers.
    void addLayer(String path, KeyFinderFunc key_finder);

    /// No layers configured.
    bool empty() const { return layers.empty(); }

    /// Total size of the encryption headers (one per layer): the logical data
    /// start offset in the physical stream.
    size_t headerBytes() const { return layers.size() * FileEncryption::Header::kSize; }

    /// `parseHeaders` has resolved the per-layer keys and headers.
    bool initialized() const { return is_initialized; }

    /// Parse the already-fetched header bytes (no I/O). For each layer, peel the
    /// layers above it at their keystream offsets, read its `FileEncryption::Header`,
    /// resolve its key via the key finder, and store the header. Sets `initialized`.
    void parseHeaders(const ChainedBuffers & header_bytes);

    /// Decrypt `size` bytes of ciphertext in place at `logical_offset`. Reentrant:
    /// constructs fresh per-layer encryptors from the immutable configuration.
    /// Per-layer keystream offset preserves `logical_offset + (N - 1 - i) * Header::kSize`.
    void decrypt(char * data, size_t size, size_t logical_offset) const;

private:
    struct Layer
    {
        /// Only used as the `path_for_logs` argument to `key_finder` (key-resolution diagnostics).
        String path;
        /// Resolves the header's key fingerprint to the key; cleared once `parseHeaders` resolves `key`.
        KeyFinderFunc key_finder;
        /// Populated by `parseHeaders`.
        String key;
    };

    VectorWithMemoryTracking<Layer> layers;
    VectorWithMemoryTracking<FileEncryption::Header> headers;
    bool is_initialized = false;
};

}

#endif
