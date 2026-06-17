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

/// Thread-safe, isolated decryption helper extracted from `ReaderExecutor`.
///
/// Holds the immutable per-layer decryption configuration (algorithm, key,
/// init vector) parsed once from the encryption headers, and decrypts served
/// ciphertext in place. `decrypt` is reentrant: it builds a fresh stack
/// `FileEncryption::Encryptor` per layer per call, so there is no shared
/// mutable state and a prefetch worker may decrypt concurrently with the
/// foreground. Per-call construction is cheap because the EVP context was
/// already allocated per call inside `Encryptor`.
///
/// Copyable: the transient sub-executor copies the parsed configuration; there
/// is no encryptor state to carry.
class ReaderExecutorDecryptor
{
public:
    using KeyFinderFunc = std::function<String(UInt128 key_fingerprint, const String & path_for_logs)>;

    /// Add a decryption layer. Call `parseHeaders` once after all layers.
    void addLayer(String path, size_t buffer_size, KeyFinderFunc key_finder);

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
        String path;
        size_t buffer_size = 0;
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
