#include <IO/ReaderExecutorDecryptor.h>

#if USE_SSL

#include <IO/ReadBufferFromMemory.h>

#include <array>

namespace DB
{

void ReaderExecutorDecryptor::addLayer(String path, KeyFinderFunc key_finder)
{
    layers.push_back(Layer{
        .path = std::move(path),
        .key_finder = std::move(key_finder),
        .key = {},
    });
}

void ReaderExecutorDecryptor::parseHeaders(const ChainedBuffers & header_bytes)
{
    /// Stacked encryption layout: only `h0` is plaintext; every later header
    /// is wrapped by all layers above it (`[h0, enc0(h1), enc0(enc1(h2)), ...]`).
    /// Parsing `hi` peels layers `0..i-1` at their current keystream offsets —
    /// same per-layer stepping as the payload path; see `decrypt`.
    VectorWithMemoryTracking<FileEncryption::Encryptor> initialized_encryptors;
    initialized_encryptors.reserve(layers.size());
    size_t offset = 0;
    for (size_t i = 0; i < layers.size(); ++i)
    {
        auto & layer = layers[i];

        /// Copy the header's 64 bytes into a mutable buffer so we can
        /// decrypt in place across the already-initialized layers.
        std::array<char, FileEncryption::Header::kSize> hdr_bytes{};
        {
            ChainedBuffers slice = header_bytes.slice(ByteRange{offset, FileEncryption::Header::kSize});
            chassert(slice.totalBytes() == FileEncryption::Header::kSize);
            slice.copyTo(hdr_bytes.data(), ByteRange{offset, FileEncryption::Header::kSize});
        }

        for (size_t j = 0; j < initialized_encryptors.size(); ++j)
        {
            const size_t layer_keystream_offset = (i - 1 - j) * FileEncryption::Header::kSize;
            initialized_encryptors[j].setOffset(layer_keystream_offset);
            initialized_encryptors[j].decrypt(hdr_bytes.data(), hdr_bytes.size(), hdr_bytes.data());
        }

        ReadBufferFromMemory rb(hdr_bytes.data(), hdr_bytes.size());
        FileEncryption::Header header;
        header.read(rb);
        layer.key = layer.key_finder(header.key_fingerprint, layer.path);
        /// The key is resolved; release the finder so no key-resolution callback is retained.
        layer.key_finder = {};
        headers.push_back(std::move(header));

        initialized_encryptors.emplace_back(
            headers.back().algorithm,
            layer.key,
            headers.back().init_vector);

        offset += FileEncryption::Header::kSize;
    }

    is_initialized = true;
}

void ReaderExecutorDecryptor::decrypt(char * data, size_t size, size_t logical_offset) const
{
    /// Per-layer keystream offset: with `N` layers (0 = outermost, N-1 =
    /// innermost), layer `i`'s stream carries the inner layers' headers ahead of
    /// the payload, so its CTR offset for `logical_offset` is
    /// `logical_offset + (N - 1 - i) * Header::kSize`; the innermost uses
    /// `logical_offset`. See `ReadBufferFromEncryptedFile::nextImpl`.
    for (size_t i = 0; i < layers.size(); ++i)
    {
        FileEncryption::Encryptor encryptor(headers[i].algorithm, layers[i].key, headers[i].init_vector);
        const size_t layer_keystream_offset = logical_offset
            + (layers.size() - 1 - i) * FileEncryption::Header::kSize;
        encryptor.setOffset(layer_keystream_offset);
        encryptor.decrypt(data, size, data);
    }
}

}

#endif
