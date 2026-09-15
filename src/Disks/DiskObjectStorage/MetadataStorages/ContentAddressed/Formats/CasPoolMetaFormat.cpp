#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasEnvelopeLimits.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <array>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
}
}

namespace DB::Cas
{

namespace PoolMetaWire
{
    constexpr WireKey pool_id{"pool_id"};
    constexpr WireKey blob_header_len{"blob_header_len"};
    constexpr WireKey gc_shards{"gc_shards"};
    constexpr WireKey min_reader_generation{"min_reader_generation"};
    constexpr WireKey algos_used{"algos_used"};
}

/// Minimum `blob_header_len` that provably fits the `cas_blob` JSON envelope's mandatory-descriptor
/// worst case. The byte-for-byte derivation (`kMandatoryDescriptorWorstCase`, currently 239 bytes) lives
/// beside the envelope key constants in `CasBlobEnvelopeFormat.cpp`, next to the compile-time proof that
/// it fits under this floor; below that bound, `encodeEnvelopeHeader` throws `LOGICAL_ERROR` on the
/// FIRST blob write (the old drop-and-retry that used to mask this is gone). We floor at 240 (a
/// multiple of 8 comfortably above the worst case, leaving at least one byte for the diagnostic `ref`
/// even at type maxima, and well under the 256 default) so a misconfigured pool fails at CREATION with
/// `BAD_ARGUMENTS`, not at first write with `LOGICAL_ERROR`.

void validatePoolBlobHeaderLen(uint64_t blob_header_len, int error_code, std::string_view what)
{
    if (blob_header_len < kMinBlobHeaderLen)
        throw Exception(error_code, "CAS {}: blob_header_len must be >= {} (blob envelope minimum), got {}",
            what, kMinBlobHeaderLen, blob_header_len);
    if (blob_header_len % 8 != 0)
        throw Exception(error_code, "CAS {}: blob_header_len must be a multiple of 8, got {}", what, blob_header_len);
    if (blob_header_len > 16 * 1024)
        throw Exception(error_code, "CAS {}: blob_header_len must be <= 16384, got {}", what, blob_header_len);
}

void validatePoolAlgosUsed(const std::vector<uint8_t> & algos_used, int error_code, std::string_view what)
{
    if (algos_used.empty())
        throw Exception(error_code, "CAS {}: algos_used must be non-empty", what);
    for (size_t i = 0; i < algos_used.size(); ++i)
    {
        /// A direct membership scan, not `blobHashAlgoName`: that throws `LOGICAL_ERROR`, which
        /// aborts at construction under a sanitizer/debug build before any catch can run, but
        /// this function validates a raw byte vector, so it must reject cleanly rather than abort.
        bool known = false;
        for (const auto & entry : kBlobHashAlgoWords.entries)
            if (static_cast<uint8_t>(entry.value) == algos_used[i])
                known = true;
        if (!known)
            throw Exception(error_code, "CAS {}: algos_used contains an unknown algo {}", what, algos_used[i]);
        if (i > 0 && algos_used[i] <= algos_used[i - 1])
            throw Exception(error_code,
                "CAS {}: algos_used must be strictly sorted with no duplicates, got {} at index {} not after {}",
                what, algos_used[i], i, algos_used[i - 1]);
    }
}

String encodePoolMeta(const PoolMeta & pm)
{
    validatePoolAlgosUsed(pm.algos_used, ErrorCodes::CORRUPTED_DATA, "pool meta");

    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::PoolMeta);

    bool first = true;
    writeHex128Field(out, PoolMetaWire::pool_id, pm.pool_id, first);
    writeNumberField(out, PoolMetaWire::blob_header_len, pm.blob_header_len, first);
    writeNumberField(out, PoolMetaWire::gc_shards, pm.gc_shards, first);
    writeNumberField(out, PoolMetaWire::min_reader_generation, pm.min_reader_generation, first);
    /// Sized by the whole algo vocabulary and safe to index by `algos_used`: the validation above
    /// admits only known algo bytes in strictly increasing order, so the vector cannot be longer
    /// than the table. Relaxing that check to non-strict ordering would overrun this array.
    std::array<std::string_view, kBlobHashAlgoWords.entries.size()> algo_words;
    for (size_t i = 0; i < pm.algos_used.size(); ++i)
        algo_words[i] = kBlobHashAlgoWords.toWord(static_cast<BlobHashAlgo>(pm.algos_used[i]), "CAS pool meta");
    writeWordArrayField(out, PoolMetaWire::algos_used, std::span{algo_words}.first(pm.algos_used.size()), first);
    closeObject(out, first);
    writeChar('\n', out);

    return std::move(out).take();
}

PoolMeta decodePoolMeta(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    const TextHeader header = expectHeaderLine(in, FormatId::PoolMeta);

    /// The format-generation baseline is 1; a header below it cannot have been written by any build
    /// this codec understands. `expectHeaderLine` above already rejects the symmetric FUTURE case
    /// (`v > G_BUILD`); reject the backward case here, before the metadata body is read.
    if (header.v < 1)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "CAS pool format {} predates the format-generation baseline; recreate the pool "
            "(CAS is pre-release, so there is no in-place migration)",
            header.v);

    const String body = readLine(in, traitsFor(FormatId::PoolMeta).line_cap, "pool meta");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "pool meta");

    PoolMeta pm;
    bool saw_pid = false;
    bool saw_gc_shards = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == PoolMetaWire::pool_id)
        {
            pm.pool_id = r.readHex128();
            saw_pid = true;
        }
        else if (key == PoolMetaWire::blob_header_len)
            pm.blob_header_len = r.readU64Number();
        else if (key == PoolMetaWire::gc_shards)
        {
            pm.gc_shards = r.readU64Number();
            saw_gc_shards = true;
        }
        else if (key == PoolMetaWire::min_reader_generation)
            pm.min_reader_generation = r.readU64Number();
        else if (key == PoolMetaWire::algos_used)
        {
            for (const String & word : r.readStringArray())
                pm.algos_used.push_back(static_cast<uint8_t>(blobHashAlgoFromWord(word, "pool meta algo")));
        }
        else
            r.skipUnknown(key);
    }
    if (!saw_pid)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: missing pool_id");
    if (!saw_gc_shards || pm.gc_shards == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: missing or zero gc_shards");
    if (!body_in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: junk after body object");
    if (!in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: trailing bytes after body line");

    validatePoolBlobHeaderLen(pm.blob_header_len, ErrorCodes::CORRUPTED_DATA, "pool meta");
    validatePoolAlgosUsed(pm.algos_used, ErrorCodes::CORRUPTED_DATA, "pool meta");

    if (G_BUILD < pm.min_reader_generation)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "CAS pool meta: pool requires reader generation {} but this build supports at most {}",
            pm.min_reader_generation, G_BUILD);

    return pm;
}

}
