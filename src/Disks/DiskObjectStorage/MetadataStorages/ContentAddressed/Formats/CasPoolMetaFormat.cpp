#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasPoolMetaFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>

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

/// Minimum `blob_header_len` that provably fits the v3 `cas_blob` JSON envelope's mandatory (always-
/// written) non-ref fields, computed at type maxima from `encodeEnvelopeHeader` (CasBlobEnvelopeFormat.cpp):
///   {"type":"cas_blob"                                        18
///   ,"v":<u32>          5 + 10 (currentCompatibilityVersion)  15
///   ,"tag":"<32 hex>"   7 + 34                                41
///   ,"bld":"<32 hex>"   7 + 34                                41
///   ,"ts":<u64>         6 + 20 (created_at_ms)                26
///   ,"by":"<32 hex>"    7 + 34                                41
///   ,"op":"<word>"      6 + 10 (longest op word "mutation")   16
///   ,"ch":<u32>         6 + 10 (VERSION_INTEGER)              16
///                                            non-ref JSON  = 214 bytes
/// The encoder then always frames the ref: `,"ref":` (7) + `""` (2) + `}` (1), and reserves byte
/// blob_header_len-1 for '\n' (1) = 11 bytes. So the mandatory content needs 214 + 11 = 225 bytes;
/// below that, encodeEnvelopeHeader throws LOGICAL_ERROR on the FIRST blob write (the old drop-and-retry
/// that used to mask this is gone). We floor at 240 (a multiple of 8 comfortably above 225, leaving
/// >= 15 bytes for the diagnostic ref even at type maxima, and well under the 256 default) so a
/// misconfigured pool fails at CREATION with BAD_ARGUMENTS, not at first write with LOGICAL_ERROR.
static constexpr uint64_t kMinBlobHeaderLen = 240;

void validatePoolBlobHeaderLen(uint64_t blob_header_len, int error_code, std::string_view what)
{
    if (blob_header_len < kMinBlobHeaderLen)
        throw Exception(error_code, "CAS {}: blob_header_len must be >= {} (v3 envelope minimum), got {}",
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
        try
        {
            blobHashAlgoName(static_cast<BlobHashAlgo>(algos_used[i]));
        }
        catch (const Exception &)
        {
            throw Exception(error_code, "CAS {}: algos_used contains an unknown algo {}", what, algos_used[i]);
        }
        if (i > 0 && algos_used[i] <= algos_used[i - 1])
            throw Exception(error_code,
                "CAS {}: algos_used must be strictly sorted with no duplicates, got {} at index {} not after {}",
                what, algos_used[i], i, algos_used[i - 1]);
    }
}

String encodePoolMeta(const PoolMeta & pm)
{
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::PoolMeta);

    bool first = true;
    writeKey(out, "pid", first);
    writeHex128Value(out, pm.pool_id);
    writeKey(out, "hln", first);
    writeIntText(pm.blob_header_len, out);
    writeKey(out, "gcs", first);
    writeIntText(pm.gc_shards, out);
    writeKey(out, "mrg", first);
    writeIntText(pm.min_reader_generation, out);
    writeKey(out, "alg", first);
    {
        /// Comma-joined algo words (tiny list, <=3): "ch128" or "ch128,sha256".
        String joined;
        for (size_t i = 0; i < pm.algos_used.size(); ++i)
        {
            if (i != 0)
                joined += ',';
            joined += blobHashAlgoName(static_cast<BlobHashAlgo>(pm.algos_used[i]));
        }
        writeStringValue(out, joined);
    }
    closeObject(out, first);
    writeChar('\n', out);

    return std::move(out).take();
}

PoolMeta decodePoolMeta(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    const TextHeader header = expectHeaderLine(in, FormatId::PoolMeta);

    /// An older pool predates a breaking ref-layer change this build cannot reconcile, so
    /// reject it before reading the metadata body. Writers always emit the current generation, while
    /// `expectHeaderLine` separately rejects a future generation that this build cannot understand.
    /// Generation 9 is the latest recreate-only authority floor and subsumes the earlier stream floors.
    if (header.v < kCommittedRefFrontierGeneration)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
            "CAS pool format {} predates generation-9 exact _ckpt committed_through recovery frontier; recreate the pool. "
            "This build requires the namespace-admission shard bound and exact recovery frontier "
            "in the generation-9 format "
            "(generation {}+), and CAS is pre-release: there is no in-place migration.",
            header.v, kCommittedRefFrontierGeneration);

    const String body = readLine(in, traitsFor(FormatId::PoolMeta).line_cap, "pool meta");
    ReadBufferFromMemory body_in(body.data(), body.size());
    JsonObjectReader r(body_in, KeyStrictness::Tolerant, "pool meta");

    PoolMeta pm;
    bool saw_pid = false;
    bool saw_gc_shards = false;
    String key;
    while (r.nextKey(key))
    {
        if (key == "pid")
        {
            pm.pool_id = r.readHex128();
            saw_pid = true;
        }
        else if (key == "hln")
            pm.blob_header_len = r.readU64Number();
        else if (key == "gcs")
        {
            pm.gc_shards = r.readU64Number();
            saw_gc_shards = true;
        }
        else if (key == "mrg")
            pm.min_reader_generation = r.readU64Number();
        else if (key == "alg")
        {
            const String joined = r.readString();
            size_t start = 0;
            while (start <= joined.size())
            {
                const size_t comma = joined.find(',', start);
                const String word = joined.substr(start, comma == String::npos ? String::npos : comma - start);
                if (word.empty())
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: empty algo word in '{}'", joined);
                pm.algos_used.push_back(static_cast<uint8_t>(blobHashAlgoFromWord(word, "pool meta algo")));
                if (comma == String::npos)
                    break;
                start = comma + 1;
            }
        }
        else
            r.skipUnknown(key);
    }
    if (!saw_pid)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: missing pid");
    if (!saw_gc_shards || pm.gc_shards == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS pool meta: missing or zero gcs");
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
