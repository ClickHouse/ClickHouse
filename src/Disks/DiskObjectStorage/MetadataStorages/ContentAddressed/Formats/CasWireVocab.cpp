#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>
#include <base/hex.h>
#include <algorithm>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

static_assert(casEnumTableCoversEnum<kTokenTypeWords, Dialect>());
static_assert(casEnumTableCoversEnum<kObjectKindWords, ObjectKind>());

std::string_view dialectWordFromString(std::string_view w, std::string_view what)
{
    /// Parse then re-render, so what a record carries is the table's own spelling rather than the
    /// caller's copy of it.
    return kTokenTypeWords.toWord(kTokenTypeWords.fromWord(w, what), what);
}

uint8_t dialectByteFromWord(std::string_view w, std::string_view what)
{
    return static_cast<uint8_t>(kTokenTypeWords.fromWord(w, what));
}

std::string_view dialectWordFromByte(uint8_t byte, std::string_view what)
{
    for (const auto & entry : kTokenTypeWords.entries)
        if (static_cast<uint8_t>(entry.value) == byte)
            return entry.word;
    /// The byte comes off persisted media, so an unknown one is malformed data, not a caller bug.
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: unknown dialect byte {}", what, byte);
}

BlobHashAlgo blobHashAlgoFromWord(std::string_view w, std::string_view what)
{
    return kBlobHashAlgoWords.fromWord(w, what);
}

std::string_view objectKindToWord(ObjectKind k)
{
    return kObjectKindWords.toWord(k, "CAS wire: ObjectKind");
}

ObjectKind objectKindFromWord(std::string_view w, std::string_view what)
{
    return kObjectKindWords.fromWord(w, what);
}

void writeTokenFields(CasJsonWriter & out, bool & first, const PersistedEtag & inc)
{
    writeStringField(out, SharedWire::token_type, dialectWordFromString(inc.dialect, "wire: dialect"), first);
    writeStringField(out, SharedWire::token, inc.value, first);
}

void writeBlobRefFields(CasJsonWriter & out, bool & first, const BlobRef & r)
{
    writeStringField(out, SharedWire::algo, blobHashAlgoName(r.algo), first);
    writeStringField(out, SharedWire::digest, codecFor(r.algo).toHex(r.digest), first);
}

void writeManifestRefFields(CasJsonWriter & out, bool & first, const ManifestRefWireKeys & keys, const ManifestRef & r)
{
    writeU64StringField(out, keys.epoch, r.writer_epoch, first);
    writeU64StringField(out, keys.build, r.build_sequence, first);
    writeNumberField(out, keys.ord, r.manifest_ordinal, first);
}

ManifestRef manifestRefFromFields(uint64_t writer_epoch, uint64_t build_sequence, uint64_t manifest_ordinal,
                                  std::string_view caller, std::string_view what)
{
    /// Check the upper bound before narrowing the caller-supplied value to the in-memory ordinal
    /// type. `checkManifestRef` then applies the shared nonzero and lower-bound checks.
    if (manifest_ordinal > kMaxManifestOrdinal)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS {}: {} manifest_ordinal {} out of range", caller, what, manifest_ordinal);
    ManifestRef r;
    r.writer_epoch = writer_epoch;
    r.build_sequence = build_sequence;
    r.manifest_ordinal = static_cast<uint32_t>(manifest_ordinal);
    checkManifestRef(r, caller, what);
    return r;
}

ManifestRef ManifestRefFields::buildRef(std::string_view what, std::string_view context) const
{
    if (!epoch || !build || !ord)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: {} manifest_ref missing epoch/build/ord", what, context);
    return manifestRefFromFields(*epoch, *build, *ord, what, context);
}

BlobRef BlobRefFields::build(std::string_view what) const
{
    if (!algo_word || !digest_hex)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: blob ref missing algo/digest", what);
    const BlobHashAlgo algo = blobHashAlgoFromWord(*algo_word, what);
    /// Validate the digest width before calling `fromHex`. A width mismatch otherwise produces
    /// `BAD_ARGUMENTS` instead of the `CORRUPTED_DATA` required for malformed serialized input,
    /// allowing an invalid record to escape the decoder's fail-closed error contract.
    const uint64_t expected_hex_len = blobHashLenFor(algo) * 2;
    if (digest_hex->size() != expected_hex_len)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS {}: digest hex width {} does not match algo width {}", what, digest_hex->size(), expected_hex_len);
    /// Same fence, same reason as the width check above: a right-width but non-lowercase-hex digest
    /// must also surface as CORRUPTED_DATA rather than `DigestCodec::fromHex`'s BAD_ARGUMENTS. Mirrors
    /// `JsonObjectReader::readHex128`'s lowercase-hex predicate.
    if (std::any_of(digest_hex->begin(), digest_hex->end(), [](char c) { return !isLowercaseHexChar(c); }))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: digest is not lowercase hex, got '{}'", what, *digest_hex);
    return BlobRef{algo, codecFor(algo).fromHex(*digest_hex)};
}

PersistedEtag TokenFields::build(std::string_view what) const
{
    if (!type_word || !value)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: token missing token_type/token", what);
    return PersistedEtag{String(dialectWordFromString(*type_word, what)), *value};
}

}
