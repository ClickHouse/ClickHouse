#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefCkptFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

void checkRefCkptInvariants(const RefCkpt & ckpt, std::string_view what)
{
    /// PRESENT means REAL. `life_epoch` may be absent (no writer of this object knew the namespace's
    /// genesis epoch), but a present one is a `writer_epoch`, and `RefTxnId` forbids a zero epoch.
    /// Accepting zero would give the field two meanings -- "unknown" and "epoch zero" -- on an object
    /// that gates destructive cleanup.
    if (ckpt.life_epoch && *ckpt.life_epoch == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: a present life_epoch must be nonzero", what);

    /// A PRESENT id must be a real one. Both components nonzero is `RefTxnId`'s own validity rule
    /// (`renderRefTxnId` refuses to build a key from anything else), so a half-zero id here would name
    /// an object that cannot exist.
    const auto check_id = [&](const std::optional<RefTxnId> & id, std::string_view field)
    {
        if (id && (id->writer_epoch == 0 || id->ref_sequence == 0))
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS {}: {} fields must both be nonzero, got {}-{}",
                what, field, id->writer_epoch, id->ref_sequence);
    };
    check_id(ckpt.checkpoint_snapshot_id, "checkpoint_snapshot_id");
    check_id(ckpt.last_epoch_seal, "last_epoch_seal");
    check_id(ckpt.committed_through, "committed_through");
    if (!ckpt.committed_through && (ckpt.checkpoint_snapshot_id || ckpt.last_epoch_seal))
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS {}: checkpoint_snapshot_id and last_epoch_seal require committed_through", what);
    if (ckpt.committed_through)
    {
        if (ckpt.life_epoch && ckpt.committed_through->writer_epoch < *ckpt.life_epoch)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS {}: committed_through must not precede life_epoch", what);
        if (ckpt.checkpoint_snapshot_id && *ckpt.committed_through < *ckpt.checkpoint_snapshot_id)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS {}: checkpoint_snapshot_id must not exceed committed_through", what);
        if (ckpt.last_epoch_seal && *ckpt.committed_through < *ckpt.last_epoch_seal)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS {}: last_epoch_seal must not exceed committed_through", what);

        /// The checkpoint carries the same finite proof as the ref-log chain. A current epoch is either
        /// closed at the frontier itself, or its frontier follows the seal of exactly the preceding
        /// numeric epoch. A seal from the same epoch below the frontier would claim that an epoch kept
        /// accepting transactions after it was closed; a larger gap would let a missing epoch masquerade
        /// as a proved boundary.
        if (ckpt.last_epoch_seal)
        {
            if (*ckpt.last_epoch_seal != *ckpt.committed_through
                && ckpt.last_epoch_seal->writer_epoch + 1 != ckpt.committed_through->writer_epoch)
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "CAS {}: last_epoch_seal must equal committed_through or close its immediately preceding writer epoch",
                    what);
        }
        else if (ckpt.life_epoch && ckpt.committed_through->writer_epoch > *ckpt.life_epoch)
        {
            /// With a known genesis epoch, an unsealed later epoch has no chain evidence. Leave the
            /// unknown-genesis contribution representable: another checkpoint writer may still merge
            /// the genesis fact before this partial contribution is encoded as durable authority.
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "CAS {}: committed_through after life_epoch requires last_epoch_seal", what);
        }
    }
}

String encodeRefCkpt(const RefCkpt & ckpt)
{
    checkRefCkptInvariants(ckpt, "cas_ref_ckpt encode");

    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::RefCkpt);
    bool first = true;
    /// Every field is encoded WHEN SET and omitted otherwise, so "nobody knows the genesis epoch",
    /// "no checkpoint yet", and "no epoch has been closed yet" are absences on the wire rather than
    /// sentinel values a reader would have to interpret. The two ids are flat epoch/sequence PAIRS,
    /// written by the one shared `RefTxnId` writer the `_log` and `_snap` formats also use, so the
    /// three ref formats cannot disagree on the encoding.
    if (ckpt.life_epoch)
    {
        writeKey(out, "le", first);
        writeU64StringValue(out, *ckpt.life_epoch);
    }
    if (ckpt.committed_through)
        writeRefTxnIdFields(out, first, "cte", "cts", *ckpt.committed_through);
    if (ckpt.checkpoint_snapshot_id)
        writeRefTxnIdFields(out, first, "cse", "css", *ckpt.checkpoint_snapshot_id);
    if (ckpt.last_epoch_seal)
        writeRefTxnIdFields(out, first, "lse", "lss", *ckpt.last_epoch_seal);
    closeObject(out, first);
    writeChar('\n', out);

    String text = std::move(out).take();
    /// The registry cap is a corruption brake, not a budget: this object has three fields and cannot
    /// approach it. Checking on the WRITE side too means an encoder bug surfaces here rather than as an
    /// object that was accepted on write and is unreadable on decode.
    const uint64_t object_cap = traitsFor(FormatId::RefCkpt).object_cap;
    if (text.size() > object_cap)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "CAS cas_ref_ckpt encode: encoded size {} exceeds the object cap {}", text.size(), object_cap);
    return text;
}

RefCkpt decodeRefCkpt(std::string_view data)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::RefCkpt);
    const String body = readLine(in, traitsFor(FormatId::RefCkpt).line_cap, "cas_ref_ckpt");
    ReadBufferFromMemory body_in(body.data(), body.size());
    /// STRICT: an unknown ordinary key is `CORRUPTED_DATA` and a `!`-prefixed one is
    /// `UNKNOWN_FORMAT_VERSION`. `_ckpt` is a control object whose every field changes what cleanup is
    /// allowed to delete, so a reader that silently ignored a key it did not understand would be
    /// deciding deletions from a body it only partially read. Duplicate keys are rejected by
    /// `JsonObjectReader` itself.
    JsonObjectReader r(body_in, KeyStrictness::Strict, "cas_ref_ckpt");

    RefCkpt ckpt;
    std::optional<uint64_t> cse;
    std::optional<uint64_t> css;
    std::optional<uint64_t> lse;
    std::optional<uint64_t> lss;
    std::optional<uint64_t> cte;
    std::optional<uint64_t> cts;
    String key;
    while (r.nextKey(key))
    {
        if (key == "le") ckpt.life_epoch = r.readU64String();
        else if (key == "cte") cte = r.readU64String();
        else if (key == "cts") cts = r.readU64String();
        else if (key == "cse") cse = r.readU64String();
        else if (key == "css") css = r.readU64String();
        else if (key == "lse") lse = r.readU64String();
        else if (key == "lss") lss = r.readU64String();
        else r.skipUnknown(key);
    }

    /// TRUNCATION IS REJECTED. Half an id pair is the dangerous shape: silently dropping it would turn
    /// a truncated body into a well-formed `_ckpt` with NO checkpoint, which reads as "nothing is
    /// deletable" today and as "recovery has no base" tomorrow -- both of which a reader would trust.
    /// Fail closed instead. (A missing whole field is a legitimate absence, not truncation: every field
    /// of this object is optional, so there is nothing to miss.)
    if (cse || css)
    {
        if (!cse || !css)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_ref_ckpt: checkpoint_snapshot_id needs both cse and css");
        ckpt.checkpoint_snapshot_id = RefTxnId{*cse, *css};
    }
    if (cte || cts)
    {
        if (!cte || !cts)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_ref_ckpt: committed_through needs both cte and cts");
        ckpt.committed_through = RefTxnId{*cte, *cts};
    }
    if (lse || lss)
    {
        if (!lse || !lss)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_ref_ckpt: last_epoch_seal needs both lse and lss");
        ckpt.last_epoch_seal = RefTxnId{*lse, *lss};
    }
    if (!body_in.eof() || !in.eof())
        throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS cas_ref_ckpt: trailing bytes");

    checkRefCkptInvariants(ckpt, "cas_ref_ckpt");
    return ckpt;
}

}
