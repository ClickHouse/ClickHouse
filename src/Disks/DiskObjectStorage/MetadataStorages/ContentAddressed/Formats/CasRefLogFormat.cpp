#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefLogFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasTextFormat.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <algorithm>
#include <optional>

namespace DB
{
namespace ErrorCodes
{
    extern const int CORRUPTED_DATA;
}
}

namespace DB::Cas
{

namespace
{

namespace RefLogWire
{
    constexpr WireKey ns{"namespace"};
    constexpr WireKey txn_epoch{"txn_epoch"};
    constexpr WireKey txn_seq{"txn_seq"};
    constexpr WireKey prev_epoch{"!prev_epoch"};
    constexpr WireKey prev_seq{"!prev_seq"};
    constexpr WireKey op{"op"};
    constexpr WireKey ref{"ref"};
    constexpr WireKey published_ms{"published_ms"};
}

constexpr EnumWireTable<RefOpKind, 5> kRefOpWords{{{
    {RefOpKind::NamespaceBirth, "namespace_birth"},
    {RefOpKind::OwnerTransition, "owner_transition"},
    {RefOpKind::SetPublishedAt, "set_published_at"},
    {RefOpKind::RemoveNamespace, "remove_namespace"},
    {RefOpKind::EpochSeal, "epoch_seal"},
}}};

static_assert(casEnumTableCoversEnum<kRefOpWords, RefOpKind>());

/// Byte budget over the encoded text. A removal-class transaction uses the larger complete-table
/// budget and has neither an op-count nor a per-op cap; normal transactions are bounded by
/// `ref_txn_max_ops` and, per op, by `ref_op_max_bytes` (checked via `encodedOpSize`, one op at a
/// time -- no accumulation).
void checkBudget(const std::vector<RefOp> & ops, size_t encoded_bytes)
{
    const bool removal = refLogTxnIsRemovalClass(ops);
    const size_t byte_limit = removal ? ref_removal_max_bytes : ref_txn_max_bytes;
    if (encoded_bytes > byte_limit)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefLogTxn: encoded size {} exceeds the {}-class byte limit {}",
            encoded_bytes, removal ? "removal" : "normal", byte_limit);
    if (removal)
        return;
    if (ops.size() > ref_txn_max_ops)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefLogTxn: {} operations exceeds the normal-class op-count limit {}", ops.size(), ref_txn_max_ops);
    for (const RefOp & op : ops)
    {
        const size_t op_bytes = encodedOpSize(op);
        if (op_bytes > ref_op_max_bytes)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefLogTxn: op encoded size {} exceeds the normal-class per-op limit {}", op_bytes, ref_op_max_bytes);
    }
}

void writeOp(CasJsonWriter & out, const RefOp & op)
{
    bool first = true;
    writeWordField(out, RefLogWire::op, refOpKindToWireWord(op.kind), first);
    switch (op.kind)
    {
        case RefOpKind::NamespaceBirth:
        case RefOpKind::RemoveNamespace:
        case RefOpKind::EpochSeal:
            break;
        case RefOpKind::OwnerTransition:
            if (op.old_binding)
                writeBindingFields(out, first, kOldBindingKeys, *op.old_binding);
            if (op.new_binding)
                writeBindingFields(out, first, kNewBindingKeys, *op.new_binding);
            break;
        case RefOpKind::SetPublishedAt:
            checkCanonicalRefName(op.ref_name, "RefLogTxn", "set_published_at ref_name");
            checkManifestRef(op.expected_manifest_ref, "RefLogTxn", "set_published_at manifest_ref");
            writeStringField(out, RefLogWire::ref, op.ref_name, first);
            writeManifestRefFields(out, first, kBareManifestRefKeys, op.expected_manifest_ref);
            writeNumberField(out, RefLogWire::published_ms, op.published_at_ms, first);
            break;
    }
    closeObject(out, first);
    writeChar('\n', out);
}

/// Collector for one binding (old/new) under a prefix.
struct BindingFields
{
    std::optional<String> kind;
    std::optional<String> ref;
    ManifestRefFields manifest_fields;

    bool any() const { return kind || ref || manifest_fields.any(); }
    RefOwnerBinding build(std::string_view what) const
    {
        if (!kind || !ref)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: {} binding missing kind/ref", what);
        RefOwnerBinding b;
        b.kind = refOwnerKindFromWord(*kind, "RefLogTxn owner binding");
        b.ref_name = *ref;
        checkCanonicalRefName(b.ref_name, "RefLogTxn", "owner binding ref_name");
        b.manifest_ref = manifest_fields.buildRef("RefLogTxn", what);
        return b;
    }
};

/// The log transaction's header-object meta line (`namespace` + txn_id + the optional `prev_epoch_seal`
/// chain). Shared by `encodeRefLogTxn` and `removalFramingSize` so the two never disagree by a byte;
/// `removalFramingSize` always passes `std::nullopt` -- a removal transaction is never a sequence-1
/// epoch-transition record. Additive: the `"!prev_epoch"`/`"!prev_seq"` pair is emitted only when
/// `prev_epoch_seal` is set, so a body without it is byte-identical to the pre-EpochSeal wire shape.
/// `!`-prefixed: `prev_epoch_seal` is INV-2 chain evidence, not cosmetic metadata -- a decoder that
/// doesn't understand it must refuse the object rather than silently drop the chain link while
/// otherwise passing the structural grammar (`JsonObjectReader::skipUnknown` rejects any unrecognized
/// `!`-key with `UNKNOWN_FORMAT_VERSION`, tolerant or not).
void writeLogMeta(CasJsonWriter & out, const String & ns, const RefTxnId & txn_id, const std::optional<RefTxnId> & prev_epoch_seal)
{
    bool first = true;
    writeStringField(out, RefLogWire::ns, ns, first);
    writeRefTxnIdFields(out, first, RefLogWire::txn_epoch, RefLogWire::txn_seq, txn_id);
    if (prev_epoch_seal)
        writeRefTxnIdFields(out, first, RefLogWire::prev_epoch, RefLogWire::prev_seq, *prev_epoch_seal);
    closeObject(out, first);
    writeChar('\n', out);
}

RefOp readOpRecord(JsonObjectReader & r, RefOpKind kind)
{
    RefOp op;
    op.kind = kind;

    /// set_published_at fields
    std::optional<String> sp_ref;
    ManifestRefFields sp_manifest_fields;
    std::optional<uint64_t> sp_published_ms;
    /// owner_transition bindings
    BindingFields ob;
    BindingFields nb;

    String key;
    while (r.nextKey(key))
    {
        if (key == RefLogWire::ref)
            sp_ref = r.readString();
        else if (matchManifestRefFields(key, r, kBareManifestRefKeys, sp_manifest_fields))
        {
        }
        else if (key == RefLogWire::published_ms)
            sp_published_ms = r.readU64Number();
        else if (key == kOldBindingKeys.kind)
            ob.kind = r.readString();
        else if (key == kOldBindingKeys.ref)
            ob.ref = r.readString();
        else if (matchManifestRefFields(key, r, kOldBindingKeys.manifest, ob.manifest_fields))
        {
        }
        else if (key == kNewBindingKeys.kind)
            nb.kind = r.readString();
        else if (key == kNewBindingKeys.ref)
            nb.ref = r.readString();
        else if (matchManifestRefFields(key, r, kNewBindingKeys.manifest, nb.manifest_fields))
        {
        }
        else if (key == "pl")
            /// `"pl"` (payload) was removed from the op wire when the `set_payload` op became
            /// `set_published_at`. The retired op WORD is already rejected by `refOpKindFromWireWord`, but this
            /// generic reader reads field keys before switching on kind, so a `"pl"` field paired with a
            /// still-recognized op word would otherwise be `skipUnknown`'d. It is a KNOWN-removed field,
            /// not a genuinely-unknown one -- reject it explicitly rather than silently discard it.
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefLogTxn: op record carries the removed \"pl\" (payload) field");
        else r.skipUnknown(key);
    }

    switch (kind)
    {
        case RefOpKind::NamespaceBirth:
        case RefOpKind::RemoveNamespace:
        case RefOpKind::EpochSeal:
            break;
        case RefOpKind::OwnerTransition:
            if (ob.any())
                op.old_binding = ob.build("old");
            if (nb.any())
                op.new_binding = nb.build("new");
            break;
        case RefOpKind::SetPublishedAt:
            if (!sp_ref || !sp_published_ms)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: set_published_at missing ref/published_ms");
            op.ref_name = *sp_ref;
            checkCanonicalRefName(op.ref_name, "RefLogTxn", "set_published_at ref_name");
            op.expected_manifest_ref = sp_manifest_fields.buildRef("RefLogTxn", "set_published_at");
            op.published_at_ms = *sp_published_ms;
            break;
    }
    return op;
}

}

std::string_view refOpKindToWireWord(RefOpKind kind)
{
    return kRefOpWords.toWord(kind, "RefLogTxn");
}

RefOpKind refOpKindFromWireWord(std::string_view w)
{
    return kRefOpWords.fromWord(w, "RefLogTxn");
}

bool refLogTxnIsEpochSeal(const RefLogTxn & txn)
{
    return txn.ops.size() == 1 && txn.ops.front().kind == RefOpKind::EpochSeal;
}

void validateEpochSealGrammarStructural(const RefLogTxn & txn)
{
    const bool has_seal_op = std::any_of(txn.ops.begin(), txn.ops.end(),
        [](const RefOp & op) { return op.kind == RefOpKind::EpochSeal; });
    if (has_seal_op && txn.ops.size() != 1)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefLogTxn: a transaction carrying an EpochSeal op must contain exactly that one op, got {} ops",
            txn.ops.size());

    if (txn.prev_epoch_seal)
    {
        checkRefTxnIdNonzero(*txn.prev_epoch_seal, "RefLogTxn", "prev_epoch_seal");
        if (txn.txn_id.ref_sequence != 1)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefLogTxn: prev_epoch_seal is only allowed at sequence 1, got txn_id {}-{}",
                txn.txn_id.writer_epoch, txn.txn_id.ref_sequence);
        /// INV-2 materializes every global writer epoch for an existing life. A sequence-1 transaction
        /// in E therefore chains to the closing seal of exactly E-1, not merely an arbitrary earlier
        /// epoch. A skip would make a missing intermediate seal look like a proved boundary and let a
        /// fold or destructive tail walk bypass it. This is a context-free property of one body, so
        /// reject it in the codec before any walker can interpret the link as evidence.
        if (txn.prev_epoch_seal->writer_epoch >= txn.txn_id.writer_epoch)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefLogTxn: prev_epoch_seal writer_epoch {} must be strictly less than this "
                "transaction's writer_epoch {}", txn.prev_epoch_seal->writer_epoch, txn.txn_id.writer_epoch);
        if (txn.prev_epoch_seal->writer_epoch + 1 != txn.txn_id.writer_epoch)
            throw Exception(ErrorCodes::CORRUPTED_DATA,
                "RefLogTxn: prev_epoch_seal writer_epoch {} must immediately precede this "
                "transaction's writer_epoch {}", txn.prev_epoch_seal->writer_epoch, txn.txn_id.writer_epoch);
    }
}

void validateEpochSealGrammarContextual(const RefLogTxn & txn, uint64_t life_epoch)
{
    /// The unconditional "forbidden outside sequence 1" half of the rule is
    /// `validateEpochSealGrammarStructural`'s job; this function only owns the required-iff rule,
    /// which is meaningless off sequence 1.
    if (txn.txn_id.ref_sequence != 1)
        return;
    const bool required = txn.txn_id.writer_epoch > life_epoch;
    if (required && !txn.prev_epoch_seal)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefLogTxn: sequence-1 txn at writer_epoch {} (life_epoch {}) must carry prev_epoch_seal",
            txn.txn_id.writer_epoch, life_epoch);
    if (!required && txn.prev_epoch_seal)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefLogTxn: sequence-1 txn at writer_epoch {} (life_epoch {}) must not carry prev_epoch_seal",
            txn.txn_id.writer_epoch, life_epoch);
}

String encodeRefLogTxn(const RefLogTxn & txn)
{
    checkRefTxnIdNonzero(txn.txn_id, "RefLogTxn", "txn_id");
    validateEpochSealGrammarStructural(txn);

    CasJsonWriter out(512);
    writeHeaderLine(out, FormatId::RefLog);

    writeLogMeta(out, txn.ns, txn.txn_id, txn.prev_epoch_seal);

    for (const RefOp & op : txn.ops)
        writeOp(out, op);

    writeTrailerLine(out, txn.ops.size());
    String text = std::move(out).take();
    checkBudget(txn.ops, text.size());
    return text;
}

RefLogTxn decodeRefLogTxn(std::string_view data, const String & expected_ns, const RefTxnId & expected_txn_id)
{
    ReadBufferFromMemory in(data.data(), data.size());
    expectHeaderLine(in, FormatId::RefLog);
    const uint64_t line_cap = traitsFor(FormatId::RefLog).line_cap;

    RefLogTxn txn;

    /// meta line
    {
        const String line = readLine(in, line_cap, "cas_ref_log");
        ReadBufferFromMemory m(line.data(), line.size());
        JsonObjectReader r(m, KeyStrictness::Tolerant, "cas_ref_log");
        bool saw_ns = false;
        bool saw_txn_epoch = false;
        bool saw_txn_seq = false;
        std::optional<uint64_t> prev_epoch;
        std::optional<uint64_t> prev_seq;
        String key;
        while (r.nextKey(key))
        {
            if (key == RefLogWire::ns)
            {
                txn.ns = r.readString();
                saw_ns = true;
            }
            else if (key == RefLogWire::txn_epoch)
            {
                txn.txn_id.writer_epoch = r.readU64String();
                saw_txn_epoch = true;
            }
            else if (key == RefLogWire::txn_seq)
            {
                txn.txn_id.ref_sequence = r.readU64String();
                saw_txn_seq = true;
            }
            else if (key == RefLogWire::prev_epoch)
                prev_epoch = r.readU64String();
            else if (key == RefLogWire::prev_seq)
                prev_seq = r.readU64String();
            else
                r.skipUnknown(key);
        }
        if (!saw_ns || !saw_txn_epoch || !saw_txn_seq)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: meta line missing namespace/txn_epoch/txn_seq");
        /// Both-or-neither: `nextKey` already rejects a repeated "!prev_epoch"/"!prev_seq" (duplicate-key check), so
        /// this only guards against a body carrying exactly one of the pair.
        if (prev_epoch || prev_seq)
        {
            if (!prev_epoch || !prev_seq)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: prev_epoch_seal needs both !prev_epoch and !prev_seq");
            txn.prev_epoch_seal = RefTxnId{*prev_epoch, *prev_seq};
        }
        if (!m.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: junk after meta line");
    }

    checkRefTxnIdNonzero(txn.txn_id, "RefLogTxn", "txn_id");
    /// The namespace and transaction id are duplicated in the body because the object key is the
    /// source of truth for which transaction is being read. Reject a valid body copied under a
    /// different key before accepting any of its operations.
    if (txn.ns != expected_ns || txn.txn_id != expected_txn_id)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "RefLogTxn: body (ns='{}', txn_id={}-{}) does not match the key it was read from "
            "(ns='{}', txn_id={}-{})",
            txn.ns, txn.txn_id.writer_epoch, txn.txn_id.ref_sequence,
            expected_ns, expected_txn_id.writer_epoch, expected_txn_id.ref_sequence);

    /// op record lines, until the trailer
    /// One line scratch and one reader for the whole loop, as the other row decoders do:
    /// rebuilding them per row costs an allocation per row for the seen-key store and the line.
    String row_line;
    JsonObjectReader row_reader;
    while (true)
    {
        readLineInto(in, row_line, line_cap, "cas_ref_log");
        ReadBufferFromMemory l(row_line.data(), row_line.size());
        row_reader.reset(l, KeyStrictness::Tolerant, "cas_ref_log");
        JsonObjectReader & r = row_reader;
        String key;
        if (!r.nextKey(key))
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: empty line");
        if (key == "n")
        {
            const uint64_t n = r.readU64Number();
            while (r.nextKey(key))
                r.skipUnknown(key);
            if (!l.eof() || !in.eof())
                throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: bytes after trailer");
            if (n != txn.ops.size())
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "RefLogTxn: trailer count {} != {} ops", n, txn.ops.size());
            break;
        }
        if (key != RefLogWire::op)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: record must start with \"op\"");
        const RefOpKind kind = refOpKindFromWireWord(r.readString());
        txn.ops.push_back(readOpRecord(r, kind));
        if (!l.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "RefLogTxn: junk after op record");
    }

    /// Finalization: `txn.ops` is now complete, so the context-free seal grammar (which needs the
    /// full op list) can run. Both directions of the codec enforce the identical rule -- see
    /// `encodeRefLogTxn`.
    validateEpochSealGrammarStructural(txn);
    checkBudget(txn.ops, data.size());
    return txn;
}

size_t encodedOpSize(const RefOp & op)
{
    CasJsonWriter out(256);
    writeOp(out, op);
    return out.size();
}

bool refLogTxnIsRemovalClass(const std::vector<RefOp> & ops)
{
    return std::any_of(ops.begin(), ops.end(), [](const RefOp & op) { return op.kind == RefOpKind::RemoveNamespace; });
}

size_t removalOpEncodedSize(RefOwnerKind owner_kind, const String & ref_name, const ManifestRef & manifest_ref)
{
    /// One exact owner-removal op, exactly as it appears in a hypothetical whole-namespace removal
    /// transaction: an owner_transition with only an old binding, no new binding.
    RefOp op;
    op.kind = RefOpKind::OwnerTransition;
    op.old_binding = RefOwnerBinding{owner_kind, ref_name, manifest_ref};

    CasJsonWriter out(256);
    writeOp(out, op);
    return out.size();
}

size_t removalFramingSize(const String & ns, const RefTxnId & txn_id, uint64_t op_count)
{
    /// Header + meta + the terminal remove_namespace op + trailer(op_count). `op_count` counts every op
    /// including the remove_namespace op (i.e. committed + precommits + 1).
    CasJsonWriter out(256);
    writeHeaderLine(out, FormatId::RefLog);
    writeLogMeta(out, ns, txn_id, std::nullopt);
    RefOp remove_op;
    remove_op.kind = RefOpKind::RemoveNamespace;
    writeOp(out, remove_op);
    writeTrailerLine(out, op_count);
    return out.size();
}

std::optional<RefLogMetaPeek> peekRefLogMeta(const String & sealed_bytes)
{
    try
    {
        const String text = openObject(FormatId::RefLog, sealed_bytes);
        ReadBufferFromMemory in(text.data(), text.size());
        const uint64_t line_cap = traitsFor(FormatId::RefLog).line_cap;
        readLine(in, line_cap, "cas_ref_log");   /// header line -- skipped, the version is not judged here
        const String meta = readLine(in, line_cap, "cas_ref_log");
        ReadBufferFromMemory m(meta.data(), meta.size());
        JsonObjectReader r(m, KeyStrictness::Tolerant, "cas_ref_log");
        RefLogMetaPeek peek;
        bool saw_ns = false;
        bool saw_epoch = false;
        bool saw_seq = false;
        String key;
        while (r.nextKey(key))
        {
            if (key == RefLogWire::ns) { peek.ns = r.readString(); saw_ns = true; }
            else if (key == RefLogWire::txn_epoch) { peek.writer_epoch = r.readU64String(); saw_epoch = true; }
            else if (key == RefLogWire::txn_seq) { peek.ref_sequence = r.readU64String(); saw_seq = true; }
            else r.skipUnknown(key);
        }
        if (!saw_ns || !saw_epoch || !saw_seq)
            return std::nullopt;
        return peek;
    }
    catch (...)
    {
        /// Deliberately total: a diagnostic that throws while explaining an anomaly replaces the
        /// anomaly's report with its own. A seal-linked txn reaches here too -- its `!`-prefixed chain
        /// keys make the tolerant reader refuse the line -- and answering `nullopt` is correct: this
        /// peek identifies a writer, it does not certify an object.
        return std::nullopt;
    }
}

}
