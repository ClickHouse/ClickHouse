#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasCodecUtil.h>
#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Primitives/CasEnumWireTableAsserts.h>
#include <Common/Exception.h>

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

constexpr EnumWireTable<RefOwnerKind, 2> kRefOwnerKindWords{{{
    {RefOwnerKind::Committed, "committed"},
    {RefOwnerKind::Precommit, "precommit"},
}}};

static_assert(casEnumTableCoversEnum<kRefOwnerKindWords, RefOwnerKind>());

}

std::string_view refOwnerKindToWord(RefOwnerKind k)
{
    return kRefOwnerKindWords.toWord(k, "CAS ref wire: RefOwnerKind");
}

RefOwnerKind refOwnerKindFromWord(std::string_view w, std::string_view what)
{
    return kRefOwnerKindWords.fromWord(w, what);
}

void checkRefTxnIdNonzero(const RefTxnId & id, std::string_view format, std::string_view field)
{
    if (id.writer_epoch == 0 || id.ref_sequence == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "{}: {} fields must both be nonzero, got {}-{}", format, field, id.writer_epoch, id.ref_sequence);
}

void writeRefTxnIdFields(CasJsonWriter & out, bool & first, WireKey epoch_key, WireKey seq_key, const RefTxnId & id)
{
    writeU64StringField(out, epoch_key, id.writer_epoch, first);
    writeU64StringField(out, seq_key, id.ref_sequence, first);
}

void writeBindingFields(CasJsonWriter & out, bool & first, const BindingWireKeys & keys, const RefOwnerBinding & binding)
{
    checkCanonicalRefName(binding.ref_name, "RefLogTxn", "owner binding ref_name");
    checkManifestRef(binding.manifest_ref, "RefLogTxn", "owner binding manifest_ref");
    writeWordField(out, keys.kind, refOwnerKindToWord(binding.kind), first);
    writeStringField(out, keys.ref, binding.ref_name, first);
    writeManifestRefFields(out, first, keys.manifest, binding.manifest_ref);
}

}
