#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Formats/CasRefWireVocab.h>
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

std::string_view refOwnerKindToWord(RefOwnerKind k)
{
    switch (k)
    {
        case RefOwnerKind::Committed: return "committed";
        case RefOwnerKind::Precommit: return "precommit";
    }
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS ref wire: unknown RefOwnerKind {}", static_cast<int>(k));
}

RefOwnerKind refOwnerKindFromWord(std::string_view w, std::string_view what)
{
    if (w == "committed") return RefOwnerKind::Committed;
    if (w == "precommit") return RefOwnerKind::Precommit;
    throw Exception(ErrorCodes::CORRUPTED_DATA, "CAS {}: unknown owner kind '{}'", what, w);
}

void checkRefTxnIdNonzero(const RefTxnId & id, std::string_view format, std::string_view field)
{
    if (id.writer_epoch == 0 || id.ref_sequence == 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA,
            "{}: {} fields must both be nonzero, got {}-{}", format, field, id.writer_epoch, id.ref_sequence);
}

void writeRefTxnIdFields(CasJsonWriter & out, bool & first, std::string_view epoch_key, std::string_view seq_key, const RefTxnId & id)
{
    writeKey(out, epoch_key, first);
    writeU64StringValue(out, id.writer_epoch);
    writeKey(out, seq_key, first);
    writeU64StringValue(out, id.ref_sequence);
}

}
