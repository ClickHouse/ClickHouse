#include <Disks/DiskObjectStorage/MetadataStorages/ContentAddressed/Pool/CasKeyReader.h>

#include <limits>

namespace DB::Cas
{

void hintRefLogsWithinEpoch(KeyReader & reader, const Layout & layout, const NamespaceLifeId & life,
                            RefTxnId first, const RefTxnId & committed_through)
{
    while (reader.pending() < reader.window() && first <= committed_through)
    {
        reader.hint(layout.refLogKey(life, first));
        if (first.ref_sequence == std::numeric_limits<uint64_t>::max())
            return;
        ++first.ref_sequence;
    }
}

void discardRefLogHintsOfEpoch(KeyReader & reader, const Layout & layout, const NamespaceLifeId & life,
                               RefTxnId first, const RefTxnId & committed_through)
{
    for (size_t n = 0; n < reader.window() && first <= committed_through; ++n)
    {
        reader.discard(layout.refLogKey(life, first));
        if (first.ref_sequence == std::numeric_limits<uint64_t>::max())
            return;
        ++first.ref_sequence;
    }
}

}
