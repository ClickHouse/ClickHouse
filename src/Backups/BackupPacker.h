#pragma once

#include <base/types.h>
#include <Backups/BackupFileInfo.h>

#include <functional>
#include <map>
#include <memory>
#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Writes one pack: a PackedFilesIO ".packed" blob bundling many small backup blobs. Drives a streaming
/// PackedArchiveWriter so the whole pack never has to be held in memory. Standalone and unit-testable --
/// it needs only an opened destination buffer and, per member, a name, byte size, and ReadBuffer factory.
class BackupPacker
{
public:
    struct MemberSource
    {
        String name; /// Member key inside the pack (the blob's data_file id, unique per member).
        UInt64 size; /// Physical byte length streamed into the pack.
        std::function<std::unique_ptr<ReadBuffer>()> create_read_buffer;
    };

    /// Streams `members` into `out` in order: the whole front index first (offsets derived from sizes),
    /// then each body. `out` is finalized on success. `version` is a PackedFilesIO format version.
    static void writePack(std::unique_ptr<WriteBuffer> out, const std::vector<MemberSource> & members, UInt8 version);

    /// Groups packed entries (pack_id >= 0) by pack_id, keeping one member per unique blob (data_file_index):
    /// the smallest-file_name entry of the (size, checksum) class, which is what restore keys on. Duplicates of
    /// one blob can carry different base_size, so only that entry's [base_size, size) suffix is the right one.
    static std::map<size_t, std::vector<size_t>> selectPackMembers(const BackupFileInfos & file_infos);
};

}
