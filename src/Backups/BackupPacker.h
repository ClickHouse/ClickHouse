#pragma once

#include <base/types.h>
#include <Backups/BackupFileInfo.h>
#include <Common/VectorWithMemoryTracking.h>

#include <functional>
#include <memory>
#include <vector>


namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// Writes one pack: a PackedFilesIO ".packed" object bundling many small backup data files, streamed through
/// PackedArchiveWriter so the whole pack never has to be held in memory.
class BackupPacker
{
public:
    struct MemberSource
    {
        String name; /// Member key inside the pack (the data file's name, unique per member).
        UInt64 size; /// Physical byte length streamed into the pack.
        std::function<std::unique_ptr<ReadBuffer>()> create_read_buffer;
    };

    /// Streams `members` into `out` in order: front index first, then each body; finalizes `out`. Returns the pack's size.
    static UInt64 writePack(std::unique_ptr<WriteBuffer> out, const std::vector<MemberSource> & members, UInt8 version);

    /// Groups packed entries by pack_id (index of the result), one member per unique data file: the smallest-file_name
    /// entry of its (size, checksum) class, the one restore keys on, since duplicates can differ in base_size.
    static VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> selectPackMembers(const BackupFileInfos & file_infos);
};

}
