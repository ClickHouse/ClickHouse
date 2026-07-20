#include <Backups/BackupPacker.h>

#include <IO/Archives/PackedArchiveWriter.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>


namespace DB
{

void BackupPacker::writePack(std::unique_ptr<WriteBuffer> out, const std::vector<MemberSource> & members, UInt8 version)
{
    VectorWithMemoryTracking<PackedArchiveWriter::Member> manifest;
    manifest.reserve(members.size());
    for (const auto & member : members)
        manifest.push_back({member.name, member.size});

    PackedArchiveWriter writer(std::move(out), std::move(manifest), version);
    for (const auto & member : members)
    {
        auto in = member.create_read_buffer();
        writer.writeMember(member.name, *in);
    }
    writer.finalize();
}

std::map<size_t, std::vector<size_t>> BackupPacker::selectPackMembers(const BackupFileInfos & file_infos)
{
    /// Per data_file_index, pick the smallest-file_name entry -- the read representative restore keys on (a
    /// duplicate may carry a different base_size, so storing its suffix would corrupt the class). Compare
    /// names explicitly, not by position: this vector is in getFileInfos() insertion order, while
    /// data_file_index indexes the separately-sorted file_infos_for_all_hosts, so assignPacks's
    /// "data_file_index == i" does not hold here.
    std::map<size_t, size_t> representative_by_data_file;
    for (size_t i = 0; i != file_infos.size(); ++i)
    {
        const auto & info = file_infos[i];
        if (info.pack_id < 0)
            continue;
        auto [it, inserted] = representative_by_data_file.try_emplace(info.data_file_index, i);
        if (!inserted && info.file_name < file_infos[it->second].file_name)
            it->second = i;
    }

    std::map<size_t, std::vector<size_t>> pack_to_member_indices;
    for (const auto & [data_file_index, member_index] : representative_by_data_file)
        pack_to_member_indices[static_cast<size_t>(file_infos[member_index].pack_id)].push_back(member_index);
    return pack_to_member_indices;
}

}
