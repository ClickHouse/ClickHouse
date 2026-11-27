#include <Backups/BackupPacker.h>

#include <IO/Archives/PackedArchiveWriter.h>
#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>
#include <Common/MapWithMemoryTracking.h>


namespace DB
{

UInt64 BackupPacker::writePack(std::unique_ptr<WriteBuffer> out, const std::vector<MemberSource> & members, UInt8 version)
{
    const WriteBuffer & written = *out;
    VectorWithMemoryTracking<PackedArchiveWriter::Member> manifest;
    /// out is not owned by a writer yet, so cancel it here or an abandoned buffer may publish an empty pack.
    try
    {
        manifest.reserve(members.size());
        for (const auto & member : members)
            manifest.push_back({member.name, member.size});
    }
    catch (...)
    {
        out->cancel();
        throw;
    }

    PackedArchiveWriter writer(std::move(out), std::move(manifest), version);
    for (const auto & member : members)
    {
        auto in = member.create_read_buffer();
        writer.writeMember(member.name, *in);
    }
    writer.finalize();
    return written.count();
}

VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> BackupPacker::selectPackMembers(const BackupFileInfos & file_infos)
{
    /// file_infos is in getFileInfos() insertion order; data_file_index indexes the separately sorted
    /// file_infos_for_all_hosts, so match representatives by file_name.
    MapWithMemoryTracking<size_t, size_t> representative_by_data_file;
    for (size_t i = 0; i != file_infos.size(); ++i)
    {
        const auto & info = file_infos[i];
        if (info.pack_id < 0)
            continue;
        auto [it, inserted] = representative_by_data_file.try_emplace(info.data_file_index, i);
        if (!inserted && info.file_name < file_infos[it->second].file_name)
            it->second = i;
    }

    VectorWithMemoryTracking<VectorWithMemoryTracking<size_t>> member_indices_by_pack_id;
    for (const auto & [data_file_index, member_index] : representative_by_data_file)
    {
        const auto pack_id = static_cast<size_t>(file_infos[member_index].pack_id);
        if (pack_id >= member_indices_by_pack_id.size())
            member_indices_by_pack_id.resize(pack_id + 1);
        member_indices_by_pack_id[pack_id].push_back(member_index);
    }
    return member_indices_by_pack_id;
}

}
