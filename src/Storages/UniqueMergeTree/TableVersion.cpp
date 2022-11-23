#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/UniqueMergeTree/TableVersion.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_EXCEPTION;
}

void TableVersion::serialize(const String & version_path, DiskPtr disk) const
{
    try
    {
        if (!disk->exists(version_path))
        {
            disk->createFile(version_path);
        }

        auto out = disk->writeFile(version_path);
        writeStringBinary(format, *out);
        writeVarUInt(version, *out);

        writeVarUInt(part_versions.size(), *out);
        for (const auto & [part_info, part_version] : part_versions)
        {
            /// Can not use writeString, no size info, deserialize will fail
            writeStringBinary(part_info.partition_id, *out);
            writeVarInt(part_info.min_block, *out);
            writeVarInt(part_info.max_block, *out);
            writeVarUInt(part_info.level, *out);
            writeVarUInt(part_info.mutation, *out);
            writeVarUInt(part_version, *out);
        }
    }
    catch (...)
    {
        throw Exception(ErrorCodes::UNKNOWN_EXCEPTION, "Serialize table version {} faild", version_path);
    }
}

void TableVersion::deserialize(const String & version_path, DiskPtr disk)
{
    if (!disk->exists(version_path))
        throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Table version file {{}} may have lost", version_path);
    size_t size;
	try
	{
        auto in = disk->readFile(version_path);

        String version_format;
        readStringBinary(version_format, *in);

        readVarUInt(version, *in);

        readVarUInt(size, *in);

        part_versions.reserve(size);

        String partition_id;
        Int64 min_block, max_block, mutation;
        UInt32 level;
        UInt64 part_version;

        for (size_t i = 0; i < size; ++i)
        {
            readStringBinary(partition_id, *in);
            readVarInt(min_block, *in);
            readVarInt(max_block, *in);
            readVarUInt(level, *in);
            readVarUInt(mutation, *in);
            readVarUInt(part_version, *in);

            part_versions.emplace(MergeTreePartInfo{partition_id, min_block, max_block, level, mutation}, part_version);
        }
	}
	catch (...)
	{
		throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Read table version {{}} failed", version_path);
	}
}

String TableVersion::dump() const
{
    WriteBufferFromOwnString s;
    writeString("Version: ", s);
    writeIntText(version, s);
    writeChar('\n', s);
    for (auto [part_info, part_version] : part_versions)
    {
        writeString(part_info.getPartName(), s);
        writeChar(':', s);
        writeIntText(part_version, s);
        writeChar('\n', s);
    }
    s.finalize();
    return s.str();
}

UInt64 TableVersion::getPartVersion(MergeTreePartInfo part_info) const
{
    if (auto it = part_versions.find(part_info); it != part_versions.end())
        return it->second;
    throw Exception(
        ErrorCodes::LOGICAL_ERROR,
        "Can not find version for part with part name {}, Table versions dump:\n{}",
        part_info.getPartName(),
        dump());
}
}
