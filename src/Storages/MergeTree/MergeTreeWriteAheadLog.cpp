#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
}


MergeTreeWriteAheadLog::MergeTreeWriteAheadLog(
    const MergeTreeData & storage_,
    const DiskPtr & disk_,
    const String  & name)
    : storage(storage_)
    , disk(disk_)
    , path(storage.getRelativeDataPath() + name)
{
    init();
}

void MergeTreeWriteAheadLog::init()
{
    out = disk->writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    block_out = std::make_unique<NativeBlockOutputStream>(*out, 0, storage.getSampleBlock());
    min_block_number = std::numeric_limits<Int64>::max();
    max_block_number = std::numeric_limits<Int64>::min();
}

void MergeTreeWriteAheadLog::write(const Block & block, const String & part_name)
{
    std::lock_guard lock(write_mutex);

    auto part_info = MergeTreePartInfo::fromPartName(part_name, storage.format_version);
    min_block_number = std::min(min_block_number, part_info.min_block);
    max_block_number = std::max(max_block_number, part_info.max_block);

    writeIntBinary(static_cast<UInt8>(0), *out); /// version
    writeStringBinary(part_name, *out);
    block_out->write(block);
    block_out->flush();

    if (out->count() > MAX_WAL_BYTES)
        rotate();
}

void MergeTreeWriteAheadLog::rotate()
{
    String new_name = String(WAL_FILE_NAME) + "_"
        + toString(min_block_number) + "_"
        + toString(max_block_number) + WAL_FILE_EXTENSION;

    Poco::File(path).renameTo(storage.getFullPathOnDisk(disk) + new_name);
    init();
}

MergeTreeData::MutableDataPartsVector MergeTreeWriteAheadLog::restore()
{
    std::lock_guard lock(write_mutex);

    MergeTreeData::MutableDataPartsVector result;
    auto in = disk->readFile(path, DBMS_DEFAULT_BUFFER_SIZE);
    NativeBlockInputStream block_in(*in, 0);

    while (!in->eof())
    {
        UInt8 version;
        String part_name;
        readIntBinary(version, *in);
        if (version != 0)
            throw Exception("Unknown WAL format version: " + toString(version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

        readStringBinary(part_name, *in);
        auto part = storage.createPart(
            part_name,
            MergeTreeDataPartType::IN_MEMORY,
            MergeTreePartInfo::fromPartName(part_name, storage.format_version),
            storage.reserveSpace(0)->getDisk(),
            part_name);

        auto block = block_in.read();
        part->minmax_idx.update(block, storage.minmax_idx_columns);
        part->partition.create(storage, block, 0);

        MergedBlockOutputStream part_out(part, block.getNamesAndTypesList(), {}, nullptr);
        part_out.writePrefix();
        part_out.write(block);
        part_out.writeSuffixAndFinalizePart(part);

        result.push_back(std::move(part));
    }

    return result;
}

}
