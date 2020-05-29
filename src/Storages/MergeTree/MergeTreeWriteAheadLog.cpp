#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <IO/ReadHelpers.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int BAD_DATA_PART_NAME;
    extern const int CORRUPTED_DATA;
}


MergeTreeWriteAheadLog::MergeTreeWriteAheadLog(
    const MergeTreeData & storage_,
    const DiskPtr & disk_,
    const String & name_)
    : storage(storage_)
    , disk(disk_)
    , name(name_)
    , path(storage.getRelativeDataPath() + name_)
{
    init();
}

void MergeTreeWriteAheadLog::init()
{
    out = disk->writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    block_out = std::make_unique<NativeBlockOutputStream>(*out, 0, storage.getSampleBlock());
    min_block_number = std::numeric_limits<Int64>::max();
    max_block_number = -1;
}

void MergeTreeWriteAheadLog::addPart(const Block & block, const String & part_name)
{
    std::lock_guard lock(write_mutex);

    auto part_info = MergeTreePartInfo::fromPartName(part_name, storage.format_version);
    min_block_number = std::min(min_block_number, part_info.min_block);
    max_block_number = std::max(max_block_number, part_info.max_block);

    writeIntBinary(static_cast<UInt8>(0), *out); /// version
    writeIntBinary(static_cast<UInt8>(ActionType::ADD_PART), *out);
    writeStringBinary(part_name, *out);
    block_out->write(block);
    block_out->flush();

    auto max_wal_bytes = storage.getSettings()->write_ahead_log_max_bytes;
    if (out->count() > max_wal_bytes)
        rotate();
}

void MergeTreeWriteAheadLog::dropPart(const String & part_name)
{
    std::lock_guard lock(write_mutex);

    writeIntBinary(static_cast<UInt8>(0), *out);
    writeIntBinary(static_cast<UInt8>(ActionType::DROP_PART), *out);
    writeStringBinary(part_name, *out);
}

void MergeTreeWriteAheadLog::rotate()
{
    String new_name = String(WAL_FILE_NAME) + "_"
        + toString(min_block_number) + "_"
        + toString(max_block_number) + WAL_FILE_EXTENSION;

    disk->replaceFile(path, storage.getRelativeDataPath() + new_name);
    init();
}

MergeTreeData::MutableDataPartsVector MergeTreeWriteAheadLog::restore()
{
    std::lock_guard lock(write_mutex);

    MergeTreeData::MutableDataPartsVector parts;
    auto in = disk->readFile(path, DBMS_DEFAULT_BUFFER_SIZE);
    NativeBlockInputStream block_in(*in, 0);
    NameSet dropped_parts;

    while (!in->eof())
    {
        MergeTreeData::MutableDataPartPtr part;
        UInt8 version;
        String part_name;
        Block block;
        ActionType action_type;

        try
        {
            readIntBinary(version, *in);
            if (version != 0)
                throw Exception("Unknown WAL format version: " + toString(version), ErrorCodes::UNKNOWN_FORMAT_VERSION);

            readIntBinary(action_type, *in);
            readStringBinary(part_name, *in);

            if (action_type == ActionType::DROP_PART)
            {
                dropped_parts.insert(part_name);
            }
            else if (action_type == ActionType::ADD_PART)
            {
                part = storage.createPart(
                    part_name,
                    MergeTreeDataPartType::IN_MEMORY,
                    MergeTreePartInfo::fromPartName(part_name, storage.format_version),
                    storage.reserveSpace(0)->getDisk(),
                    part_name);

                block = block_in.read();
            }
            else
            {
                throw Exception("Unknown action type: " + toString(static_cast<UInt8>(action_type)), ErrorCodes::CORRUPTED_DATA);
            }
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA
                || e.code() == ErrorCodes::UNKNOWN_FORMAT_VERSION
                || e.code() == ErrorCodes::BAD_DATA_PART_NAME
                || e.code() == ErrorCodes::CORRUPTED_DATA)
            {
                LOG_WARNING(&Logger::get(storage.getLogName() + " (WriteAheadLog)"),
                    "WAL file '" << path << "' is broken. " << e.displayText());

                /// If file is broken, do not write new parts to it.
                /// But if it contains any part rotate and save them.
                if (max_block_number == -1)
                    disk->remove(path);
                else if (name == DEFAULT_WAL_FILE)
                    rotate();

                break;
            }
            throw;
        }

        if (action_type == ActionType::ADD_PART)
        {
            MergedBlockOutputStream part_out(part, block.getNamesAndTypesList(), {}, nullptr);

            part->minmax_idx.update(block, storage.minmax_idx_columns);
            if (storage.partition_key_expr)
                part->partition.create(storage, block, 0);
            if (storage.hasSortingKey())
                storage.sorting_key_expr->execute(block);

            part_out.writePrefix();
            part_out.write(block);
            part_out.writeSuffixAndFinalizePart(part);

            min_block_number = std::min(min_block_number, part->info.min_block);
            max_block_number = std::max(max_block_number, part->info.max_block);
            parts.push_back(std::move(part));
        }
    }

    MergeTreeData::MutableDataPartsVector result;
    std::copy_if(parts.begin(), parts.end(), std::back_inserter(result),
        [&dropped_parts](const auto & part) { return dropped_parts.count(part->name) == 0; });

    return result;
}

std::optional<MergeTreeWriteAheadLog::MinMaxBlockNumber>
MergeTreeWriteAheadLog::tryParseMinMaxBlockNumber(const String & filename)
{
    Int64 min_block;
    Int64 max_block;
    ReadBufferFromString in(filename);
    if (!checkString(WAL_FILE_NAME, in)
        || !checkChar('_', in)
        || !tryReadIntText(min_block, in)
        || !checkChar('_', in)
        || !tryReadIntText(max_block, in))
    {
        return {};
    }

    return std::make_pair(min_block, max_block);
}

}
