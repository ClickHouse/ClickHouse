#include <Storages/MergeTree/MergeTreeWriteAheadLog.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeDataPartState.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <IO/MemoryReadWriteBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/logger_useful.h>
#include <Poco/JSON/JSON.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/JSON/Parser.h>
#include <sys/time.h>

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
    MergeTreeData & storage_,
    const DiskPtr & disk_,
    const String & name_)
    : storage(storage_)
    , disk(disk_)
    , name(name_)
    , path(storage.getRelativeDataPath() + name_)
    , pool(storage.getContext()->getSchedulePool())
    , log(&Poco::Logger::get(storage.getLogName() + " (WriteAheadLog)"))
{
    init();
    sync_task = pool.createTask("MergeTreeWriteAheadLog::sync", [this]
    {
        std::lock_guard lock(write_mutex);
        out->sync();
        sync_scheduled = false;
        sync_cv.notify_all();
    });
}

MergeTreeWriteAheadLog::~MergeTreeWriteAheadLog()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}


void MergeTreeWriteAheadLog::dropAllWriteAheadLogs(DiskPtr disk_to_drop, std::string relative_data_path)
{
    std::vector<std::string> files;
    disk_to_drop->listFiles(relative_data_path, files);
    for (const auto & file : files)
    {
        if (file.starts_with(WAL_FILE_NAME))
            disk_to_drop->removeFile(fs::path(relative_data_path) / file);
    }
}

void MergeTreeWriteAheadLog::init()
{
    out = disk->writeFile(path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);

    /// Small hack: in NativeWriter header is used only in `getHeader` method.
    /// To avoid complex logic of changing it during ALTERs we leave it empty.
    block_out = std::make_unique<NativeWriter>(*out, 0, Block{});
    min_block_number = std::numeric_limits<Int64>::max();
    max_block_number = -1;
    bytes_at_last_sync = 0;
}

void MergeTreeWriteAheadLog::dropPart(const String & part_name)
{
    std::unique_lock lock(write_mutex);

    writeIntBinary(WAL_VERSION, *out);

    ActionMetadata metadata{};
    metadata.write(*out);

    writeIntBinary(static_cast<UInt8>(ActionType::DROP_PART), *out);
    writeStringBinary(part_name, *out);
    out->next();
}

void MergeTreeWriteAheadLog::rotate(const std::unique_lock<std::mutex> &)
{
    String new_name = String(WAL_FILE_NAME) + "_"
        + toString(min_block_number) + "_"
        + toString(max_block_number) + WAL_FILE_EXTENSION;

    /// Finalize stream before file rename
    out->finalize();
    disk->replaceFile(path, storage.getRelativeDataPath() + new_name);
    init();
}

MergeTreeData::MutableDataPartsVector MergeTreeWriteAheadLog::restore(
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    DataPartsLock & parts_lock,
    bool readonly)
{
    std::unique_lock lock(write_mutex);

    MergeTreeData::MutableDataPartsVector parts;
    auto in = disk->readFile(path);
    NativeReader block_in(*in, 0);
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
            ActionMetadata metadata;

            readIntBinary(version, *in);
            if (version > 0)
            {
                metadata.read(*in);
            }

            readIntBinary(action_type, *in);
            readStringBinary(part_name, *in);

            if (action_type == ActionType::DROP_PART)
            {
                dropped_parts.insert(part_name);
            }
            else if (action_type == ActionType::ADD_PART)
            {
                auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + part_name, disk, 0);

                part = storage.getDataPartBuilder(part_name, single_disk_volume, part_name)
                    .withPartType(MergeTreeDataPartType::InMemory)
                    .withPartStorageType(MergeTreeDataPartStorageType::Full)
                    .build();

                part->uuid = metadata.part_uuid;
                block = block_in.read();

                if (storage.getActiveContainingPart(part->info, MergeTreeDataPartState::Active, parts_lock))
                    continue;
            }
            else
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unknown action type: {}", toString(static_cast<UInt8>(action_type)));
            }
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::CANNOT_READ_ALL_DATA
                || e.code() == ErrorCodes::UNKNOWN_FORMAT_VERSION
                || e.code() == ErrorCodes::BAD_DATA_PART_NAME
                || e.code() == ErrorCodes::CORRUPTED_DATA)
            {
                LOG_WARNING(log, "WAL file '{}' is broken. {}", path, e.displayText());

                /// If file is broken, do not write new parts to it.
                /// But if it contains any part rotate and save them.
                if (max_block_number == -1)
                {
                    if (!readonly)
                        disk->removeFile(path);
                }
                else if (name == DEFAULT_WAL_FILE_NAME)
                    rotate(lock);

                break;
            }
            throw;
        }

        if (action_type == ActionType::ADD_PART)
        {
            MergedBlockOutputStream part_out(
                part,
                metadata_snapshot,
                block.getNamesAndTypesList(),
                {},
                CompressionCodecFactory::instance().get("NONE", {}),
                NO_TRANSACTION_PTR);

            part->minmax_idx->update(block, storage.getMinMaxColumnsNames(metadata_snapshot->getPartitionKey()));
            part->partition.create(metadata_snapshot, block, 0, context);
            part->setColumns(block.getNamesAndTypesList(), {}, metadata_snapshot->getMetadataVersion());
            if (metadata_snapshot->hasSortingKey())
                metadata_snapshot->getSortingKey().expression->execute(block);

            part_out.write(block);

            for (const auto & projection : metadata_snapshot->getProjections())
            {
                auto projection_block = projection.calculate(block, context);
                auto temp_part = MergeTreeDataWriter::writeProjectionPart(storage, log, projection_block, projection, part.get());
                temp_part.finalize();
                if (projection_block.rows())
                    part->addProjectionPart(projection.name, std::move(temp_part.part));
            }

            part_out.finalizePart(part, false);

            min_block_number = std::min(min_block_number, part->info.min_block);
            max_block_number = std::max(max_block_number, part->info.max_block);
            parts.push_back(std::move(part));
        }
    }

    MergeTreeData::MutableDataPartsVector result;
    std::copy_if(parts.begin(), parts.end(), std::back_inserter(result),
        [&dropped_parts](const auto & part) { return dropped_parts.count(part->name) == 0; });

    /// All parts in WAL had been already committed into the disk -> clear the WAL
    if (!readonly && result.empty())
    {
        LOG_DEBUG(log, "WAL file '{}' had been completely processed. Removing.", path);
        disk->removeFile(path);
        init();
        return {};
    }

    return result;
}

void MergeTreeWriteAheadLog::shutdown()
{
    {
        std::unique_lock lock(write_mutex);
        if (shutdown_called)
             return;

        if (sync_scheduled)
            sync_cv.wait(lock, [this] { return !sync_scheduled; });

        shutdown_called = true;
        out->finalize();
        out.reset();
    }

    /// Do it without lock, otherwise inversion between pool lock and write_mutex is possible
    sync_task->deactivate();
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

String MergeTreeWriteAheadLog::ActionMetadata::toJSON() const
{
    Poco::JSON::Object json;

    if (part_uuid != UUIDHelpers::Nil)
        json.set(JSON_KEY_PART_UUID, toString(part_uuid));

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    json.stringify(oss);

    return oss.str();
}

void MergeTreeWriteAheadLog::ActionMetadata::fromJSON(const String & buf)
{
    Poco::JSON::Parser parser;
    auto json = parser.parse(buf).extract<Poco::JSON::Object::Ptr>();

    if (json->has(JSON_KEY_PART_UUID))
        part_uuid = parseFromString<UUID>(json->getValue<std::string>(JSON_KEY_PART_UUID));
}

void MergeTreeWriteAheadLog::ActionMetadata::read(ReadBuffer & meta_in)
{
    readIntBinary(min_compatible_version, meta_in);
    if (min_compatible_version > WAL_VERSION)
        throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION,
                        "WAL metadata version {} is not compatible with this ClickHouse version",
                        toString(min_compatible_version));

    size_t metadata_size;
    readVarUInt(metadata_size, meta_in);

    if (metadata_size == 0)
        return;

    String buf(metadata_size, ' ');
    meta_in.readStrict(buf.data(), metadata_size);

    fromJSON(buf);
}

void MergeTreeWriteAheadLog::ActionMetadata::write(WriteBuffer & meta_out) const
{
    writeIntBinary(min_compatible_version, meta_out);

    String ser_meta = toJSON();

    writeVarUInt(static_cast<UInt32>(ser_meta.length()), meta_out);
    writeString(ser_meta, meta_out);
}

}
