#include <Disks/DiskLocal.h>
#include <Disks/SingleDiskVolume.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Compression/CompressionFactory.h>
#include <Common/SipHash.h>
#include <Storages/MergeTree/MergeTreeSettings.h>

#include <boost/program_options.hpp>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityAdaptive.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>
#include <Storages/MergeTree/MergeTreeDataPartWide.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>

namespace fs = std::filesystem;
namespace po = boost::program_options;

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

struct RepareEntry
{
    enum class Status
    {
        Ok,
        NeedRepare,
        Broken,
    };

    Status status = Status::Ok;
    ISerialization::Kind from_kind = ISerialization::Kind::DEFAULT;
    ISerialization::Kind to_kind = ISerialization::Kind::DEFAULT;
};

RepareEntry checkColumnWidePart(
    const NameAndTypePair & column,
    const IDataPartStorage & part_storage,
    const SerializationInfoByName & infos,
    const String & mark_extension)
{
    auto default_serialization = column.type->getDefaultSerialization();
    auto sparse_serialization = column.type->getSparseSerialization();

    bool has_all_streams_for_sparse = true;
    bool has_all_streams_for_default = true;

    auto exists_stream = [&](const auto & substream_path)
    {
        auto stream_name = ISerialization::getFileNameForStream(column, substream_path);
        if (part_storage.existsFile(stream_name + ".bin") && part_storage.existsFile(stream_name + mark_extension))
            return true;

        stream_name = sipHash128String(stream_name);
        return part_storage.existsFile(stream_name + ".bin") && part_storage.existsFile(stream_name + mark_extension);
    };

    default_serialization->enumerateStreams([&](const auto & substream_path)
    {
        if (!exists_stream(substream_path))
            has_all_streams_for_default = false;
    });

    sparse_serialization->enumerateStreams([&](const auto & substream_path)
    {
        if (!exists_stream(substream_path))
            has_all_streams_for_sparse = false;
    });

    RepareEntry entry;

    if (!has_all_streams_for_default && !has_all_streams_for_sparse)
    {
        entry.status = RepareEntry::Status::Broken;
        return entry;
    }

    auto it = infos.find(column.name);
    entry.to_kind = has_all_streams_for_sparse ? ISerialization::Kind::SPARSE : ISerialization::Kind::DEFAULT;
    entry.from_kind = it == infos.end() ? ISerialization::Kind::DEFAULT : it->second->getKind();

    if (entry.from_kind == entry.to_kind)
    {
        entry.status = RepareEntry::Status::Ok;
        return entry;
    }

    entry.status = RepareEntry::Status::NeedRepare;
    return entry;
}

size_t getBytesBetweenMarksInCompactPart(
    const IDataPartStorage & part_storage,
    MarkInCompressedFile from_mark,
    MarkInCompressedFile to_mark)
{
    if (from_mark.offset_in_decompressed_block != 0 || to_mark.offset_in_decompressed_block != 0)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Reading from compact parts with non-zero offset in decompressed block is not supported");

    size_t total_bytes = 0;
    size_t offset_in_file = from_mark.offset_in_compressed_file;
    auto file_in = part_storage.readFile(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION, {}, {});

    while (offset_in_file < to_mark.offset_in_compressed_file)
    {
        file_in->seek(offset_in_file, SEEK_SET);
        file_in->ignore(sizeof(CityHash_v1_0_2::uint128));

        WriteBufferFromOwnString header_out;
        copyData(*file_in, header_out, ICompressionCodec::getHeaderSize());
        auto header_str = header_out.str();

        uint8_t method = ICompressionCodec::readMethod(header_str.data());
        auto codec = CompressionCodecFactory::instance().get(method);

        size_t size_compressed = codec->readCompressedBlockSize(header_str.data());
        size_t size_decompressed = codec->readDecompressedBlockSize(header_str.data());

        total_bytes += size_decompressed;
        offset_in_file += size_compressed + sizeof(CityHash_v1_0_2::uint128);
    }

    return total_bytes;
}

void tryReadColumnInCompactPart(
    MergeTreeReaderStream & stream,
    const IDataPartStorage & part_storage,
    const MergeTreeMarksGetter & marks_getter,
    const NameAndTypePair & name_and_type,
    const ISerialization & serialization,
    const MergeTreeIndexGranularity & index_granularity,
    size_t column_position,
    size_t num_columns)
{
    auto get_next_mark = [&](size_t row_idx)
    {
        if (column_position + 1 == num_columns)
            return marks_getter.getMark(row_idx + 1, 0);

        return marks_getter.getMark(row_idx, column_position + 1);
    };

    size_t marks_count = index_granularity.getMarksCountWithoutFinal();
    for (size_t i = 0; i < marks_count; ++i)
    {
        size_t rows_to_read = index_granularity.getMarkRows(i);
        ColumnPtr column = name_and_type.type->createColumn(serialization);

        size_t bytes_in_granule = getBytesBetweenMarksInCompactPart(part_storage, marks_getter.getMark(i, column_position), get_next_mark(i));
        stream.seekToMarkAndColumn(i, column_position);
        String granule_data;

        {
            WriteBufferFromString granule_out(granule_data);
            copyData(*stream.getDataBuffer(), granule_out, bytes_in_granule);
        }

        ReadBufferFromString granule_in(granule_data);
        ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
        ISerialization::DeserializeBinaryBulkStatePtr state;

        deserialize_settings.getter = [&](const auto &) -> ReadBuffer * { return &granule_in; };
        deserialize_settings.object_and_dynamic_read_statistics = true;
        deserialize_settings.use_specialized_prefixes_and_suffixes_substreams = true;
        deserialize_settings.data_part_type = MergeTreeDataPartType::Compact;

        serialization.deserializeBinaryBulkStatePrefix(deserialize_settings, state, nullptr);
        serialization.deserializeBinaryBulkWithMultipleStreams(column, 0, rows_to_read, deserialize_settings, state, nullptr);

        if (column->size() != rows_to_read)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Column {} has {} rows, but expected {}", column_position, column->size(), rows_to_read);

        if (!granule_in.eof())
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Cannot read all data for column: {}", column_position);
    }
}

RepareEntry checkColumnCompactPart(
    MergeTreeReaderStream & stream,
    const IDataPartStorage & part_storage,
    const MergeTreeMarksGetter & marks_getter,
    const NameAndTypePair & column,
    size_t column_position,
    size_t num_columns,
    const SerializationInfoByName & infos,
    const MergeTreeIndexGranularity & index_granularity)
{
    bool can_read_as_default = false;
    bool can_read_as_sparse = false;

    try
    {
        auto default_serialization = column.type->getDefaultSerialization();
        tryReadColumnInCompactPart(stream, part_storage, marks_getter, column, *default_serialization, index_granularity, column_position, num_columns);
        can_read_as_default = true;
    }
    catch (...)
    {
        can_read_as_default = false;
    }

    if (column.type->supportsSparseSerialization())
    {
        try
        {
            auto sparse_serialization = column.type->getSparseSerialization();
            tryReadColumnInCompactPart(stream, part_storage, marks_getter, column, *sparse_serialization, index_granularity, column_position, num_columns);
            can_read_as_sparse = true;
        }
        catch (...)
        {
            can_read_as_sparse = false;
        }
    }

    RepareEntry entry;

    if (!can_read_as_default && !can_read_as_sparse)
    {
        entry.status = RepareEntry::Status::Broken;
        return entry;
    }

    if (can_read_as_default && can_read_as_sparse)
    {
        entry.status = RepareEntry::Status::Ok;
        return entry;
    }

    auto it = infos.find(column.name);
    entry.to_kind = can_read_as_sparse ? ISerialization::Kind::SPARSE : ISerialization::Kind::DEFAULT;
    entry.from_kind = it == infos.end() ? ISerialization::Kind::DEFAULT : it->second->getKind();

    if (entry.from_kind == entry.to_kind)
    {
        entry.status = RepareEntry::Status::Ok;
        return entry;
    }

    entry.status = RepareEntry::Status::NeedRepare;
    return entry;
}

int mainEntryClickHouseRestorePartsSerialization(int argc, char ** argv)
try
{
    po::options_description desc("Allowed options");
    desc.add_options()
        ("dry-run,n", po::bool_switch(), "Dry run")
        ("input-dir,i", po::value<std::string>()->required(), "Input directory")
        ;

    po::variables_map options;
    po::store(po::parse_command_line(argc, argv, desc), options);

    auto part_dir = options["input-dir"].as<std::string>();

    if (!fs::exists(part_dir))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Input directory '{}' does not exist", part_dir);

    std::cerr << "Input directory: " << part_dir << std::endl;

    auto disk =  std::make_shared<DiskLocal>("tmp_local_disk", "./");
    auto volume = std::make_shared<SingleDiskVolume>("tmp_local_volume", disk);

    MutableDataPartStoragePtr part_storage = std::make_shared<DataPartStorageOnDiskFull>(volume, "./", part_dir);
    auto mark_type = MergeTreeIndexGranularityInfo::getMarksTypeFromFilesystem(*part_storage);
    MergeTreeIndexGranularityInfo index_granularity_info(*mark_type, 8192, 10 * 1024 * 1024);
    bool need_repair = false;

    if (!mark_type)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No mark type found in the input directory '{}'", part_dir);

    std::cerr << "Mark type: (" << mark_type->describe() << ")" << std::endl;

    NamesAndTypesList columns;
    SerializationInfoByName infos({});
    MergeTreeIndexGranularityPtr index_granularity = std::make_shared<MergeTreeIndexGranularityAdaptive>();

    if (auto columns_in = part_storage->readFileIfExists("columns.txt", {}, {}))
        columns.readText(*columns_in);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Columns file not found in the input directory '{}'", part_dir);

    if (auto in = part_storage->readFileIfExists("serialization.json", {}, {}))
        infos = SerializationInfoByName::readJSON(columns, *in);

    auto change_kind = [&](const NameAndTypePair & column, ISerialization::Kind to_kind)
    {
        auto it = infos.find(column.name);

        if (it != infos.end())
        {
            it->second->setKind(to_kind);
        }
        else if (to_kind == ISerialization::Kind::SPARSE)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Column {} not found in serialization.json", column.name);
        }
    };

    if (mark_type->part_type == MergeTreeDataPartType::Wide)
    {
        String any_column_filename = columns.front().name;
        MergeTreeDataPartWide::loadIndexGranularityImpl(index_granularity, index_granularity_info, *part_storage, any_column_filename, MergeTreeSettings{});

        for (const auto & column : columns)
        {
            auto repare_entry = checkColumnWidePart(column, *part_storage, infos, mark_type->getFileExtension());

            if (repare_entry.status == RepareEntry::Status::Broken)
            {
                std::cout << "Column " << column.name << " is broken and cannot be repaired" << std::endl;
            }
            else if (repare_entry.status == RepareEntry::Status::NeedRepare)
            {
                std::cout << "Column " << column.name << " needs repare from " << toString(repare_entry.from_kind) << " to " << toString(repare_entry.to_kind) << std::endl;
                change_kind(column, repare_entry.to_kind);
                need_repair = true;
            }
        }
    }
    else
    {
        MergeTreeDataPartCompact::loadIndexGranularityImpl(index_granularity, index_granularity_info, columns.size(), *part_storage, MergeTreeSettings{});

        auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
            part_storage,
            nullptr,
            MergeTreeDataPartCompact::DATA_FILE_NAME + mark_type->getFileExtension(),
            index_granularity->getMarksCount(),
            index_granularity_info,
            false,
            ReadSettings{},
            nullptr,
            columns.size());

        MergeTreeReaderSettings reader_settings;
        reader_settings.allow_different_codecs = true;

        std::unique_ptr<MergeTreeReaderStream> stream = std::make_unique<MergeTreeReaderStreamAllOfMultipleColumns>(
            part_storage,
            MergeTreeDataPartCompact::DATA_FILE_NAME,
            MergeTreeDataPartCompact::DATA_FILE_EXTENSION,
            index_granularity->getMarksCount(),
            MarkRanges{{0, index_granularity->getMarksCount()}},
            reader_settings,
            nullptr,
            part_storage->getFileSize(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION),
            marks_loader,
            nullptr,
            CLOCK_MONOTONIC);

        size_t column_position = 0;
        auto marks_getter = marks_loader->loadMarks();

        for (const auto & column : columns)
        {
            auto repare_entry = checkColumnCompactPart(*stream, *part_storage, *marks_getter, column, column_position, columns.size(), infos, *index_granularity);

            if (repare_entry.status == RepareEntry::Status::Broken)
            {
                std::cout << "Column " << column.name << " is broken and cannot be repaired" << std::endl;
            }
            else if (repare_entry.status == RepareEntry::Status::NeedRepare)
            {
                std::cout << "Column " << column.name << " needs repare from " << toString(repare_entry.from_kind) << " to " << toString(repare_entry.to_kind) << std::endl;
                change_kind(column, repare_entry.to_kind);
                need_repair = true;
            }

            ++column_position;
        }
    }

    bool dry_run = options["dry-run"].as<bool>();

    if (!dry_run && need_repair)
    {
        std::cout << "Applying changes to repair serialization.json" << std::endl;
        auto out = part_storage->writeFile("serialization.json", 4096, WriteSettings{});
        infos.writeJSON(*out);
        out->finalize();
    }

    return (need_repair && dry_run) ? 1 : 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << '\n';
    return 127;
}
