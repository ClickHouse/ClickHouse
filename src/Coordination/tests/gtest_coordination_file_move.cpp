#include "config.h"

#if USE_NURAFT

#include <Coordination/tests/gtest_coordination_common.h>

#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Common/ProfileEvents.h>
#include <Disks/DiskLocal.h>
#include <IO/HashingWriteBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileDecorator.h>

#include <array>
#include <atomic>
#include <stdexcept>

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsBool disk_move_verify_destination_read_back;
    extern const CoordinationSettingsUInt64 disk_move_retries_during_init;
    extern const CoordinationSettingsUInt64 disk_move_retries_wait_ms;
}

namespace ProfileEvents
{
    extern const Event S3CompleteMultipartUploadAdoptedExistingObject;
}

namespace
{

class ChunkedReadBuffer final : public DB::ReadBuffer
{
public:
    ChunkedReadBuffer(std::string data_, size_t chunk_size_)
        : DB::ReadBuffer(nullptr, 0)
        , data(std::move(data_))
        , chunk_size(chunk_size_)
    {
    }

private:
    bool nextImpl() override
    {
        if (offset == data.size())
            return false;
        const size_t size = std::min(chunk_size, data.size() - offset);
        working_buffer = Buffer(data.data() + offset, data.data() + offset + size);
        offset += size;
        return true;
    }

    std::string data;
    size_t offset = 0;
    const size_t chunk_size;
};

DB::KeeperFileDigest expectedDigest(std::string_view data)
{
    CityHash_v1_0_2::uint128 hash(0, 0);
    size_t offset = 0;
    while (data.size() - offset >= DBMS_DEFAULT_HASHING_BLOCK_SIZE)
    {
        hash = CityHash_v1_0_2::CityHash128WithSeed(
            data.data() + offset,
            DBMS_DEFAULT_HASHING_BLOCK_SIZE,
            hash);
        offset += DBMS_DEFAULT_HASHING_BLOCK_SIZE;
    }
    if (offset != data.size())
        hash = CityHash_v1_0_2::CityHash128WithSeed(data.data() + offset, data.size() - offset, hash);
    return {.size = data.size(), .hash = hash};
}

class FailingFirstMarkerSyncWriteBuffer final : public DB::WriteBufferFromFileDecorator
{
public:
    FailingFirstMarkerSyncWriteBuffer(std::unique_ptr<DB::WriteBuffer> impl_, std::atomic_size_t & sync_attempts_)
        : DB::WriteBufferFromFileDecorator(std::move(impl_))
        , sync_attempts(sync_attempts_)
    {
    }

    void sync() override
    {
        DB::WriteBufferFromFileDecorator::sync();
        if (++sync_attempts == 1)
            throw std::runtime_error("injected first marker sync failure");
    }

private:
    std::atomic_size_t & sync_attempts;
};

class MarkerRetryDiskLocal final : public DB::DiskLocal
{
public:
    using DB::DiskLocal::DiskLocal;

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override
    {
        auto buffer = DB::DiskLocal::writeFile(path, buf_size, mode, settings);
        if (path == "tmp_destination")
            return std::make_unique<FailingFirstMarkerSyncWriteBuffer>(std::move(buffer), marker_sync_attempts);
        return buffer;
    }

    std::atomic_size_t marker_sync_attempts{0};
};

class CountingDiskLocal final : public DB::DiskLocal
{
public:
    using DB::DiskLocal::DiskLocal;

    void prepareRead(
        const String & path,
        const DB::ReadSettings & settings,
        std::optional<size_t> read_hint,
        DB::ReadPipeline & pipeline) const override
    {
        if (path == "destination")
        {
            ++destination_reads;
            if (!settings.enable_filesystem_cache)
                ++cache_bypassing_destination_reads;
        }
        DB::DiskLocal::prepareRead(path, settings, read_hint, pipeline);
    }

    mutable std::atomic<size_t> destination_reads{0};
    mutable std::atomic<size_t> cache_bypassing_destination_reads{0};
};

class AdoptingSourceDiskLocal final : public DB::DiskLocal
{
public:
    using DB::DiskLocal::DiskLocal;

    void copyFile(
        const String & from_file_path,
        DB::IDisk & to_disk,
        const String & to_file_path,
        const DB::ReadSettings & read_settings,
        const DB::WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override
    {
        DB::DiskLocal::copyFile(from_file_path, to_disk, to_file_path, read_settings, write_settings, cancellation_hook);
        auto stale = to_disk.writeFile(to_file_path);
        constexpr std::string_view stale_data = "stale-bytes";
        stale->write(stale_data.data(), stale_data.size());
        stale->finalize();
        ProfileEvents::increment(ProfileEvents::S3CompleteMultipartUploadAdoptedExistingObject);
    }
};

enum class MoveFailureMode : uint8_t
{
    None,
    MarkerWrite,
    Copy,
    DestinationSize,
    MarkerRemove,
    SourceRemove,
};

class FailingMoveDisk final : public DB::DiskLocal
{
public:
    FailingMoveDisk(const String & disk_name, const String & path, MoveFailureMode failure_mode_)
        : DB::DiskLocal(disk_name, path)
        , failure_mode(failure_mode_)
    {
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeFile(
        const String & path,
        size_t buf_size,
        DB::WriteMode mode,
        const DB::WriteSettings & settings) override
    {
        if (failure_mode == MoveFailureMode::MarkerWrite && path.starts_with("tmp_"))
            throw std::runtime_error("marker write failure");
        return DB::DiskLocal::writeFile(path, buf_size, mode, settings);
    }

    void copyFile(
        const String & from_file_path,
        DB::IDisk & to_disk,
        const String & to_file_path,
        const DB::ReadSettings & read_settings,
        const DB::WriteSettings & write_settings,
        const std::function<void()> & cancellation_hook) override
    {
        if (failure_mode == MoveFailureMode::Copy)
            throw std::runtime_error("copy failure");
        DB::DiskLocal::copyFile(from_file_path, to_disk, to_file_path, read_settings, write_settings, cancellation_hook);
    }

    size_t getFileSize(const String & path) const override
    {
        const size_t size = DB::DiskLocal::getFileSize(path);
        return failure_mode == MoveFailureMode::DestinationSize && path == "destination" ? size + 1 : size;
    }

    void removeFileIfExists(const String & path) override
    {
        if ((failure_mode == MoveFailureMode::MarkerRemove && path.starts_with("tmp_"))
            || (failure_mode == MoveFailureMode::SourceRemove && path == "source"))
            throw std::runtime_error("remove failure");
        DB::DiskLocal::removeFileIfExists(path);
    }

private:
    MoveFailureMode failure_mode;
};

void writeFile(const DB::DiskPtr & disk, const String & path, std::string_view contents)
{
    auto output = disk->writeFile(path);
    output->write(contents.data(), contents.size());
    output->finalize();
}

}

TEST(KeeperMoveMarker, VersionOneEncodingAndParsing)
{
    const DB::KeeperFileDigest digest{
        .size = 0x0102030405060708ULL,
        .hash = CityHash_v1_0_2::uint128(0x1112131415161718ULL, 0x2122232425262728ULL)};
    const auto marker = DB::serializeKeeperMoveMarker(digest);

    ASSERT_EQ(marker.size(), 33);
    EXPECT_EQ(std::string_view(marker.data(), 8), "KEEPERMV");
    EXPECT_EQ(static_cast<uint8_t>(marker[8]), 1);
    EXPECT_EQ(static_cast<uint8_t>(marker[9]), 0x08);
    EXPECT_EQ(static_cast<uint8_t>(marker[16]), 0x01);
    EXPECT_EQ(static_cast<uint8_t>(marker[17]), 0x18);
    EXPECT_EQ(static_cast<uint8_t>(marker[24]), 0x11);
    EXPECT_EQ(static_cast<uint8_t>(marker[25]), 0x28);
    EXPECT_EQ(static_cast<uint8_t>(marker[32]), 0x21);

    const auto parsed = DB::parseKeeperMoveMarker(marker);
    ASSERT_TRUE(parsed);
    EXPECT_EQ(*parsed, digest);
}

TEST(KeeperMoveMarker, ClassifiesEveryInvalidShape)
{
    const DB::KeeperFileDigest digest{.size = 1, .hash = CityHash_v1_0_2::uint128(2, 3)};
    const auto marker = DB::serializeKeeperMoveMarker(digest);

    EXPECT_EQ(DB::parseKeeperMoveMarker("").error(), DB::KeeperMoveMarkerParseError::LegacyEmpty);
    for (size_t size = 1; size != marker.size(); ++size)
        EXPECT_EQ(DB::parseKeeperMoveMarker(std::string_view(marker).substr(0, size)).error(), DB::KeeperMoveMarkerParseError::Malformed)
            << "size=" << size;

    auto malformed = marker;
    malformed[0] = 'X';
    EXPECT_EQ(DB::parseKeeperMoveMarker(malformed).error(), DB::KeeperMoveMarkerParseError::Malformed);
    malformed = marker;
    malformed[8] = 2;
    EXPECT_EQ(DB::parseKeeperMoveMarker(malformed).error(), DB::KeeperMoveMarkerParseError::UnknownVersion);
    malformed += "future-version-fields";
    EXPECT_EQ(DB::parseKeeperMoveMarker(malformed).error(), DB::KeeperMoveMarkerParseError::UnknownVersion);
    EXPECT_EQ(DB::parseKeeperMoveMarker(marker + "x").error(), DB::KeeperMoveMarkerParseError::Malformed);
}

TEST(KeeperMoveMarker, ReadsFutureVersionHeaderFromLongerMarker)
{
    fs::create_directories("./tmp");
    ChangelogDirTest root("./tmp/gtest_keeper_move_marker_future_version");
    auto disk = std::make_shared<DB::DiskLocal>("marker", root.path);

    const DB::KeeperFileDigest digest{.size = 1, .hash = CityHash_v1_0_2::uint128(2, 3)};
    auto marker = DB::serializeKeeperMoveMarker(digest);
    marker[8] = 2;
    marker += std::string(128, 'x');
    writeFile(disk, "marker", marker);

    const auto parsed = DB::readKeeperMoveMarker(disk, "marker");
    ASSERT_FALSE(parsed);
    EXPECT_EQ(parsed.error(), DB::KeeperMoveMarkerParseError::UnknownVersion);
}

TEST(KeeperMoveMarker, DigestUsesVersionOneBlockConstructionAcrossInputChunks)
{
    constexpr std::array<size_t, 8> sizes{0, 1, 2047, 2048, 2049, 4095, 4096, 4097};
    constexpr std::array<size_t, 6> chunk_sizes{1, 17, 2047, 2048, 3073, 8192};
    for (const size_t size : sizes)
    {
        std::string data(size, '\0');
        for (size_t i = 0; i != data.size(); ++i)
            data[i] = static_cast<char>(i * 17);
        const auto expected = expectedDigest(data);
        for (const size_t chunk_size : chunk_sizes)
        {
            ChunkedReadBuffer input(data, chunk_size);
            EXPECT_EQ(DB::computeKeeperFileDigest(input), expected)
                << "size=" << size << " chunk_size=" << chunk_size;
        }
    }
}

TEST(KeeperFileMove, LocalMoveAndCallbackOutcomes)
{
    fs::create_directories("./tmp");
    ChangelogDirTest root("./tmp/gtest_keeper_file_move_order");
    fs::create_directories(root.path + "/source");
    fs::create_directories(root.path + "/destination");
    auto source = std::make_shared<DB::DiskLocal>("source", root.path + "/source");
    auto destination = std::make_shared<DB::DiskLocal>("destination", root.path + "/destination");
    writeFile(source, "file", "keeper-data");
    auto keeper_context = makeKeeperContext(false);

    const auto result = DB::moveFileBetweenDisks(
        source,
        "file",
        destination,
        "file",
        [&]
        {
            return true;
        },
        getLogger("KeeperFileMoveTest"),
        keeper_context);

    EXPECT_TRUE(result);
    EXPECT_FALSE(source->existsFile("file"));
    EXPECT_TRUE(destination->existsFile("file"));
    EXPECT_FALSE(destination->existsFile("tmp_file"));

    writeFile(source, "rejected", "source-remains");
    const auto rejected = DB::moveFileBetweenDisks(
        source,
        "rejected",
        destination,
        "rejected",
        [] { return false; },
        getLogger("KeeperFileMoveTest"),
        keeper_context);
    ASSERT_FALSE(rejected);
    EXPECT_EQ(rejected.error(), DB::KeeperMoveError::CallbackRejectedOrThrew);
    EXPECT_TRUE(source->existsFile("rejected"));
    EXPECT_TRUE(destination->existsFile("rejected"));

    writeFile(source, "exception", "source-remains");
    const auto callback_exception = DB::moveFileBetweenDisks(
        source,
        "exception",
        destination,
        "exception",
        []() -> bool { throw std::runtime_error("callback failure"); },
        getLogger("KeeperFileMoveTest"),
        keeper_context);
    ASSERT_FALSE(callback_exception);
    EXPECT_EQ(callback_exception.error(), DB::KeeperMoveError::CallbackRejectedOrThrew);
    EXPECT_TRUE(source->existsFile("exception"));
}

TEST(KeeperFileMove, DestinationReadBackIsDisabledByDefaultAndCacheBypassingWhenEnabled)
{
    fs::create_directories("./tmp");
    ChangelogDirTest root("./tmp/gtest_keeper_file_move_read_back");
    fs::create_directories(root.path + "/source");
    fs::create_directories(root.path + "/destination");
    auto source = std::make_shared<DB::DiskLocal>("source", root.path + "/source");
    auto destination = std::make_shared<CountingDiskLocal>("destination", root.path + "/destination");
    writeFile(source, "first", "first-data");
    auto keeper_context = makeKeeperContext(false);

    EXPECT_TRUE(DB::moveFileBetweenDisks(
        source, "first", destination, "destination", {}, getLogger("KeeperFileMoveTest"), keeper_context));
    EXPECT_EQ(destination->destination_reads, 0);

    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::disk_move_verify_destination_read_back] = true;
    auto verifying_context = makeKeeperContext(false, settings);
    writeFile(source, "second", "second-data");
    EXPECT_TRUE(DB::moveFileBetweenDisks(
        source, "second", destination, "destination", {}, getLogger("KeeperFileMoveTest"), verifying_context));
    EXPECT_EQ(destination->destination_reads, 1);
    EXPECT_EQ(destination->cache_bypassing_destination_reads, 1);
}

TEST(KeeperFileMove, MarkerPublicationRetriesAfterSyncFailure)
{
    fs::create_directories("./tmp");
    ChangelogDirTest root("./tmp/gtest_keeper_file_move_marker_retry");
    fs::create_directories(root.path + "/source");
    fs::create_directories(root.path + "/destination");
    auto source = std::make_shared<DB::DiskLocal>("source", root.path + "/source");
    auto destination = std::make_shared<MarkerRetryDiskLocal>("destination", root.path + "/destination");
    writeFile(source, "source", "immutable-source");

    auto settings = std::make_shared<DB::CoordinationSettings>();
    (*settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 2;
    (*settings)[DB::CoordinationSetting::disk_move_retries_wait_ms] = 0;
    const auto result = DB::moveFileBetweenDisks(
        source,
        "source",
        destination,
        "destination",
        {},
        getLogger("KeeperFileMoveTest"),
        makeKeeperContext(false, settings));

    ASSERT_TRUE(result);
    EXPECT_EQ(destination->marker_sync_attempts.load(), 2);
    EXPECT_FALSE(destination->existsFile("tmp_destination"));
    EXPECT_TRUE(destination->existsFile("destination"));
}

TEST(KeeperFileMove, AdoptedMultipartObjectForcesCacheBypassingDigestValidation)
{
    fs::create_directories("./tmp");
    ChangelogDirTest root("./tmp/gtest_keeper_file_move_adopted_object");
    fs::create_directories(root.path + "/source");
    fs::create_directories(root.path + "/destination");
    auto source = std::make_shared<AdoptingSourceDiskLocal>("source", root.path + "/source");
    auto destination = std::make_shared<CountingDiskLocal>("destination", root.path + "/destination");
    writeFile(source, "source", "fresh-bytes");

    const auto result = DB::moveFileBetweenDisks(
        source,
        "source",
        destination,
        "destination",
        {},
        getLogger("KeeperFileMoveTest"),
        makeKeeperContext(false));

    ASSERT_FALSE(result);
    EXPECT_EQ(result.error(), DB::KeeperMoveError::CopyCompletedDestinationValidationFailed);
    EXPECT_TRUE(source->existsFile("source"));
    EXPECT_TRUE(destination->existsFile("tmp_destination"));
    EXPECT_EQ(destination->destination_reads, 1);
    EXPECT_EQ(destination->cache_bypassing_destination_reads, 1);
}

TEST(KeeperFileMove, EveryFailurePhaseReturnsTypedOutcomeAndPreservesAuthority)
{
    struct Case
    {
        MoveFailureMode source_failure;
        MoveFailureMode destination_failure;
        DB::KeeperMoveError expected;
    };
    constexpr std::array cases{
        Case{MoveFailureMode::None, MoveFailureMode::MarkerWrite, DB::KeeperMoveError::FailedBeforeMarkerPublication},
        Case{MoveFailureMode::Copy, MoveFailureMode::None, DB::KeeperMoveError::MarkerPublishedCopyNotCompleted},
        Case{MoveFailureMode::None, MoveFailureMode::DestinationSize, DB::KeeperMoveError::CopyCompletedDestinationValidationFailed},
        Case{MoveFailureMode::None, MoveFailureMode::MarkerRemove, DB::KeeperMoveError::MarkerRemovalFailed},
        Case{MoveFailureMode::SourceRemove, MoveFailureMode::None, DB::KeeperMoveError::DestinationPublishedSourceRemovalFailed},
    };

    fs::create_directories("./tmp");
    size_t case_index = 0;
    for (const auto & test_case : cases)
    {
        SCOPED_TRACE(case_index);
        ChangelogDirTest root("./tmp/gtest_keeper_file_move_failure_" + std::to_string(case_index));
        fs::create_directories(root.path + "/source");
        fs::create_directories(root.path + "/destination");
        auto source = std::make_shared<FailingMoveDisk>("source", root.path + "/source", test_case.source_failure);
        auto destination = std::make_shared<FailingMoveDisk>("destination", root.path + "/destination", test_case.destination_failure);
        writeFile(source, "source", "authoritative");

        auto settings = std::make_shared<DB::CoordinationSettings>();
        (*settings)[DB::CoordinationSetting::disk_move_retries_during_init] = 1;
        (*settings)[DB::CoordinationSetting::disk_move_retries_wait_ms] = 0;
        auto keeper_context = makeKeeperContext(false, settings);
        bool callback_called = false;
        const auto result = DB::moveFileBetweenDisks(
            source,
            "source",
            destination,
            "destination",
            [&]
            {
                callback_called = true;
                return true;
            },
            getLogger("KeeperFileMoveTest"),
            keeper_context);

        ASSERT_FALSE(result);
        EXPECT_EQ(result.error(), test_case.expected);
        EXPECT_TRUE(source->existsFile("source"));
        EXPECT_EQ(callback_called, test_case.expected == DB::KeeperMoveError::DestinationPublishedSourceRemovalFailed);
        if (test_case.expected == DB::KeeperMoveError::DestinationPublishedSourceRemovalFailed)
        {
            EXPECT_TRUE(destination->existsFile("destination"));
            EXPECT_FALSE(destination->existsFile("tmp_destination"));
        }
        ++case_index;
    }
}

#endif
