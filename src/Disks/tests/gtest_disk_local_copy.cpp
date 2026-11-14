#include <gtest/gtest.h>

#include <Common/FailPoint.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadHelpers.h>
#include <Poco/TemporaryFile.h>
#include <base/scope_guard.h>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <mutex>
#include <stdexcept>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace std::chrono_literals;

namespace
{
class CopyCancelled : public std::runtime_error
{
public:
    CopyCancelled() : std::runtime_error("Copy cancelled") {}
};
}


class DiskLocalCopyTest : public testing::Test
{
protected:
    void SetUp() override
    {
        std::filesystem::create_directories(temp_dir.path() + "/source/");
        std::filesystem::create_directories(temp_dir.path() + "/destination/");
        source = std::make_shared<DB::DiskLocal>("source", temp_dir.path() + "/source/");
        destination = std::make_shared<DB::DiskLocal>("destination", temp_dir.path() + "/destination/");

        ASSERT_FALSE(read_settings.local_throttler);
        ASSERT_FALSE(write_settings.local_throttler);

        contents.resize(4 * DB::DBMS_DEFAULT_BUFFER_SIZE + 17);
        for (size_t i = 0; i < contents.size(); ++i)
            contents[i] = static_cast<char>(i % 251);

        source->createDirectories("part/");
        auto out = source->writeFile("part/data.bin");
        out->write(contents.data(), contents.size());
        out->finalize();
    }

    String readContents(const DB::DiskPtr & disk, const String & path = "part/data.bin") const
    {
        auto in = disk->readFile(path, read_settings);
        String result;
        DB::readStringUntilEOF(result, *in);
        return result;
    }

    void writeContents(const DB::DiskPtr & disk, const String & path, const String & data) const
    {
        auto out = disk->writeFile(path);
        out->write(data.data(), data.size());
        out->finalize();
    }

    Poco::TemporaryFile temp_dir{"tmp"};
    DB::DiskPtr source;
    DB::DiskPtr destination;
    DB::ReadSettings read_settings;
    DB::WriteSettings write_settings;
    String contents;
};


TEST_F(DiskLocalCopyTest, CancelsAfterCopyStarts)
{
    size_t hook_calls = 0;
    size_t copied_before_cancellation = 0;
    auto cancel_after_write = [&]
    {
        ++hook_calls;
        if (!destination->existsFile("part/data.bin"))
            return;
        copied_before_cancellation = destination->getFileSize("part/data.bin");
        if (copied_before_cancellation > 0)
            throw std::runtime_error("Copy cancelled");
    };

    EXPECT_THROW(
        source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, cancel_after_write),
        std::runtime_error);

    EXPECT_GT(hook_calls, 1);
    EXPECT_GT(copied_before_cancellation, 0);
    ASSERT_TRUE(destination->existsFile("part/data.bin"));
    EXPECT_LT(destination->getFileSize("part/data.bin"), contents.size());
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, CopiesWithCancellationHook)
{
    size_t hook_calls = 0;
    auto cancellation_hook = [&]
    {
        ++hook_calls;
    };
    source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, cancellation_hook);

    EXPECT_GT(hook_calls, 1);
    EXPECT_TRUE(readContents(destination) == contents);
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, CopiesWithoutCancellationHook)
{
    source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, {});

    EXPECT_TRUE(readContents(destination) == contents);
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, OverwritesWithEmptySource)
{
    source->truncateFile("part/data.bin", 0);
    destination->createDirectory("part");
    writeContents(destination, "part/data.bin", contents);

    source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {});

    EXPECT_EQ(destination->getFileSize("part/data.bin"), 0);
    EXPECT_TRUE(readContents(destination).empty());
    EXPECT_TRUE(readContents(source).empty());
}


TEST_F(DiskLocalCopyTest, RejectsSourceTruncatedDuringCopy)
{
    bool source_truncated = false;
    auto truncate_after_write = [&]
    {
        if (source_truncated || !destination->existsFile("part/data.bin"))
            return;
        const auto copied = destination->getFileSize("part/data.bin");
        if (copied == 0 || copied >= contents.size())
            return;
        source->truncateFile("part/data.bin", 0);
        source_truncated = true;
    };

    EXPECT_ANY_THROW(
        source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, truncate_after_write));

    ASSERT_TRUE(source_truncated);
    EXPECT_TRUE(readContents(source).empty());
    const auto copied = readContents(destination);
    EXPECT_GT(copied.size(), 0);
    EXPECT_LT(copied.size(), contents.size());
    EXPECT_TRUE(copied == contents.substr(0, copied.size()));
}


TEST_F(DiskLocalCopyTest, PreservesRestrictionsDuringCopy)
{
    struct Permissions
    {
        mode_t directory;
        mode_t file;
    };
    for (const auto & permissions : {Permissions{0750, 0600}, Permissions{0700, 0640}})
    {
        source->chmod("part", permissions.directory);
        source->chmod("part/data.bin", permissions.file);
        for (const bool cancel : {false, true})
        {
            SCOPED_TRACE(::testing::Message() << "directory_mode=" << permissions.directory << " cancel=" << cancel);
            const String directory = "permissions_" + std::to_string(permissions.directory) + (cancel ? "_cancel/" : "_complete/");
            const String filename = directory + "data.bin";
            auto check_permissions = [&]
            {
                EXPECT_EQ(destination->stat(directory).st_mode & 0777 & ~permissions.directory, 0);
                EXPECT_EQ(destination->stat(filename).st_mode & 0777 & ~permissions.file, 0);
            };
            bool observed_partial_copy = false;
            auto cancellation_hook = [&]
            {
                if (observed_partial_copy || !destination->existsFile(filename) || destination->getFileSize(filename) == 0)
                    return;
                observed_partial_copy = true;
                EXPECT_LT(destination->getFileSize(filename), contents.size());
                check_permissions();
                if (cancel)
                    throw CopyCancelled();
            };
            if (cancel)
            {
                EXPECT_THROW(
                    source->copyDirectoryContent("part/", destination, directory, read_settings, write_settings, cancellation_hook),
                    CopyCancelled);
            }
            else
            {
                source->copyDirectoryContent("part/", destination, directory, read_settings, write_settings, cancellation_hook);
            }
            EXPECT_TRUE(observed_partial_copy);
            check_permissions();
            const auto copied = readContents(destination, filename);
            if (cancel)
            {
                EXPECT_GT(copied.size(), 0);
                EXPECT_LT(copied.size(), contents.size());
                EXPECT_TRUE(copied == contents.substr(0, copied.size()));
            }
            else
            {
                EXPECT_TRUE(copied == contents);
            }
            EXPECT_EQ(source->stat("part").st_mode & 0777, permissions.directory);
            EXPECT_EQ(source->stat("part/data.bin").st_mode & 0777, permissions.file);
            EXPECT_TRUE(readContents(source) == contents);
        }
    }
}


TEST_F(DiskLocalCopyTest, OverwritesWithoutChangingReadonlySourceOrBackup)
{
    fs::create_hard_link(source->getPath() + "part/data.bin", source->getPath() + "backup.bin");
    source->chmod("part/data.bin", 0440);
    const auto original_source = source->stat("part/data.bin");
    destination->createDirectory("part");
    destination->chmod("part", 0700);
    writeContents(destination, "part/data.bin", contents + "trailing bytes");
    destination->chmod("part/data.bin", 0640);
    const auto original_destination = destination->stat("part/data.bin");
    bool observed_partial_copy = false;
    auto check_permissions_after_write = [&]
    {
        const auto copied = destination->getFileSize("part/data.bin");
        if (copied > 0 && copied < contents.size())
        {
            observed_partial_copy = true;
            EXPECT_EQ(destination->stat("part/data.bin").st_mode & 0777, 0440);
        }
    };

    source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, check_permissions_after_write);

    EXPECT_TRUE(observed_partial_copy);
    EXPECT_TRUE(readContents(destination) == contents);
    EXPECT_EQ(destination->stat("part/data.bin").st_mode & 0777, 0440);
    EXPECT_EQ(destination->stat("part/data.bin").st_ino, original_destination.st_ino);
    EXPECT_EQ(destination->stat("part").st_mode & 0777, 0700);

    source->copyDirectoryContent("part/", destination, "readonly/", read_settings, write_settings, [] {});
    EXPECT_TRUE(readContents(destination, "readonly/data.bin") == contents);
    EXPECT_EQ(destination->stat("readonly/data.bin").st_mode & 0777 & ~0440, 0);
    EXPECT_EQ(source->stat("part/data.bin").st_ino, original_source.st_ino);
    EXPECT_EQ(source->stat("part/data.bin").st_mode & 0777, 0440);
    EXPECT_EQ(source->stat("backup.bin").st_mode & 0777, 0440);
    EXPECT_TRUE(readContents(source) == contents);
    EXPECT_TRUE(readContents(source, "backup.bin") == contents);
}


#if USE_LIBFIU
TEST_F(DiskLocalCopyTest, OverwritesDestinationCreatedDuringCopy)
{
    destination->createDirectory("part");
    ASSERT_FALSE(destination->existsFile("part/data.bin"));
    const String fail_point = "copy_local_file_pause_before_open";
    DB::FailPointInjection::enableFailPoint(fail_point);
    std::future<void> copy;
    SCOPE_EXIT({
        DB::FailPointInjection::disableFailPoint(fail_point);
        if (copy.valid())
            copy.wait();
    });
    copy = std::async(std::launch::async, [&]
    {
        source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {});
    });

    DB::FailPointInjection::waitForPause(fail_point);
    ASSERT_FALSE(destination->existsFile("part/data.bin"));
    writeContents(destination, "part/data.bin", contents + "trailing bytes");
    DB::FailPointInjection::disableFailPoint(fail_point);
    copy.get();

    EXPECT_EQ(destination->getFileSize("part/data.bin"), contents.size());
    EXPECT_TRUE(readContents(destination) == contents);
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, RejectsDestinationLinkedToSourceDuringCopy)
{
    destination->createDirectory("part");
    ASSERT_FALSE(destination->existsFile("part/data.bin"));
    const auto original_source = source->stat("part/data.bin");
    const String fail_point = "copy_local_file_pause_before_open";
    DB::FailPointInjection::enableFailPoint(fail_point);
    std::future<void> copy;
    SCOPE_EXIT({
        DB::FailPointInjection::disableFailPoint(fail_point);
        if (copy.valid())
            copy.wait();
    });
    copy = std::async(std::launch::async, [&]
    {
        source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {});
    });

    DB::FailPointInjection::waitForPause(fail_point);
    fs::create_hard_link(source->getPath() + "part/data.bin", destination->getPath() + "part/data.bin");
    DB::FailPointInjection::disableFailPoint(fail_point);
    EXPECT_THROW(copy.get(), fs::filesystem_error);

    EXPECT_TRUE(readContents(source) == contents);
    EXPECT_TRUE(readContents(destination) == contents);
    EXPECT_EQ(source->stat("part/data.bin").st_mode, original_source.st_mode);
    EXPECT_EQ(source->stat("part/data.bin").st_ino, original_source.st_ino);
}
#endif


TEST_F(DiskLocalCopyTest, CopiesNestedAndEmptyDirectories)
{
    source->createDirectories("part/nested/empty/");
    source->chmod("part/nested", 0700);
    source->chmod("part/nested/empty", 0700);
    writeContents(source, "part/nested/data.bin", contents);
    source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {});

    EXPECT_TRUE(destination->existsDirectory("part/nested/empty/"));
    EXPECT_EQ(destination->stat("part/nested").st_mode & 0777, 0700);
    EXPECT_EQ(destination->stat("part/nested/empty").st_mode & 0777, 0700);
    EXPECT_TRUE(readContents(destination, "part/nested/data.bin") == contents);
    EXPECT_TRUE(readContents(source, "part/nested/data.bin") == contents);
    EXPECT_TRUE(readContents(destination) == contents);
}


TEST_F(DiskLocalCopyTest, FollowsSourceAndDestinationSymlinks)
{
    fs::create_symlink("data.bin", source->getPath() + "part/link.bin");
    fs::create_directory_symlink("part", source->getPath() + "part_link");
    destination->createDirectory("files");
    fs::create_directory_symlink("files", destination->getPath() + "part");
    writeContents(destination, "existing.bin", "previous contents");
    fs::create_symlink("../existing.bin", destination->getPath() + "files/data.bin");
    fs::create_symlink("../absent.bin", destination->getPath() + "files/link.bin");

    source->copyDirectoryContent("part_link/", destination, "part/", read_settings, write_settings, [] {});

    EXPECT_TRUE(readContents(destination, "existing.bin") == contents);
    EXPECT_TRUE(readContents(destination, "absent.bin") == contents);
    EXPECT_TRUE(fs::is_symlink(destination->getPath() + "part"));
    EXPECT_TRUE(fs::is_symlink(destination->getPath() + "files/data.bin"));
    EXPECT_TRUE(fs::is_symlink(destination->getPath() + "files/link.bin"));
    EXPECT_TRUE(fs::is_symlink(source->getPath() + "part/link.bin"));
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, CopiesFileIntoDirectory)
{
    destination->createDirectory("part");
    source->copyDirectoryContent("part/data.bin", destination, "part/", read_settings, write_settings, [] {});
    EXPECT_TRUE(readContents(destination) == contents);
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, RejectsSourceAsDestination)
{
    const auto original = source->stat("part/data.bin");
    EXPECT_ANY_THROW(source->copyDirectoryContent("part/data.bin", source, "part/data.bin", read_settings, write_settings, [] {}));
    EXPECT_TRUE(readContents(source) == contents);
    EXPECT_EQ(source->stat("part/data.bin").st_mode, original.st_mode);
    EXPECT_EQ(source->stat("part/data.bin").st_ino, original.st_ino);
}


TEST_F(DiskLocalCopyTest, RejectsDestinationLinkedToSource)
{
    destination->createDirectory("part");
    fs::create_hard_link(source->getPath() + "part/data.bin", destination->getPath() + "part/data.bin");
    const auto original = source->stat("part/data.bin");
    EXPECT_ANY_THROW(source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {}));
    EXPECT_TRUE(readContents(source) == contents);
    EXPECT_EQ(source->stat("part/data.bin").st_mode, original.st_mode);
    EXPECT_EQ(source->stat("part/data.bin").st_ino, original.st_ino);
    EXPECT_TRUE(readContents(destination) == contents);
}


TEST_F(DiskLocalCopyTest, RejectsDestinationSymlinkToSource)
{
    destination->createDirectory("part");
    fs::create_symlink(fs::absolute(source->getPath() + "part/data.bin"), destination->getPath() + "part/data.bin");
    const auto original = source->stat("part/data.bin");
    EXPECT_ANY_THROW(source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {}));
    EXPECT_TRUE(readContents(source) == contents);
    EXPECT_EQ(source->stat("part/data.bin").st_mode, original.st_mode);
    EXPECT_EQ(source->stat("part/data.bin").st_ino, original.st_ino);
}


TEST_F(DiskLocalCopyTest, RejectsFifoSource)
{
    ASSERT_EQ(mkfifo((source->getPath() + "fifo").c_str(), 0600), 0);
    EXPECT_ANY_THROW(source->copyDirectoryContent("fifo", destination, "fifo", read_settings, write_settings, [] {}));
    EXPECT_TRUE(readContents(source) == contents);
}


TEST_F(DiskLocalCopyTest, RejectsFifoDestination)
{
    writeContents(source, "part/data.bin", "payload");
    destination->createDirectory("part");
    const auto filename = destination->getPath() + "part/data.bin";
    ASSERT_EQ(mkfifo(filename.c_str(), 0600), 0);
    const int fifo = open(filename.c_str(), O_RDWR | O_NONBLOCK | O_CLOEXEC);
    ASSERT_NE(fifo, -1);
    SCOPE_EXIT({ close(fifo); });

    EXPECT_ANY_THROW(source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, [] {}));
    EXPECT_EQ(readContents(source), "payload");
    char buffer[16];
    EXPECT_EQ(read(fifo, buffer, sizeof(buffer)), -1);
    EXPECT_EQ(errno, EAGAIN);
}


TEST_F(DiskLocalCopyTest, CancelsWhileSharedCopierPoolIsOccupied)
{
    constexpr size_t workers = 16;
    std::mutex mutex;
    std::condition_variable condition;
    size_t paused = 0;
    bool released = false;
    bool cancelled = false;
    size_t target_copied_bytes = 0;
    std::vector<std::future<void>> background;
    std::future<void> target;
    SCOPE_EXIT({
        {
            std::lock_guard lock(mutex);
            released = true;
            cancelled = true;
        }
        condition.notify_all();
        for (auto & copy : background)
            if (copy.valid())
                copy.wait();
        if (target.valid())
            target.wait();
    });

    for (size_t i = 0; i < workers; ++i)
    {
        const String directory = "background_" + std::to_string(i) + "/";
        source->createDirectory(directory);
        writeContents(source, directory + "data.bin", contents);
        background.emplace_back(std::async(std::launch::async, [&, directory]
        {
            bool held = false;
            auto hold_after_write = [&]
            {
                const String filename = directory + "data.bin";
                if (held || !destination->existsFile(filename) || destination->getFileSize(filename) == 0)
                    return;
                EXPECT_LT(destination->getFileSize(filename), contents.size());
                std::unique_lock lock(mutex);
                held = true;
                ++paused;
                condition.notify_all();
                condition.wait(lock, [&] { return released; });
            };
            source->DB::IDisk::copyDirectoryContent(directory, destination, directory, read_settings, write_settings, hold_after_write);
        }));
    }
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(condition.wait_for(lock, 20s, [&] { return paused == workers; }));
    }
    target = std::async(std::launch::async, [&]
    {
        auto cancellation_hook = [&]
        {
            if (!destination->existsFile("part/data.bin"))
                return;
            const auto copied = destination->getFileSize("part/data.bin");
            if (copied == 0 || copied >= contents.size())
                return;
            std::unique_lock lock(mutex);
            target_copied_bytes = copied;
            condition.notify_all();
            condition.wait(lock, [&] { return cancelled; });
            throw CopyCancelled();
        };
        source->copyDirectoryContent("part/", destination, "part/", read_settings, write_settings, cancellation_hook);
    });
    {
        std::unique_lock lock(mutex);
        ASSERT_TRUE(condition.wait_for(lock, 20s, [&] { return target_copied_bytes > 0; }))
            << "Target did not write while the shared copier workers remained occupied";
        EXPECT_LT(target_copied_bytes, contents.size());
        EXPECT_FALSE(released);
        EXPECT_EQ(paused, workers);
        cancelled = true;
    }
    condition.notify_all();
    EXPECT_EQ(target.wait_for(3s), std::future_status::ready);
    {
        std::lock_guard lock(mutex);
        EXPECT_FALSE(released);
        EXPECT_EQ(paused, workers);
        released = true;
    }
    condition.notify_all();
    EXPECT_THROW(target.get(), CopyCancelled);
    for (auto & copy : background)
        copy.get();

    for (size_t i = 0; i < workers; ++i)
    {
        const String filename = "background_" + std::to_string(i) + "/data.bin";
        EXPECT_TRUE(readContents(source, filename) == contents);
        EXPECT_TRUE(readContents(destination, filename) == contents);
    }
    EXPECT_TRUE(readContents(source) == contents);
    ASSERT_TRUE(destination->existsFile("part/data.bin"));
    const auto copied = readContents(destination);
    EXPECT_GT(copied.size(), 0);
    EXPECT_LT(copied.size(), contents.size());
    EXPECT_TRUE(copied == contents.substr(0, copied.size()));
}
