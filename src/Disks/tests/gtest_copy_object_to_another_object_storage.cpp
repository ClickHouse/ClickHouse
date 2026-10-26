#include <gtest/gtest.h>

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Core/Defines.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>

#include <unistd.h> /// for ::getpid

#include <filesystem>
#include <memory>
#include <string>

namespace fs = std::filesystem;

namespace
{

/// A scoped temp directory that cleans itself up on destruction.
struct ScopedTempDir
{
    fs::path path;
    explicit ScopedTempDir(const std::string & name_hint)
        : path(fs::temp_directory_path() / fs::path(name_hint + "_" + std::to_string(::getpid())))
    {
        std::error_code ec;
        fs::remove_all(path, ec);
        fs::create_directories(path);
    }
    ~ScopedTempDir()
    {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

/// A local object storage that counts how many times an object is opened for reading and for
/// writing, so a test can tell how many times a copy actually transferred the data.
class CountingLocalObjectStorage : public DB::LocalObjectStorage
{
public:
    using LocalObjectStorage::LocalObjectStorage;

    std::unique_ptr<DB::ReadBufferFromFileBase> readObject( /// NOLINT
        const DB::StoredObject & object,
        const DB::ReadSettings & read_settings,
        std::optional<size_t> read_hint = {},
        bool use_external_buffer = false,
        bool restrict_seek = false) const override
    {
        ++reads;
        return LocalObjectStorage::readObject(object, read_settings, read_hint, use_external_buffer, restrict_seek);
    }

    std::unique_ptr<DB::WriteBufferFromFileBase> writeObject( /// NOLINT
        const DB::StoredObject & object,
        DB::WriteMode mode,
        std::optional<DB::ObjectAttributes> attributes = {},
        size_t buf_size = DB::DBMS_DEFAULT_BUFFER_SIZE,
        const DB::WriteSettings & write_settings = {}) override
    {
        ++writes;
        return LocalObjectStorage::writeObject(object, mode, attributes, buf_size, write_settings);
    }

    mutable size_t reads = 0;
    size_t writes = 0;
};

}

/// `copyObjectToAnotherObjectStorage` with the same storage as the destination is a plain
/// `copyObject`: the data must be transferred exactly once, not copied natively and then read and
/// written over again through buffers.
TEST(CopyObjectToAnotherObjectStorage, SameStorageCopiesOnce)
{
    ScopedTempDir dir("copy_object_to_another_object_storage");
    DB::LocalObjectStorageSettings settings(/*disk_name_=*/"test_local", /*key_prefix_=*/dir.path.string() + "/", /*read_only_=*/false);
    CountingLocalObjectStorage storage(std::move(settings));

    const DB::StoredObject source((dir.path / "source").string());
    const DB::StoredObject destination((dir.path / "destination").string());

    {
        auto out = storage.writeObject(source, DB::WriteMode::Rewrite, {}, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteSettings{});
        out->write("payload", 7);
        out->finalize();
    }
    storage.reads = 0;
    storage.writes = 0;

    storage.copyObjectToAnotherObjectStorage(source, destination, DB::ReadSettings{}, DB::WriteSettings{}, storage);

    /// The local `copyObject` reads the source once and writes the destination once.
    ASSERT_EQ(storage.reads, 1u);
    ASSERT_EQ(storage.writes, 1u);

    std::string copied;
    {
        auto in = storage.readObject(destination, DB::ReadSettings{});
        DB::readStringUntilEOF(copied, *in);
    }
    ASSERT_EQ(copied, "payload");
}
