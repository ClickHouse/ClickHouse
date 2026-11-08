#include <gtest/gtest.h>

#include <config.h>

#if USE_AVRO

#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>

#include <unistd.h>
#include <utime.h>

#include <atomic>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace fs = std::filesystem;
using namespace DB;

namespace DB::ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

struct ScopedTempDir
{
    fs::path path;

    explicit ScopedTempDir(const std::string & name)
        : path(fs::temp_directory_path() / (name + "_" + std::to_string(::getpid())))
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

void writeFile(const fs::path & path, const std::string & contents)
{
    fs::create_directories(path.parent_path());
    std::ofstream out;
    out.exceptions(std::ios::failbit | std::ios::badbit);
    out.open(path, std::ios::binary | std::ios::trunc);
    out << contents;
    out.close();
}

class CountingLocalObjectStorage : public LocalObjectStorage
{
public:
    using LocalObjectStorage::LocalObjectStorage;

    void listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t max_keys) const override
    {
        ++list_calls;
        LocalObjectStorage::listObjects(path, children, max_keys);
    }

    mutable std::atomic<size_t> list_calls = 0;
};

std::shared_ptr<CountingLocalObjectStorage> makeLocalObjectStorage(const fs::path & root)
{
    return std::make_shared<CountingLocalObjectStorage>(
        LocalObjectStorageSettings("test_paimon_schema_diagnostics", root.string(), /*read_only_=*/false));
}

}

TEST(PaimonSchemaDiagnostics, SchemaPrefixHoldsOnlyUnmatchedObject)
{
    ScopedTempDir temporary_directory("ch_gtest_paimon_schema_unmatched");
    auto table = temporary_directory.path / "test.db" / "test_table";
    const String schema_prefix = (table / Paimon::PAIMON_SCHEMA_DIR).string();
    const auto other = fs::path(schema_prefix) / "other.txt";
    writeFile(other, "");
    /// A fixed modification time, so the rendering asserted below is the object's own, not a wall clock.
    ::utimbuf times{};
    times.actime = 1000000000;
    times.modtime = 1000000000;
    ASSERT_EQ(::utime(other.c_str(), &times), 0);

    auto object_storage = makeLocalObjectStorage(temporary_directory.path);
    PaimonTableClient client(object_storage, table.string(), getContext().context);

    try
    {
        client.getLatestTableSchemaInfo();
        FAIL() << "Expected FILE_DOESNT_EXIST";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST);

        const String & message = e.message();
        EXPECT_NE(message.find("The metadata file for Paimon table with path"), String::npos) << message;
        EXPECT_NE(message.find("No schema file was found under " + schema_prefix + ","), String::npos) << message;

        EXPECT_NE(message.find("1 entry: /other.txt (2001-09-09T01:46:40Z)"), String::npos) << message;
    }

    /// The message must describe the listing that decided the throw, so no second one is issued.
    EXPECT_EQ(object_storage->list_calls.load(), 1u);
}

TEST(PaimonSchemaDiagnostics, SchemaPrefixEmpty)
{
    ScopedTempDir temporary_directory("ch_gtest_paimon_schema_empty");
    auto table = temporary_directory.path / "test.db" / "test_table";
    const String schema_prefix = (table / Paimon::PAIMON_SCHEMA_DIR).string();
    fs::create_directories(schema_prefix);

    auto object_storage = makeLocalObjectStorage(temporary_directory.path);
    PaimonTableClient client(object_storage, table.string(), getContext().context);

    try
    {
        client.getLatestTableSchemaInfo();
        FAIL() << "Expected FILE_DOESNT_EXIST";
    }
    catch (const DB::Exception & e)
    {
        EXPECT_EQ(e.code(), DB::ErrorCodes::FILE_DOESNT_EXIST);

        const String & message = e.message();
        EXPECT_NE(message.find("The metadata file for Paimon table with path"), String::npos) << message;
        EXPECT_NE(message.find("No schema file was found under " + schema_prefix + ", which held 0 entries"), String::npos)
            << message;
    }

    EXPECT_EQ(object_storage->list_calls.load(), 1u);
}

#endif
