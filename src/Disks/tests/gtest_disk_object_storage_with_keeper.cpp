#include <fmt/ranges.h>
#include <gtest/gtest.h>

#include <Disks/DiskFactory.h>
#include <Disks/registerDisks.h>
#include <Disks/IDiskTransaction.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBuffer.h>

#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/FailPoint.h>
#include <Core/ServerUUID.h>
#include <Core/Defines.h>

#include <string>

namespace fs = std::filesystem;

namespace
{

void setUpConfig(const std::string & file_name)
{
    std::string content = R"(
<clickhouse>
    <zookeeper>
        <implementation>testkeeper</implementation>
    </zookeeper>

    <logger>
        <level>test</level>
        <console>true</console>
    </logger>

    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <mysql_port>9004</mysql_port>

    <path>./</path>

    <mlock_executable>true</mlock_executable>

    <users>
        <default>
            <password></password>

            <networks>
                <ip>::/0</ip>
            </networks>

            <profile>default</profile>
            <quota>default</quota>
            <access_management>1</access_management>
        </default>
    </users>

    <profiles>
        <default/>
    </profiles>

    <quotas>
        <default/>
    </quotas>

    <storage_configuration>
        <disks>
            <local_metadata_with_keeper>
                <type>object_storage</type>

                <object_storage_type>local</object_storage_type>
                <path>local_blob_storage_dir/</path>

                <metadata_type>keeper</metadata_type>

                <metadata_cache_enabled>1</metadata_cache_enabled>
                <metadata_cache_full_directory_lists>1</metadata_cache_full_directory_lists>
                <metadata_cache_cleanup_interval>0</metadata_cache_cleanup_interval>
                <metadata_background_cleanup>
                    <enabled>true</enabled>
                    <!-- Set VERY aggressive deleted objects cleanups -->
                    <deleted_objects_delay_sec>0</deleted_objects_delay_sec>
                    <old_transactions_delay_sec>100</old_transactions_delay_sec>
                    <interval_sec>0</interval_sec>
                </metadata_background_cleanup>
            </local_metadata_with_keeper>
        </disks>
    </storage_configuration>

</clickhouse>
)";

    if (fs::exists(file_name))
        fs::remove_all(file_name);

    {
        DB::WriteBufferFromFile wb(file_name);
        DB::writeText(content, wb);
        wb.finalize();
    }
}

std::string readAll(DB::ReadBuffer & rb)
{
    std::string str;
    DB::readStringUntilEOF(str, rb);
    return str;
}

}

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FailPoints
{
    extern const char disk_object_storage_fail_commit_metadata_transaction[];
}

}

class DiskObjectStorageWithKeeperTest : public testing::Test
{
public:
    static const std::string config_path;

    static void SetUpTestSuite()
    {
        DB::ServerUUID::setRandomForUnitTests();

        setUpConfig(config_path);
        DB::ConfigProcessor config_processor(config_path, true, true);
        auto config = config_processor.loadConfig(false);
        getContext().context->setConfig(config.configuration);

        DB::registerDisks(/*global_skip_access_check*/ true);
    }

    static void removeAll()
    {
         for (const auto & [_, disk] : initialized_disks)
         {
            std::vector<String> file_names;
            disk->listFiles(".", file_names);

            for (const auto & name : file_names)
                disk->removeRecursive(name);
         }
    }

    void removeAllWithOrphanBlobs()
    {
         for (const auto & [_, disk] : initialized_disks)
         {
            std::vector<String> file_names;
            disk->listFiles(".", file_names);

            for (const auto & name : file_names)
                disk->removeRecursive(name);

            /// Also remove all blobs from object storage
            /// This test suite leaves some garbage in object storage after itself
            /// due to known issue with transactions and metadata keeper.
            auto blobs = listAllBlobs(disk);
            for (const auto & blob : blobs)
            {
                DB::ObjectStoragePtr object_storage = disk->getObjectStorage();
                object_storage->removeObjectIfExists(DB::StoredObject(blob));
            }
         }
    }

    std::set<std::string> listAllBlobs(DB::DiskPtr disk)
    {
        DB::ObjectStoragePtr object_storage = disk->getObjectStorage();

        DB::RelativePathsWithMetadata children;
        auto common_key_prefix = fs::path(object_storage->getCommonKeyPrefix()) / "";
        object_storage->listObjects(common_key_prefix, children, /* max_keys */ 0);

        std::set<std::string> blobs;
        for (const auto & child : children)
            blobs.insert(child->relative_path);
        return blobs;
    }

    static void TearDownTestSuite()
    {
        removeAll();

        for (const auto & [_, disk] : initialized_disks)
            disk->shutdown();
        initialized_disks.clear();

        // other tests may also register disks, so we need to clear the registry
        DB::clearDiskRegistry();
    }

    std::string getTestName()
    {
        const auto * const test_info = testing::UnitTest::GetInstance()->current_test_info();
        return test_info->name();
    }

    DB::DiskPtr getDiskObjectStorage()
    {
        if (!initialized_disks.empty())
            return initialized_disks.begin()->second;

        auto & factory = DB::DiskFactory::instance();
        std::string name = "local_metadata_with_keeper";
        std::string prefix = "storage_configuration.disks." + name;
        auto disk = factory.create(
            name,
            getContext().context->getConfigRef(),
            prefix,
            getContext().context,
            initialized_disks,
            /*attach*/ false,
            /*custom_disk*/ true,
            /*skip_types*/ {});

        initialized_disks.emplace(name, disk);

        return disk;
    }

    void TearDown() override
    {
        removeAllWithOrphanBlobs();
    }

private:
    static DB::DisksMap initialized_disks;
};

const std::string DiskObjectStorageWithKeeperTest::config_path = "./config_file_for_test.xml";
DB::DisksMap DiskObjectStorageWithKeeperTest::initialized_disks = {};

TEST_F(DiskObjectStorageWithKeeperTest, CreateDisk)
{
    auto disk = getDiskObjectStorage();
    EXPECT_TRUE(disk->isDisk());
    EXPECT_EQ(disk->getName(), "local_metadata_with_keeper");
    EXPECT_EQ(disk->getPath(), "");

    EXPECT_EQ(listAllBlobs(disk).size(), 0);

    std::string blobs = fmt::format("{}", fmt::join(listAllBlobs(disk), ", "));
    EXPECT_EQ(blobs, "");
}

TEST_F(DiskObjectStorageWithKeeperTest, WriteListReadFile)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));

    std::vector<String> files;
    disk->listFiles(".", files);

    EXPECT_EQ(files.size(), 1);
    EXPECT_EQ(files, std::vector<String>{file_name});

    EXPECT_EQ(disk->getFileSize(file_name), file_content.size());

    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    disk->removeFile(file_name);
    EXPECT_EQ(listAllBlobs(disk).size(), 0);
}

TEST_F(DiskObjectStorageWithKeeperTest, WriteFileTxCommit)
{
    auto disk = getDiskObjectStorage();

    auto tx = disk->createTransaction();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = tx->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    tx->commit();

    EXPECT_TRUE(disk->existsFile(file_name));

    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, WriteFileTxUndo)
{
    auto disk = getDiskObjectStorage();

    auto tx = disk->createTransaction();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = tx->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    tx->undo();

    EXPECT_FALSE(disk->existsFile(file_name));
    EXPECT_EQ(listAllBlobs(disk).size(), 0);
}

TEST_F(DiskObjectStorageWithKeeperTest, RewriteFile)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";

    {
        auto tx = disk->createTransaction();

        {
            auto wb = tx->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
            DB::writeText(rewrite_file_content, *wb);
            wb->finalize();
        }

        tx->commit();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), rewrite_file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, RewriteFileUndo)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";

    {
        auto tx = disk->createTransaction();

        {
            auto wb = tx->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
            DB::writeText(rewrite_file_content, *wb);
            wb->finalize();
        }

        tx->undo();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, RewriteFileTxCommitFail)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";

    auto tx = disk->createTransaction();

    {
        auto wb = tx->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();
    }

    DB::FailPointInjection::enableFailPoint("disk_object_storage_fail_commit_metadata_transaction");

    EXPECT_THROW(tx->commit(), DB::Exception);

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, MoveAndRewriteFile)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";


    std::string new_file_name = getTestName() + "_file_moved";
    {
        auto tx = disk->createTransaction();

        tx->moveFile(file_name, new_file_name);

        auto wb = tx->writeFile(new_file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();

        tx->commit();
    }

    EXPECT_TRUE(disk->existsFile(new_file_name));
    EXPECT_EQ(readAll(*disk->readFile(new_file_name, {})), rewrite_file_content);
    /// It should be 1 blob here.
    /// But keeper metadata storage unable to remove old object in transaction
    /// when new object created without proving set of blobs, it is operations like move and hardlink.
    EXPECT_EQ(listAllBlobs(disk).size(), 2);
}

TEST_F(DiskObjectStorageWithKeeperTest, MoveAndRewriteFileTxUndo)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";


    std::string new_file_name = getTestName() + "_file_moved";
    {
        auto tx = disk->createTransaction();

        tx->moveFile(file_name, new_file_name);

        auto wb = tx->writeFile(new_file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();

        tx->undo();
    }

    EXPECT_FALSE(disk->existsFile(new_file_name));
    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, MoveAndRewriteFileTxCommitFail)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";


    std::string new_file_name = getTestName() + "_file_moved";
    {
        auto tx = disk->createTransaction();

        tx->moveFile(file_name, new_file_name);

        auto wb = tx->writeFile(new_file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();

        DB::FailPointInjection::enableFailPoint("disk_object_storage_fail_commit_metadata_transaction");
        EXPECT_THROW(tx->commit(), DB::Exception);
    }

    EXPECT_FALSE(disk->existsFile(new_file_name));
    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, HardLinkAndRewriteFile)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";


    std::string new_file_name = getTestName() + "_file_linked";
    {
        auto tx = disk->createTransaction();

        tx->createHardLink(file_name, new_file_name);

        auto wb = tx->writeFile(new_file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();

        tx->commit();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), rewrite_file_content);

    EXPECT_TRUE(disk->existsFile(new_file_name));
    EXPECT_EQ(readAll(*disk->readFile(new_file_name, {})), rewrite_file_content);

    /// It should be 1 blob here.
    /// But keeper metadata storage unable to remove old object in transaction
    /// when new object created without proving set of blobs, it is operations like move and hardlink.
    EXPECT_EQ(listAllBlobs(disk).size(), 2);
}

TEST_F(DiskObjectStorageWithKeeperTest, HardLinkAndRewriteFileTxUndo)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";


    std::string new_file_name = getTestName() + "_file_linked";
    {
        auto tx = disk->createTransaction();

        tx->createHardLink(file_name, new_file_name);

        auto wb = tx->writeFile(new_file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();

        tx->undo();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);

    EXPECT_FALSE(disk->existsFile(new_file_name));

    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageWithKeeperTest, HardLinkAndRewriteFileTxCommitFail)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    std::string rewrite_file_content = getTestName() + "_rewritten_file_context";


    std::string new_file_name = getTestName() + "_file_linked";
    {
        auto tx = disk->createTransaction();

        tx->createHardLink(file_name, new_file_name);

        auto wb = tx->writeFile(new_file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
        DB::writeText(rewrite_file_content, *wb);
        wb->finalize();

        DB::FailPointInjection::enableFailPoint("disk_object_storage_fail_commit_metadata_transaction");
        EXPECT_THROW(tx->commit(), DB::Exception);
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);

    EXPECT_FALSE(disk->existsFile(new_file_name));

    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}
