#include <gtest/gtest.h>

#include <Disks/DiskFactory.h>
#include <Disks/registerDisks.h>
#include <Disks/IDiskTransaction.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBuffer.h>

#include "Common/Exception.h"
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/FailPoint.h>
#include <Core/Defines.h>

#include <string>

namespace fs = std::filesystem;

namespace
{

void setUpConfig(const std::string & file_name)
{
    std::string content = R"(
<clickhouse>
    <logger>
        <level>trace</level>
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
            <local_object_storage_disk>
                <type>object_storage</type>
                <object_storage_type>local_blob_storage</object_storage_type>
                <path>local_blob_storage_dir/</path>
                <metadata_type>local</metadata_type>
                <use_fake_transaction>false</use_fake_transaction>
            </local_object_storage_disk>
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

class DiskObjectStorageTest : public testing::Test
{
public:
    static const std::string config_path;

    static void SetUpTestSuite()
    {
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
        std::string name = "local_object_storage_disk";
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
        removeAll();
    }

private:
    static DB::DisksMap initialized_disks;
};

const std::string DiskObjectStorageTest::config_path = "./config_file_for_test.xml";
DB::DisksMap DiskObjectStorageTest::initialized_disks = {};

TEST_F(DiskObjectStorageTest, CreateDisk)
{
    auto disk = getDiskObjectStorage();
    EXPECT_TRUE(disk->isDisk());
    EXPECT_EQ(disk->getName(), "local_object_storage_disk");
    EXPECT_EQ(disk->getPath(), "./disks/local_object_storage_disk/");

    EXPECT_EQ(listAllBlobs(disk).size(), 0);
}

TEST_F(DiskObjectStorageTest, WriteListReadFile)
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

TEST_F(DiskObjectStorageTest, WriteFileTxCommit)
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

TEST_F(DiskObjectStorageTest, WriteFileTxUndo)
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

TEST_F(DiskObjectStorageTest, RewriteFile)
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

TEST_F(DiskObjectStorageTest, RewriteFileUndo)
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

TEST_F(DiskObjectStorageTest, RewriteFileTxCommitFail)
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

TEST_F(DiskObjectStorageTest, MoveAndRewriteFile)
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
    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageTest, MoveAndRewriteFileTxUndo)
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

TEST_F(DiskObjectStorageTest, MoveAndRewriteFileTxCommitFail)
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

TEST_F(DiskObjectStorageTest, HardLinkAndRewriteFile)
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

    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}

TEST_F(DiskObjectStorageTest, HardLinkAndRewriteFileTxUndo)
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

TEST_F(DiskObjectStorageTest, HardLinkAndRewriteFileTxCommitFail)
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

TEST_F(DiskObjectStorageTest, TruncateFileToZero)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";

    disk->truncateFile(file_name, 0);
    EXPECT_FALSE(disk->existsFile(file_name));

    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 1);

    {
        auto tx = disk->createTransaction();

        tx->truncateFile(file_name, 0);

        tx->commit();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), "");

    EXPECT_EQ(listAllBlobs(disk).size(), 0);
}

TEST_F(DiskObjectStorageTest, TruncateFileToZeroInsideTx)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";

    std::string file_content = getTestName() + "_file_context";

    {
        auto tx = disk->createTransaction();

        {
            auto wb = tx->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Rewrite, DB::WriteSettings{});
            DB::writeText(file_content, *wb);
            wb->finalize();
        }

        tx->truncateFile(file_name, 0);

        tx->commit();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), "");
    EXPECT_EQ(listAllBlobs(disk).size(), 0);
}

TEST_F(DiskObjectStorageTest, TruncateFileToNotZero)
{
    auto disk = getDiskObjectStorage();

    std::string file_name = getTestName() + "_file";
    std::string file_content = getTestName() + "_file_context";

    {
        auto wb = disk->writeFile(file_name);
        DB::writeText(file_content, *wb);
        wb->finalize();
    }

    std::string appended_file_content = getTestName() + "_rewritten_file_context";

    {
        auto wb = disk->writeFile(file_name, DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Append, DB::WriteSettings{});
        DB::writeText(appended_file_content, *wb);
        wb->finalize();
    }

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content + appended_file_content);
    EXPECT_EQ(listAllBlobs(disk).size(), 2);

    disk->truncateFile(file_name, file_content.size());

    EXPECT_TRUE(disk->existsFile(file_name));
    EXPECT_EQ(readAll(*disk->readFile(file_name, {})), file_content);

    EXPECT_EQ(listAllBlobs(disk).size(), 1);
}
