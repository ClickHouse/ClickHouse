#include <filesystem>
#include <Disks/DiskLocal.h>
#include <Disks/DiskOverlay.h>
#include <Disks/IDisk.h>
#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <gtest/gtest.h>
#include <Disks/ObjectStorages/IObjectStorage_fwd.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Core/Defines.h>
#include <Disks/WriteMode.h>

using DB::DiskPtr, DB::MetadataStoragePtr;

DB::DiskPtr createLocalDisk(const std::string & path)
{
    fs::create_directory(path);
    return std::make_shared<DB::DiskLocal>("local_disk", path);
}

DB::MetadataStoragePtr createLocalDiskMetaData(const std::string & path)
{
    return std::make_shared<DB::MetadataStorageFromDisk>(createLocalDisk(path), "...");
}

class OverlayTest : public testing::Test {
public:
    DiskPtr base, diff, over;
    MetadataStoragePtr meta, tr_meta;

    void SetUp() override {
        fs::create_directories("tmp/test_overlay");

        base = createLocalDisk("tmp/test_overlay/base");
        diff = createLocalDisk("tmp/test_overlay/over");

        meta = createLocalDiskMetaData("tmp/test_overlay/meta");
        tr_meta = createLocalDiskMetaData("tmp/test_overlay/tr_meta");

        over = std::make_shared<DB::DiskOverlay>("disk_overlay", base, diff, meta, tr_meta);
    }

    void TearDown() override {
        fs::remove_all("tmp/test_overlay");
    }

    void writeToFileBase(const String& path, const String& text) const {
        std::unique_ptr<DB::WriteBuffer> out = base->writeFile(path);
        writeString(text, *out);
    }

    void writeToFileOver(const String& path, const String& text, bool append) const {
        std::unique_ptr<DB::WriteBuffer> out = over->writeFile(path, DB::DBMS_DEFAULT_BUFFER_SIZE, append ? DB::WriteMode::Append : DB::WriteMode::Rewrite);
        writeString(text, *out);
    }

    String readFromFileOver(const String& path) const {
        String result;
        std::unique_ptr<DB::ReadBuffer> in = over->readFile(path);
        readString(result, *in);
        return result;
    }
};

TEST_F(OverlayTest, createRemoveFile)
{
    base->createFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), true);

    over->removeFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), false);

    over->createFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), true);

    over->removeFile("file.txt");
    EXPECT_EQ(over->exists("file.txt"), false);
}

TEST_F(OverlayTest, listFiles)
{
    base->createDirectory("folder");
    base->createFile("folder/file1.txt");
    
    over->createFile("folder/file2.txt");

    std::vector<String> paths, corr({"file1.txt", "file2.txt"});
    over->listFiles("folder", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);

    over->writeFile("folder/file1.txt", DB::DBMS_DEFAULT_BUFFER_SIZE, DB::WriteMode::Append);
    over->listFiles("folder", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);
}

TEST_F(OverlayTest, moveFile)
{
    base->createFile("file1.txt");
    over->moveFile("file1.txt", "file2.txt");

    std::vector<String> paths, corr({"file2.txt"});
    over->listFiles("", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);

    over->createFile("file1.txt");
    corr = {"file1.txt", "file2.txt"};
    over->listFiles("", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);
}

TEST_F(OverlayTest, directoryIterator)
{
    base->createDirectory("folder");
    base->createFile("folder/file2.txt");
    base->createDirectory("folder/folder");

    over->createFile("folder/file1.txt");

    std::vector<String> paths, corr({"folder/file1.txt", "folder/file2.txt", "folder/folder/"});

    for (auto iter = over->iterateDirectory("folder"); iter->isValid(); iter->next()) {
        paths.push_back(iter->path());
    }
    std::sort(paths.begin(), paths.end());

    EXPECT_EQ(paths, corr);
}

TEST_F(OverlayTest, moveDirectory)
{
    base->createDirectory("folder1");
    base->createDirectory("folder2");
    base->createFile("folder1/file1.txt");
    base->createDirectory("folder1/inner");

    over->createFile("folder1/file2.txt");
    over->createFile("folder1/inner/file0.txt");

    over->moveDirectory("folder1", "folder2/folder1");

    std::vector<String> paths, corr({"file1.txt", "file2.txt", "inner"});
    over->listFiles("folder2/folder1", paths);
    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);

    corr = {"file0.txt"};
    over->listFiles("folder2/folder1/inner", paths);
    EXPECT_EQ(paths, corr);

    EXPECT_TRUE(!over->exists("folder1"));
}

TEST_F(OverlayTest, readFileBaseEmpty) {
    base->createFile("file.txt");

    writeToFileOver("file.txt", "test data", true);
    EXPECT_EQ(readFromFileOver("file.txt"), "test data");
}


TEST_F(OverlayTest, readFile)
{
    writeToFileBase("file.txt", "test data");
    EXPECT_EQ(readFromFileOver("file.txt"), "test data");

    writeToFileOver("file1.txt", "test data1", false);
    EXPECT_EQ(readFromFileOver("file1.txt"), "test data1");

    writeToFileOver("file.txt", " data1", true);
    EXPECT_EQ(readFromFileOver("file.txt"), "test data data1");

    writeToFileOver("file.txt", " data1", false);
    EXPECT_EQ(readFromFileOver("file.txt"), " data1");
}

TEST_F(OverlayTest, moveDeleteReadListFile) {
    base->createDirectory("folder");
    writeToFileBase("folder/file.txt", "test data");
    writeToFileBase("file1.txt", "test data 1");

    over->createDirectory("folder2");
    over->moveDirectory("folder", "folder2/folder");

    writeToFileOver("folder2/folder/file.txt", " more data", true);
    EXPECT_EQ(readFromFileOver("folder2/folder/file.txt"), "test data more data");

    over->removeFile("folder2/folder/file.txt");
    writeToFileOver("folder2/folder/file.txt", "more data", true);
    EXPECT_EQ(readFromFileOver("folder2/folder/file.txt"), "more data");

    writeToFileOver("file1.txt", "more data", false);

    std::vector<String> paths, corr({"file1.txt", "folder2"});
    over->listFiles("", paths);

    std::sort(paths.begin(), paths.end());
    EXPECT_EQ(paths, corr);
}

TEST_F(OverlayTest, hardlink) {
    base->createDirectory("folder");
    writeToFileBase("folder/file.txt", "test data");
    
    over->createHardLink("folder/file.txt", "folder/file1.txt");
    EXPECT_EQ(readFromFileOver("folder/file1.txt"), "test data");

    over->removeFile("folder/file.txt");
    writeToFileOver("folder/file1.txt", " more data", true);

    EXPECT_EQ(readFromFileOver("folder/file1.txt"), "test data more data");
}

TEST_F(OverlayTest, copy) {
    writeToFileBase("file.txt", "test data");
    writeToFileBase("file2.txt", "other data");

    writeToFileOver("file.txt", " more data", true);

    over->copyFile("file.txt", *over, "file1.txt");

    EXPECT_EQ(readFromFileOver("file1.txt"), "test data more data");

    over->copyFile("file2.txt", *over, "file3.txt");
    EXPECT_EQ(readFromFileOver("file3.txt"), "other data");
}
