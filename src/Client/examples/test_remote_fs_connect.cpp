#include <cassert>

#include <iostream>
#include <thread>
#include <chrono>
#include <atomic>

#include <Client/RemoteFSConnectionPool.h>
#include <Client/RemoteFSConnection.h>

#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>

using namespace DB;
using namespace std::chrono_literals;

int main()
try
{
    Poco::Logger::setLevel(Poco::Logger::root().name(), Poco::Message::Priority::PRIO_TRACE);
    Poco::AutoPtr<Poco::ConsoleChannel> log = new Poco::ConsoleChannel;
    Poco::Logger::setChannel(Poco::Logger::root().name(), log.get());

    ConnectionTimeouts timeouts(
        Poco::Timespan(1000000), /// Connection timeout.
        Poco::Timespan(1000000), /// Send timeout.
        Poco::Timespan(1000000)  /// Receive timeout.
    );
    RemoteFSConnectionPool pool(2, "localhost", 9012, "test_disk");
    auto conn = pool.get(timeouts, true);
    auto conn2 = pool.get(timeouts, true);

    conn->clearDirectory("");
    
    assert((conn->getAvailableSpace() != 0));
    assert((conn2->getTotalSpace() != 0));

    conn->createFile("file_name");
    assert((conn->exists("file_name") && conn->isFile("file_name")));

    conn->createDirectory("dir_path");
    assert((conn->exists("dir_path") && conn->isDirectory("dir_path")));

    try
    {
        conn->createDirectory("dir/path");
        assert(false);
    }
    catch(...)
    {
        std::cerr << "CreateDirectory expected error" << '\n';
    }

    conn->createDirectories("dir/path");
    assert((conn->exists("dir/path") && conn->isDirectory("dir/path")));
    conn->removeRecursive("dir");

    conn->clearDirectory("");
    assert((!conn->exists("file_name")));
    assert((!conn->exists("dir_path")));

    conn->createDirectories("dir1/dir2");
    conn->moveDirectory("dir1/dir2", "dir2");
    assert((!conn->exists("dir1/dir2") && conn->exists("dir2")));

    conn->startIterateDirectory("");
    String dir_entry;
    size_t entry_count = 0;
    while (conn->nextDirectoryIteratorEntry(dir_entry)) 
    {
        assert((dir_entry == "dir1/" || dir_entry == "dir2/"));
        entry_count++;
    }
    assert((entry_count == 2));

    std::vector<String> files;
    conn->listFiles("", files);
    assert((files.size() == 2));
    for (const auto & file : files) 
    {
        assert((file == "dir1" || file == "dir2"));
    }

    conn->clearDirectory("");

    conn->createFile("file1");
    conn->createFile("file2");

    conn->moveFile("file1", "file3");
    assert((!conn->exists("file1") && conn->exists("file3")));
    try 
    {
        conn->moveFile("file3", "file2");
        assert(false);
    }
    catch (...)
    {
        std::cerr << "MoveFile expected error" << '\n';
    }
    conn->replaceFile("file3", "file1");
    assert((conn->exists("file1") && !conn->exists("file3")));

    conn->replaceFile("file1", "file2");
    assert((!conn->exists("file1") && !conn->exists("file3") && conn->exists("file2")));

    conn->copy("file2", "file1");
    assert((conn->exists("file1") && conn->exists("file2")));

    conn->startWriteFile("file1", 5, WriteMode::Append);
    conn->writeDataPacket("12345");
    conn->endWriteFile();

    assert((conn->getFileSize("file1") == 5));

    conn->startWriteFile("file1", 5, WriteMode::Append);
    conn->writeDataPacket("67890");
    conn->endWriteFile();

    assert((conn->getFileSize("file1") == 10));

    conn->startWriteFile("file1", 5, WriteMode::Rewrite);
    conn->writeDataPacket("abcde");
    conn->endWriteFile();

    assert((conn->getFileSize("file1") == 5));

    assert((conn->readFile("file1", 0, 5) == "abcde"));
    assert((conn->readFile("file1", 2, 3) == "cde"));

    conn->removeFile("file2");
    assert((!conn->exists("file2")));

    try
    {
        conn->removeFile("file2");
        assert(false);
    }
    catch (...)
    {
        std::cerr << "RemoveFile expected error" << '\n';
    }
    conn->removeFileIfExists("file2");
    conn->removeFileIfExists("file1");
    assert((!conn->exists("file1")));

    conn->createDirectories("path/to/dir1/dir2");
    try
    {
        conn->removeDirectory("path");
        assert(false);
    }
    catch (...)
    {
        std::cerr << "RemoveDirectory expected error" << '\n';
    }
    conn->removeDirectory("path/to/dir1/dir2");
    assert((!conn->exists("path/to/dir1/dir2")));
    conn->removeRecursive("path/to/dir1");
    assert((!conn->exists("path/to/dir1")));
    conn->removeRecursive("path");
    assert((!conn->exists("path")));

    conn->createFile("file1");
    time_t time = conn->getLastModified("file1").epochTime();
    std::this_thread::sleep_for(1s);
    assert((time == conn->getLastModified("file1").epochTime()));

    conn->startWriteFile("file1", 5, WriteMode::Append);
    conn->writeDataPacket("12345");
    conn->endWriteFile();
    assert((time < conn->getLastModified("file1").epochTime()));

    conn->setLastModified("file1", Poco::Timestamp::fromEpochTime(time));
    assert((time == conn->getLastModified("file1").epochTime()));

    time = conn->getLastChanged("file1");
    std::this_thread::sleep_for(1s);

    conn->createHardLink("file1", "file2");
    assert((conn->exists("file1") && conn->exists("file2")));
    assert((time < conn->getLastChanged("file1")));

    conn->startWriteFile("file1", 5, WriteMode::Append);
    conn->writeDataPacket("12345");
    conn->endWriteFile();

    assert((conn->getFileSize("file2") == 10));

    conn->truncateFile("file2", 2);
    assert((conn->getFileSize("file2") == 2));

    conn->clearDirectory("");
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
