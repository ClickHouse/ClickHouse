#include <cassert>

#include <iostream>
#include <chrono>
#include <memory>

#include <Disks/DiskRemote.h>

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
    
    DiskPtr disk = std::make_shared<DiskRemote>("test", "localhost", 9012, "test_disk");
    auto buf = disk->writeFile("file1", 5, WriteMode::Rewrite, {});
    buf->write("1234567890", 10);
    buf->next();
    buf->finalize();

    assert((disk->getFileSize("file1") == 10));

    auto read_buf = disk->readFile("file1", { .remote_fs_buffer_size = 4 });
    assert((!read_buf->eof()));
    String data;
    data.resize(3);

    size_t bytes_read = read_buf->read(data.data(), 3);
    assert((bytes_read == 3));
    assert((data == "123"));

    read_buf->seek(5, SEEK_SET);
    assert((!read_buf->eof()));
    bytes_read = read_buf->read(data.data(), 3);
    assert((bytes_read == 3));
    std::cout << "Data: " << data << std::endl;
    assert((data == "678"));

    auto dir_iter = disk->iterateDirectory("");
    for(; dir_iter->isValid(); dir_iter->next()) {
        assert((dir_iter->path() == "file1"));
    }
}
catch (const Poco::Exception & e)
{
    std::cerr << e.displayText() << "\n";
}
