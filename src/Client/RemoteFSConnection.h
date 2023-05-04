#pragma once

#include <Common/logger_useful.h>

#include <Poco/Net/StreamSocket.h>

#include <IO/ConnectionTimeouts.h>
#include <IO/ReadBufferFromPocoSocket.h>
#include <IO/WriteBufferFromPocoSocket.h>
#include <Disks/WriteMode.h>

namespace DB
{
class RemoteFSConnection;

using RemoteFSConnectionPtr = std::shared_ptr<RemoteFSConnection>;

class RemoteFSConnection
{
public:
    RemoteFSConnection(const String & host_, UInt16 port_,
        const String & disk_name_, size_t conn_id_);

    ~RemoteFSConnection();

    /// For log and exception messages.
    const String & getDescription() const;
    const String & getHost() const;
    UInt16 getPort() const;

    // TODO send requests
    UInt64 getTotalSpace();
    UInt64 getAvailableSpace();
    bool exists(const String & path);
    bool isFile(const String & path);
    bool isDirectory(const String & path);
    size_t getFileSize(const String & path);
    void createDirectory(const String & path);
    void createDirectories(const String & path);
    void clearDirectory(const String & path);
    void moveDirectory(const String & from_path, const String & to_path);
    void startIterateDirectory(const String & path);
    bool nextDirectoryIteratorEntry(String & entry);
    void createFile(const String & path);
    void moveFile(const String & from_path, const String & to_path);
    void replaceFile(const String & from_path, const String & to_path);
    void copy(const String & from_path, const String & to_path);
    void copyDirectoryContent(const String & from_dir, const String & to_dir);
    void listFiles(const String & path, std::vector<String> & file_names);
    size_t readFile(const String & path, size_t offset, size_t size, char * data);
    void startWriteFile(const String & path, size_t buf_size, WriteMode mode);
    void writeData(const char * data_packet, size_t size);
    void endWriteFile();
    void removeFile(const String & path);
    void removeFileIfExists(const String & path);
    void removeDirectory(const String & path);
    void removeRecursive(const String & path);
    void setLastModified(const String & path, const Poco::Timestamp & timestamp);
    Poco::Timestamp getLastModified(const String & path);
    time_t getLastChanged(const String & path);
    void setReadOnly(const String & path);
    void createHardLink(const String & src_path, const String & dst_path);
    void truncateFile(const String & path, size_t size);

    void forceConnected(const ConnectionTimeouts & timeouts); 
    bool isConnected() const { return connected; }
    bool checkConnected(const ConnectionTimeouts & timeouts) { return connected && ping(timeouts); }
    void disconnect();

    size_t outBytesCount() const { return out ? out->count() : 0; }
    size_t inBytesCount() const { return in ? in->count() : 0; }

    Poco::Net::Socket * getSocket() { return socket.get(); }

private:
    String host;
    UInt16 port;
    String disk_name;

    size_t conn_id;

    /// Address is resolved during the first connection (or the following reconnects)
    /// Use it only for logging purposes
    std::optional<Poco::Net::SocketAddress> current_resolved_address;

    /// For messages in log and in exceptions.
    String description;
    void setDescription();

    /// Returns resolved address if it was resolved.
    std::optional<Poco::Net::SocketAddress> getResolvedAddress() const;

    bool connected = false;

    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::shared_ptr<ReadBufferFromPocoSocket> in;
    std::shared_ptr<WriteBufferFromPocoSocket> out;

    /// Logger is created lazily, for avoid to run DNS request in constructor.
    class LoggerWrapper
    {
    public:
        explicit LoggerWrapper(RemoteFSConnection & parent_)
            : log(nullptr), parent(parent_)
        {
        }

        Poco::Logger * get()
        {
            if (!log)
                log = &Poco::Logger::get("RemoteFSConnection (" + parent.getDescription() + ")");

            return log;
        }

    private:
        std::atomic<Poco::Logger *> log;
        RemoteFSConnection & parent;
    };

    LoggerWrapper log_wrapper;

    void connect(const ConnectionTimeouts & timeouts);
    void sendHello();
    void receiveHello();

    void receiveAndCheckPacketType(UInt64 expected_packet_type, const char * expected_packet_name);

    bool ping(const ConnectionTimeouts & timeouts);

    std::unique_ptr<Exception> receiveException() const;

    [[noreturn]] void throwUnexpectedPacket(UInt64 packet_type, const char * expected) const;
};

}
