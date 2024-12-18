#pragma once

#include <Core/MySQL/MySQLReplication.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Poco/Net/StreamSocket.h>

namespace DB
{
using namespace MySQLProtocol;
using namespace Generic;

namespace MySQLReplication
{

class IBinlog
{
public:
    virtual ~IBinlog() = default;
    virtual bool tryReadEvent(BinlogEventPtr & to, UInt64 ms) = 0;
    virtual Position getPosition() const = 0;
    enum Checksum : UInt8
    {
        NONE = 0,
        CRC32 = 1
    };
    virtual void setChecksum(Checksum /*checksum*/) { }
    static Checksum checksumFromString(const String & checksum);
};

using BinlogPtr = std::shared_ptr<IBinlog>;

class BinlogParser : public IBinlog
{
public:
    Position getPosition() const override { return position; }
    void setPosition(const Position & position_) { position = position_; }
    void setChecksum(Checksum checksum) override;
    static void updatePosition(const BinlogEventPtr & event, Position & position);
    /// Checks if \a older is older position than \a newer
    static bool isNew(const Position & older, const Position & newer);

protected:
    Position position;
    BinlogEventPtr event;
    std::map<UInt64, std::shared_ptr<TableMapEvent>> table_maps;
    size_t checksum_signature_length = 4;
    MySQLCharsetPtr flavor_charset = std::make_shared<MySQLCharset>();
    void parseEvent(EventHeader & event_header, ReadBuffer & event_payload);
};

class BinlogFromSocket : public BinlogParser
{
public:
    void connect(const String & host, UInt16 port, const String & user, const String & password);
    void start(UInt32 slave_id, const String & executed_gtid_set);
    bool tryReadEvent(BinlogEventPtr & to, UInt64 ms) override;

private:
    void disconnect();
    bool connected = false;
    uint8_t sequence_id = 0;
    const uint32_t client_capabilities = CLIENT_PROTOCOL_41 | CLIENT_PLUGIN_AUTH | CLIENT_SECURE_CONNECTION;

    std::unique_ptr<ReadBuffer> in;
    std::unique_ptr<WriteBuffer> out;
    std::unique_ptr<Poco::Net::StreamSocket> socket;
    std::optional<Poco::Net::SocketAddress> address;
    std::shared_ptr<MySQLProtocol::PacketEndpoint> packet_endpoint;

    void handshake(const String & user, const String & password);
    void registerSlaveOnMaster(UInt32 slave_id);
    void writeCommand(char command, const String & query);
};

class BinlogFromFile : public BinlogParser
{
public:
    void open(const String & filename);
    bool tryReadEvent(BinlogEventPtr & to, UInt64 ms) override;

private:
    std::unique_ptr<ReadBuffer> in;
};

class IBinlogFactory
{
public:
    virtual ~IBinlogFactory() = default;
    virtual BinlogPtr createBinlog(const String & executed_gtid_set) = 0;
};

using BinlogFactoryPtr = std::shared_ptr<IBinlogFactory>;

class BinlogFromFileFactory : public IBinlogFactory
{
public:
    explicit BinlogFromFileFactory(const String & filename_);
    BinlogPtr createBinlog(const String & executed_gtid_set) override;

private:
    const String filename;
};

class BinlogFromSocketFactory : public IBinlogFactory
{
public:
    BinlogFromSocketFactory(const String & host_, UInt16 port_, const String & user_, const String & password_);
    BinlogPtr createBinlog(const String & executed_gtid_set) override;

private:
    const String host;
    const UInt16 port;
    const String user;
    const String password;
};

bool operator==(const Position & left, const Position & right);

}
}
