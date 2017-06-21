#pragma once
#include <Common/Exception.h>
#include <Common/ZooKeeper/Types.h>
#include <Common/ProfileEvents.h>


namespace DB
{
    namespace ErrorCodes
    {
        extern const int KEEPER_EXCEPTION;
    }
}

namespace ProfileEvents
{
    extern const Event ZooKeeperExceptions;
}


namespace zkutil
{

class KeeperException : public DB::Exception
{
private:
    /// delegate constructor, used to minimize repetition; last parameter used for overload resolution
    KeeperException(const std::string & msg, const int32_t code, int)
        : DB::Exception(msg, DB::ErrorCodes::KEEPER_EXCEPTION), code(code) { incrementEventCounter(); }

public:
    KeeperException(const std::string & msg) : KeeperException(msg, ZOK, 0) {}
    KeeperException(const std::string & msg, const int32_t code)
        : KeeperException(msg + " (" + zerror(code) + ")", code, 0) {}
    KeeperException(const int32_t code) : KeeperException(zerror(code), code, 0) {}
    KeeperException(const int32_t code, const std::string & path)
        : KeeperException(std::string{zerror(code)} + ", path: " + path, code, 0) {}

    KeeperException(const KeeperException & exc) : DB::Exception(exc), code(exc.code) { incrementEventCounter(); }

    const char * name() const throw() { return "zkutil::KeeperException"; }
    const char * className() const throw() { return "zkutil::KeeperException"; }
    KeeperException * clone() const { return new KeeperException(*this); }

    /// при этих ошибках надо переинициализировать сессию с zookeeper
    bool isUnrecoverable() const
    {
        return code == ZINVALIDSTATE || code == ZSESSIONEXPIRED || code == ZSESSIONMOVED;
    }

    /// любая ошибка связанная с работой сети, перевыбором мастера
    /// при этих ошибках надо либо повторить запрос повторно, либо переинициализировать сессию (см. isUnrecoverable())
    bool isHardwareError() const
    {
        return isUnrecoverable() || code == ZCONNECTIONLOSS || code == ZOPERATIONTIMEOUT;
    }

    bool isTemporaryError() const
    {
        return code == ZCONNECTIONLOSS || code == ZOPERATIONTIMEOUT;
    }

    const int32_t code;

private:
    static void incrementEventCounter()
    {
        ProfileEvents::increment(ProfileEvents::ZooKeeperExceptions);
    }

};

};
