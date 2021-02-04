#pragma once

#include <functional>
#include <string>
#include <boost/noncopyable.hpp>


namespace DB
{
class WriteBuffer;


/** Provides that no more than one server works with one data directory.
  */
class ServerUUIDFile : private boost::noncopyable
{
public:
    using FillFunction = std::function<void(WriteBuffer &)>;

    ServerUUIDFile(std::string path_, FillFunction fill_);
    ~ServerUUIDFile();

    /// You can use one of these functions to fill the file or provide your own.
    static FillFunction write_server_uuid;

private:
    const std::string path;
    FillFunction fill;
    int fd = -1;
};


}
