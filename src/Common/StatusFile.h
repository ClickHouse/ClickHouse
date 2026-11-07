#pragma once

#include <string>
#include <functional>
#include <boost/noncopyable.hpp>


namespace DB
{

class WriteBuffer;


/** Provides that no more than one server works with one data directory.
  */
class StatusFile : private boost::noncopyable
{
public:
    using FillFunction = std::function<void(WriteBuffer &)>;

    /// An empty `fill` only locks the file and leaves one that already exists as it is.
    StatusFile(std::string path_, FillFunction fill);
    ~StatusFile();

    /// You can use one of these functions to fill the file or provide your own.
    static FillFunction write_pid;
    static FillFunction write_full_info;

private:
    const std::string path;
    const bool lock_only;
    bool existed = false;
    int fd = -1;
};


}
