#pragma once

#include <string>
#include <boost/noncopyable.hpp>


namespace DB
{


/** Provides that no more than one server works with one data directory.
  */
class StatusFile : private boost::noncopyable
{
public:
    explicit StatusFile(const std::string & path_);
    ~StatusFile();

private:
    const std::string path;
    int fd = -1;
};


}
