#pragma once

#include <string>
#include <boost/noncopyable.hpp>


namespace DB
{


/** Обеспечивает, что с одной директорией с данными может одновременно работать не более одного сервера.
  */
class StatusFile : private boost::noncopyable
{
public:
	StatusFile(const std::string & path_);
	~StatusFile();

private:
	const std::string path;
	int fd = -1;
};


}
