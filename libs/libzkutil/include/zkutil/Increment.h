#pragma once

#include <zkutil/ZooKeeper.h>

namespace zkutil 
{
	
class Increment
{
public:
	Increment(ZooKeeperPtr zk_, const std::string & path_) 
	: zk(zk_), path(path_)
	{
		zk->createAncestors(path);
	}
	
	size_t get()
	{
		LOG_TRACE(log, "Get increment");
		
		size_t result = 0;
		std::string result_str;
		zkutil::Stat stat;
		
		bool success = false;
		do
		{
			if (zk->tryGet(path, result_str, &stat))
			{
				result = std::stol(result_str) + 1;
				success = zk->trySet(path, std::to_string(result), stat.version) == ZOK;
			}
			else
			{
				success = zk->tryCreate(path, std::to_string(result), zkutil::CreateMode::Persistent) == ZOK;
			}
		} 
		while(!success);
		
		return result;
	}
private:
	zkutil::ZooKeeperPtr zk;
	std::string path;
	Logger * log = &Logger::get("zkutil::Increment");
};

}