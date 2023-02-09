//
// Command.h
//
// Library: Redis
// Package: Redis
// Module:  Command
//
// Definition of the Command class.
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Redis_Command_INCLUDED
#define Redis_Command_INCLUDED


#include "Poco/Redis/Redis.h"
#include "Poco/Redis/Array.h"
#include <vector>
#include <map>


namespace Poco {
namespace Redis {


class Redis_API Command: public Array
	/// Helper class for creating commands. This class contains
	/// factory methods for commonly used Redis commands.
	///
	/// There are two ways of building commands:
	///
	///     1. Use this class and the factory methods
	///     2. Use the Array or Command class and build the command using the add
	///        method or << operator.
	///
	/// For example:
	///
	///     Command cmd = Command::set("mykey", "Hello");
	///
	/// is the same as:
	///
	///     Array cmd;
	///     cmd << "SET" << "mykey" << "Hello";
	///
{
public:
	typedef std::vector<std::string> StringVec;

	Command(const std::string& command);
		/// Creates a command.

	Command(const Command& copy);
		/// Creates a command by copying another one.

	virtual ~Command();
		/// Destroys the command.

	static Command append(const std::string& key, const std::string& value);
		/// Creates and returns an APPEND command.

	static Command blpop(const StringVec& lists, Int64 timeout = 0);
		/// Creates and returns a BLPOP command.

	static Command brpop(const StringVec& lists, Int64 timeout = 0);
		/// Creates and returns a BRPOP command.

	static Command brpoplpush(const std::string& sourceList, const std::string& destinationList, Int64 timeout = 0);
		/// Creates and returns a BRPOPLPUSH command.

	static Command decr(const std::string& key, Int64 by = 0);
		/// Creates and returns an DECR or DECRBY command. Calls DECR when by is omitted or zero.

	static Command del(const std::string& key);
		/// Creates and returns an DEL command.

	static Command del(const StringVec& keys);
		/// Creates and returns an DEL command.

	static Command get(const std::string& key);
		/// Creates and returns an GET command.

	static Command exists(const std::string& key);
		/// Creates and returns an EXISTS command.

	static Command hdel(const std::string& hash, const std::string& field);
		/// Creates and returns an HDEL command.

	static Command hdel(const std::string& hash, const StringVec& fields);
		/// Creates and returns an HDEL command.

	static Command hexists(const std::string& hash, const std::string& field);
		/// Creates and returns an HEXISTS command.

	static Command hget(const std::string& hash, const std::string& field);
		/// Creates and returns an HGET command.

	static Command hgetall(const std::string& hash);
		/// Creates and returns an HGETALL command.

	static Command hincrby(const std::string& hash, const std::string& field, Int64 by = 1);
		/// Creates and returns an HINCRBY command.

	static Command hkeys(const std::string& hash);
		/// Creates and returns an HKEYS command.

	static Command hlen(const std::string& hash);
		/// Creates and returns an HLEN command.

	static Command hmget(const std::string& hash, const StringVec& fields);
		/// Creates and returns an HMGET command.

	static Command hmset(const std::string& hash, std::map<std::string, std::string>& fields);
		/// Creates and returns a HMSET command.

	static Command hset(const std::string& hash, const std::string& field, const std::string& value, bool create = true);
		/// Creates and returns an HSET or HSETNX (when create is false) command.

	static Command hset(const std::string& hash, const std::string& field, Int64 value, bool create = true);
		/// Creates and returns an HSET or HSETNX (when create is false) command.

	static Command hstrlen(const std::string& hash, const std::string& field);
		/// Creates and returns an HSTRLEN command (Available for Redis 3.2).

	static Command hvals(const std::string& hash);
		/// Creates and returns an HVALS command.

	static Command incr(const std::string& key, Int64 by = 0);
		/// Creates and returns an INCR or INCRBY command. Calls INCR when by is omitted or zero.

	static Command lindex(const std::string& list, Int64 index = 0);
		/// Creates and returns a LINDEX command.

	static Command linsert(const std::string& list, bool before, const std::string& reference, const std::string& value);
		/// Creates and returns a LINSERT command.

	static Command llen(const std::string& list);
		/// Creates and returns a LLEN command.

	static Command lpop(const std::string& list);
		/// Creates and returns a LPOP command.

	static Command lpush(const std::string& list, const std::string& value, bool create = true);
		/// Creates and returns a LPUSH or LPUSHX (when create is false) command.

	static Command lpush(const std::string& list, const StringVec& value, bool create = true);
		/// Creates and returns a LPUSH or LPUSHX (when create is false) command.

	static Command lrange(const std::string& list, Int64 start = 0, Int64 stop = -1);
		/// Creates and returns a LRANGE command. When start and stop is omitted an LRANGE
		/// command will returned that returns all items of the list.

	static Command lrem(const std::string& list, Int64 count, const std::string& value);
		/// Creates and returns a LREM command.

	static Command lset(const std::string& list, Int64 index, const std::string& value);
		/// Creates and returns a LSET command.

	static Command ltrim(const std::string& list, Int64 start = 0, Int64 stop = -1);
		/// Creates and returns a LTRIM command.

	static Command mget(const StringVec& keys);
		/// Creates and returns a MGET command.

	static Command mset(const std::map<std::string, std::string>& keyvalues, bool create = true);
		/// Creates and returns a MSET or MSETNX (when create is false) command.

	static Command sadd(const std::string& set, const std::string& value);
		/// Creates and returns a SADD command.

	static Command sadd(const std::string& set, const StringVec& values);
		/// Creates and returns a SADD command.

	static Command scard(const std::string& set);
		/// Creates and returns a SCARD command.

	static Command sdiff(const std::string& set1, const std::string& set2);
		/// Creates and returns a SDIFF command.Creates and returns

	static Command sdiff(const std::string& set, const StringVec& sets);
		/// Creates and returns a SDIFF command.

	static Command sdiffstore(const std::string& set, const std::string& set1, const std::string& set2);
		/// Creates and returns a SDIFFSTORE command.

	static Command sdiffstore(const std::string& set, const StringVec& sets);
		/// Creates and returns a SDIFFSTORE command.

	static Command set(const std::string& key, const std::string& value, bool overwrite = true, const Poco::Timespan& expireTime = 0, bool create = true);
		/// Creates and returns a SET command to set the key with a value.

	static Command set(const std::string& key, Int64 value, bool overwrite = true, const Poco::Timespan& expireTime = 0, bool create = true);
		/// Creates and returns a SET command to set the key with a value.

	static Command sinter(const std::string& set1, const std::string& set2);
		/// Creates and returns a SINTER command.

	static Command sinter(const std::string& set, const StringVec& sets);
		/// Creates and returns a SINTER command.

	static Command sinterstore(const std::string& set, const std::string& set1, const std::string& set2);
		/// Creates and returns a SINTERSTORE command.

	static Command sinterstore(const std::string& set, const StringVec& sets);
		/// Creates and returns a SINTERSTORE command.

	static Command sismember(const std::string& set, const std::string& member);
		/// Creates and returns a SISMEMBER command.

	static Command smembers(const std::string& set);
		/// Creates and returns a SMEMBERS command.

	static Command smove(const std::string& source, const std::string& destination, const std::string& member);
		/// Creates and returns a SMOVE command.

	static Command spop(const std::string& set, Int64 count = 0);
		/// Creates and returns a SPOP command.

	static Command srandmember(const std::string& set, Int64 count = 0);
		/// Creates and returns a SRANDMEMBER command.

	static Command srem(const std::string& set, const std::string& member);
		/// Creates and returns a SREM command.

	static Command srem(const std::string& set, const StringVec& members);
		/// Creates and returns a SREM command.

	static Command sunion(const std::string& set1, const std::string& set2);
		/// Creates and returns a SUNION command.

	static Command sunion(const std::string& set, const StringVec& sets);
		/// Creates and returns a SUNION command.

	static Command sunionstore(const std::string& set, const std::string& set1, const std::string& set2);
		/// Creates and returns a SUNIONSTORE command.

	static Command sunionstore(const std::string& set, const StringVec& sets);
		/// Creates and returns a SUNIONSTORE command.

	static Command rename(const std::string& key, const std::string& newName, bool overwrite = true);
		/// Creates and returns a RENAME or RENAMENX when overwrite is false.

	static Command rpop(const std::string& list);
		/// Creates and returns a RPOP command.

	static Command rpoplpush(const std::string& sourceList, const std::string& destinationList);
		/// Creates and returns a RPOPLPUSH command.

	static Command rpush(const std::string& list, const std::string& value, bool create = true);
		/// Creates and returns a RPUSH or RPUSHX (when create is false) command.

	static Command rpush(const std::string& list, const StringVec& value, bool create = true);
		/// Creates and returns a RPUSH or RPUSHX (when create is false) command.

	static Command expire(const std::string& key, Int64 seconds);
		/// Creates and returns an EXPIRE command.

	static Command ping();
		/// Creates and returns a PING command.

	static Command multi();
		/// Creates and returns a MULTI command.

	static Command exec();
		/// Creates and returns a EXEC command.

	static Command discard();
		/// Creates and returns a DISCARD command.
};


} } // namespace Poco::Redis


#endif // Redis_Command_INCLUDED
