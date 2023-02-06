//
// RedisTest.cpp
//
// Copyright (c) 2015, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Exception.h"
#include "Poco/Delegate.h"
#include "Poco/Thread.h"
#include "RedisTest.h"
#include "Poco/Redis/AsyncReader.h"
#include "Poco/Redis/Command.h"
#include "Poco/Redis/PoolableConnectionFactory.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include <iostream>


using namespace Poco::Redis;


bool RedisTest::_connected = false;
Poco::Redis::Client RedisTest::_redis;


RedisTest::RedisTest(const std::string& name):
	CppUnit::TestCase("Redis"),
	_host("localhost"),
	_port(6379)
{
#if POCO_OS == POCO_OS_ANDROID
	_host = "10.0.2.2";
#endif
	if (!_connected)
	{
		try
		{
			Poco::Timespan t(10, 0); // Connect within 10 seconds
			_redis.connect(_host, _port, t);
			_connected = true;
			std::cout << "Connected to [" << _host << ':' << _port << ']' << std::endl;
		}
		catch (Poco::Exception& e)
		{
			std::cout << "Couldn't connect to [" << _host << ':' << _port << ']' << e.message() << ". " << std::endl;
		}
	}
}


RedisTest::~RedisTest()
{
	if (_connected)
	{
		_redis.disconnect();
		_connected = false;
		std::cout << "Disconnected from [" << _host << ':' << _port << ']' << std::endl;
	}
}


void RedisTest::setUp()
{
}


void RedisTest::tearDown()
{
}


void RedisTest::testAPPEND()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("mykey");

	Command setCommand = Command::set("mykey", "Hello");
	try
	{
		std::string result = _redis.execute<std::string>(setCommand);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	Command appendCommand = Command::append("mykey", " World");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(appendCommand);
		assert(result == 11);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	Command getCommand = Command::get("mykey");
	try
	{
		BulkString result = _redis.execute<BulkString>(getCommand);
		assert(result.value().compare("Hello World") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testBLPOP()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the lists are not there yet ...
	std::vector<std::string> lists;
	lists.push_back("list1");
	lists.push_back("list2");
	Command delCommand = Command::del(lists);
	try
	{
		_redis.execute<Poco::Int64>(delCommand);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values;
	values.push_back("a");
	values.push_back("b");
	values.push_back("c");

	try
	{
		Command rpush = Command::rpush("list1", values);
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command blpop = Command::blpop(lists);
	try
	{
		Array result = _redis.execute<Array>(blpop);
		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("list1") == 0);
		assert(result.get<BulkString>(1).value().compare("a") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testBRPOP()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the lists are not there yet ...
	std::vector<std::string> lists;
	lists.push_back("list1");
	lists.push_back("list2");
	Command delCommand = Command::del(lists);
	try
	{
		_redis.execute<Poco::Int64>(delCommand);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values;
	values.push_back("a");
	values.push_back("b");
	values.push_back("c");

	try
	{
		Command rpush = Command::rpush("list1", values);
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command brpop = Command::brpop(lists);
	try
	{
		Array result = _redis.execute<Array>(brpop);
		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("list1") == 0);
		assert(result.get<BulkString>(1).value().compare("c") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testDECR()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command set = Command::set("mykey", 10);
	try
	{
		std::string result = _redis.execute<std::string>(set);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command decr = Command::decr("mykey");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(decr);
		assert(result == 9);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	set = Command::set("mykey", "234293482390480948029348230948");
	try
	{
		std::string result = _redis.execute<std::string>(set);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(decr);
		fail("This must fail");
	}
	catch (RedisException& e)
	{
		// ERR value is not an integer or out of range
	}

}


void RedisTest::testECHO()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Array command;
	command.add("ECHO")
		.add("Hello World");

	try
	{
		BulkString result = _redis.execute<BulkString>(command);
		assert(!result.isNull());
		assert(result.value().compare("Hello World") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testError()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Array command;
	command.add("Wrong Command");

	try
	{
		BulkString result = _redis.execute<BulkString>(command);
		fail("Invalid command must throw RedisException");
	}
	catch (RedisException& e)
	{
		// Must fail
	}
}


void RedisTest::testEVAL()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command cmd("EVAL");
	cmd << "return {1, 2, {3, 'Hello World!'}}" << Poco::NumberFormatter::format(0);

	try
	{
		Array value = _redis.execute<Array>(cmd);
		assert(value.size() == 3);

		Poco::Int64 i = value.get<Poco::Int64>(0);
		assert(i == 1);
		i = value.get<Poco::Int64>(1);
		assert(i == 2);

		Array a = value.get<Array>(2);
		assert(a.size() == 2);
		i = a.get<Poco::Int64>(0);
		assert(i == 3);
		BulkString s = a.get<BulkString>(1);
		assert(s.value().compare("Hello World!") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

}


void RedisTest::testHDEL()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field1", "foo");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hdel = Command::hdel("myhash", "field1");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(hdel);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hdel = Command::hdel("myhash", "field2");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(hdel);
		assert(result == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHEXISTS()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field1", "foo");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hexists = Command::hexists("myhash", "field1");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(hexists);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hexists = Command::hexists("myhash", "field2");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(hexists);
		assert(result == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHGETALL()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field1", "Hello");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hset = Command::hset("myhash", "field2", "World");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hgetall = Command::hgetall("myhash");
	try
	{
		Array result = _redis.execute<Array>(hgetall);
		assert(result.size() == 4);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHINCRBY()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field", 5);
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hincrby = Command::hincrby("myhash", "field");
	try
	{
		Poco::Int64 n = _redis.execute<Poco::Int64>(hincrby);
		assert(n == 6);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hincrby = Command::hincrby("myhash", "field", -1);
	try
	{
		Poco::Int64 n = _redis.execute<Poco::Int64>(hincrby);
		assert(n == 5);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hincrby = Command::hincrby("myhash", "field", -10);
	try
	{
		Poco::Int64 n = _redis.execute<Poco::Int64>(hincrby);
		assert(n == -5);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHKEYS()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field1", "Hello");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hset = Command::hset("myhash", "field2", "World");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hlen = Command::hlen("myhash");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hlen);
		assert(value == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hkeys = Command::hkeys("myhash");
	try
	{
		Array result = _redis.execute<Array>(hkeys);
		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("field1") == 0);
		assert(result.get<BulkString>(1).value().compare("field2") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHMGET()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field1", "Hello");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hset = Command::hset("myhash", "field2", "World");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> fields;
	fields.push_back("field1");
	fields.push_back("field2");
	fields.push_back("field3");
	Command hmget = Command::hmget("myhash", fields);
	try
	{
		Array result = _redis.execute<Array>(hmget);
		assert(result.size() == 3);

		assert(result.get<BulkString>(0).value().compare("Hello") == 0);
		assert(result.get<BulkString>(1).value().compare("World") == 0);
		assert(result.get<BulkString>(2).isNull());
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHSET()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	Command hset = Command::hset("myhash", "field1", "Hello");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(hset);
		assert(value == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hget = Command::hget("myhash", "field1");
	try
	{
		BulkString s = _redis.execute<BulkString>(hget);
		assert(s.value().compare("Hello") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHMSET()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	std::map<std::string, std::string> fields;
	fields.insert(std::make_pair<std::string, std::string>("field1", "Hello"));
	fields.insert(std::make_pair<std::string, std::string>("field2", "World"));

	Command hmset = Command::hmset("myhash", fields);
	try
	{
		std::string result = _redis.execute<std::string>(hmset);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hget = Command::hget("myhash", "field1");
	try
	{
		BulkString s = _redis.execute<BulkString>(hget);
		assert(s.value().compare("Hello") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hget = Command::hget("myhash", "field2");
	try
	{
		BulkString s = _redis.execute<BulkString>(hget);
		assert(s.value().compare("World") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

}


void RedisTest::testHSTRLEN()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	std::map<std::string, std::string> fields;
	fields.insert(std::make_pair<std::string, std::string>("f1", "HelloWorld"));
	fields.insert(std::make_pair<std::string, std::string>("f2", "99"));
	fields.insert(std::make_pair<std::string, std::string>("f3", "-256"));

	Command hmset = Command::hmset("myhash", fields);
	try
	{
		std::string result = _redis.execute<std::string>(hmset);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hstrlen = Command::hstrlen("myhash", "f1");
	try
	{
		Poco::Int64 len = _redis.execute<Poco::Int64>(hstrlen);
		assert(len == 10);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hstrlen = Command::hstrlen("myhash", "f2");
	try
	{
		Poco::Int64 len = _redis.execute<Poco::Int64>(hstrlen);
		assert(len == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	hstrlen = Command::hstrlen("myhash", "f3");
	try
	{
		Poco::Int64 len = _redis.execute<Poco::Int64>(hstrlen);
		assert(len == 4);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testHVALS()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myhash");

	std::map<std::string, std::string> fields;
	fields.insert(std::make_pair<std::string, std::string>("field1", "Hello"));
	fields.insert(std::make_pair<std::string, std::string>("field2", "World"));

	Command hmset = Command::hmset("myhash", fields);
	try
	{
		std::string result = _redis.execute<std::string>(hmset);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command hvals = Command::hvals("myhash");
	try
	{
		Array result = _redis.execute<Array>(hvals);
		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("Hello") == 0);
		assert(result.get<BulkString>(1).value().compare("World") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testINCR()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command command = Command::set("mykey", "10");
	// A set responds with a simple OK string
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	command = Command::incr("mykey");
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(command);
		assert(value == 11);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testINCRBY()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command command = Command::set("mykey", "10");
	// A set responds with a simple OK string
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	command = Command::incr("mykey", 5);
	try
	{
		Poco::Int64 value = _redis.execute<Poco::Int64>(command);
		assert(value == 15);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testPING()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Array command;
	command.add("PING");

	// A PING without a custom strings, responds with a simple "PONG" string
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("PONG") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

#ifndef OLD_REDIS_VERSION
	// A PING with a custom string responds with a bulk string
	command.add("Hello");
	try
	{
		BulkString result = _redis.execute<BulkString>(command);
		assert(!result.isNull());
		assert(result.value().compare("Hello") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
#endif
}


void RedisTest::testLPOP()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command rpush = Command::rpush("mylist", "one");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "two");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);

		rpush = Command::rpush("mylist", "three");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lpop = Command::lpop("mylist");
	try
	{
		BulkString result = _redis.execute<BulkString>(lpop);
		assert(result.value().compare("one") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("two") == 0);
		assert(result.get<BulkString>(1).value().compare("three") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}

}


void RedisTest::testLSET()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command rpush = Command::rpush("mylist", "one");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "two");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);

		rpush = Command::rpush("mylist", "three");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lset = Command::lset("mylist", 0, "four");
	try
	{
		std::string result = _redis.execute<std::string>(lset);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	lset = Command::lset("mylist", -2, "five");
	try
	{
		std::string result = _redis.execute<std::string>(lset);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 3);
		assert(result.get<BulkString>(0).value().compare("four") == 0);
		assert(result.get<BulkString>(1).value().compare("five") == 0);
		assert(result.get<BulkString>(2).value().compare("three") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}

}


void RedisTest::testLINDEX()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command lpush = Command::lpush("mylist", "World");
		Poco::Int64 result = _redis.execute<Poco::Int64>(lpush);
		assert(result == 1);

		lpush = Command::lpush("mylist", "Hello");
		result = _redis.execute<Poco::Int64>(lpush);
		assert(result == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lindex = Command::lindex("mylist", 0);
	try
	{
		BulkString result = _redis.execute<BulkString>(lindex);
		assert(result.value().compare("Hello") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testLINSERT()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command rpush = Command::rpush("mylist", "Hello");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "World");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);

		Command linsert = Command::linsert("mylist", true, "World", "There");
		result = _redis.execute<Poco::Int64>(linsert);
		assert(result == 3);

		Command lrange = Command::lrange("mylist", 0, -1);
		Array range = _redis.execute<Array>(lrange);
		assert(range.size() == 3);

		assert(range.get<BulkString>(0).value().compare("Hello") == 0);
		assert(range.get<BulkString>(1).value().compare("There") == 0);
		assert(range.get<BulkString>(2).value().compare("World") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testLREM()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		std::vector<std::string> list;
		list.push_back("hello");
		list.push_back("hello");
		list.push_back("foo");
		list.push_back("hello");
		Command rpush = Command::rpush("mylist", list);
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 4);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lrem = Command::lrem("mylist", -2, "hello");
	try
	{
		Poco::Int64 n = _redis.execute<Poco::Int64>(lrem);
		assert(n == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("hello") == 0);
		assert(result.get<BulkString>(1).value().compare("foo") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testLTRIM()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command rpush = Command::rpush("mylist", "one");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "two");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);

		rpush = Command::rpush("mylist", "three");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command ltrim = Command::ltrim("mylist", 1);
	try
	{
		std::string result = _redis.execute<std::string>(ltrim);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("two") == 0);
		assert(result.get<BulkString>(1).value().compare("three") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}

}


void RedisTest::testMSET()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command command("MSET");
	command << "key1" << "Hello" << "key2" << "World";

	// A MSET responds with a simple OK string
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	command.clear();
	command.add("MGET")
		.add("key1")
		.add("key2")
		.add("nonexisting");
	try
	{
		Array result = _redis.execute<Array>(command);

		assert(result.size() == 3);
		BulkString value = result.get<BulkString>(0);
		assert(value.value().compare("Hello") == 0);

		value = result.get<BulkString>(1);
		assert(value.value().compare("World") == 0);

		value = result.get<BulkString>(2);
		assert(value.isNull());
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testMSETWithMap()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	std::map<std::string, std::string> keyValuePairs;
	keyValuePairs.insert(std::make_pair<std::string, std::string>("key1", "Hello"));
	keyValuePairs.insert(std::make_pair<std::string, std::string>("key2", "World"));

	Command mset = Command::mset(keyValuePairs);

	// A MSET responds with a simple OK string
	try
	{
		std::string result = _redis.execute<std::string>(mset);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> keys;
	keys.push_back("key1");
	keys.push_back("key2");
	keys.push_back("nonexisting");

	Command mget = Command::mget(keys);
	try
	{
		Array result = _redis.execute<Array>(mget);

		assert(result.size() == 3);
		BulkString value = result.get<BulkString>(0);
		assert(value.value().compare("Hello") == 0);

		value = result.get<BulkString>(1);
		assert(value.value().compare("World") == 0);

		value = result.get<BulkString>(2);
		assert(value.isNull());
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testMULTI()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure keys are gone from a previous testrun ...
	delKey("foo");
	delKey("bar");

	Array command;
	command.add("MULTI");
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	command.clear();
	command.add("INCR")
		.add("foo");
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("QUEUED") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	command.clear();
	command.add("INCR")
		.add("bar");
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("QUEUED") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	command.clear();
	command.add("EXEC");
	try
	{
		Array result = _redis.execute<Array>(command);
		assert(result.size() == 2);

		Poco::Int64 v = result.get<Poco::Int64>(0);
		assert(v == 1);
		v = result.get<Poco::Int64>(1);
		assert(v == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testPipeliningWithSendCommands()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	std::vector<Array> commands;

	Array ping;
	ping.add("PING");
	commands.push_back(ping);
	commands.push_back(ping);

	Array result = _redis.sendCommands(commands);

	// We expect 2 results
	assert(result.size() == 2);

	// The 2 results must be simple PONG strings
	for (size_t i = 0; i < 2; ++i)
	{
		try
		{
			std::string pong = result.get<std::string>(i);
			assert(pong.compare("PONG") == 0);
		}
		catch (...)
		{
			fail("An exception occurred");
		}
	}
}


void RedisTest::testPipeliningWithWriteCommand()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Array ping;
	ping.add("PING");

	_redis.execute<void>(ping);
	_redis.execute<void>(ping);
	_redis.flush();

	// We expect 2 results with simple "PONG" strings
	for (int i = 0; i < 2; ++i)
	{
		std::string pong;
		try
		{
			_redis.readReply<std::string>(pong);
			assert(pong.compare("PONG") == 0);
		}
		catch (RedisException& e)
		{
			fail(e.message());
		}
	}
}


class RedisSubscriber
{
public:
	void onMessage(const void* pSender, RedisEventArgs& args)
	{
		if (!args.message().isNull())
		{
			Type<Array>* arrayType = dynamic_cast<Type<Array>*>(args.message().get());
			if (arrayType != NULL)
			{
				Array& array = arrayType->value();
				if (array.size() == 3)
				{
					BulkString type = array.get<BulkString>(0);
					if (type.value().compare("unsubscribe") == 0)
					{
						Poco::Int64 n = array.get<Poco::Int64>(2);
						// When 0, no subscribers anymore, so stop reading ...
						if (n == 0) args.stop();
					}
				}
				else
				{
					// Wrong array received. Stop the reader
					args.stop();
				}
			}
			else
			{
				// Invalid type of message received. Stop the reader ...
				args.stop();
			}
		}
	}

	void onError(const void* pSender, RedisEventArgs& args)
	{
		std::cout << args.exception()->className() << std::endl;
		// No need to call stop, AsyncReader stops automatically when an
		// exception is received.
	}
};


void RedisTest::testPubSub()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	RedisSubscriber subscriber;

	Array subscribe;
	subscribe.add("SUBSCRIBE").add("test");

	_redis.execute<void>(subscribe);
	_redis.flush();

	AsyncReader reader(_redis);
	reader.redisResponse += Poco::delegate(&subscriber, &RedisSubscriber::onMessage);
	reader.redisException += Poco::delegate(&subscriber, &RedisSubscriber::onError);
	reader.start();

	std::cout << "Sleeping ..." << std::endl;
	Poco::Thread::sleep(10000);

	Array unsubscribe;
	unsubscribe.add("UNSUBSCRIBE");

	_redis.execute<void>(unsubscribe);
	_redis.flush();
}


void RedisTest::testSADD()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	Command sadd = Command::sadd("myset", "Hello");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "World");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "World");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSCARD()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	Command sadd = Command::sadd("myset", "Hello");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "World");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command scard = Command::scard("myset");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(scard);
		assert(result == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSDIFF()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("key1");
	delKey("key2");

	std::vector<std::string> values1;
	values1.push_back("a");
	values1.push_back("b");
	values1.push_back("c");
	Command sadd = Command::sadd("key1", values1);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values2;
	values2.push_back("c");
	values2.push_back("d");
	values2.push_back("e");
	sadd = Command::sadd("key2", values2);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sdiff = Command::sdiff("key1", "key2");
	try
	{
		Array result = _redis.execute<Array>(sdiff);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSDIFFSTORE()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("key");
	delKey("key1");
	delKey("key2");

	std::vector<std::string> values1;
	values1.push_back("a");
	values1.push_back("b");
	values1.push_back("c");
	Command sadd = Command::sadd("key1", values1);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values2;
	values2.push_back("c");
	values2.push_back("d");
	values2.push_back("e");
	sadd = Command::sadd("key2", values2);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sdiffstore = Command::sdiffstore("key", "key1", "key2");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sdiffstore);
		assert(result == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("key");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSET()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Array command;
	command.add("SET").add("mykey").add("Hello");

	// A set responds with a simple OK string
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	command.add("NX");
	// A set NX responds with a Null bulk string
	// when the key is already set
	try
	{
		BulkString result = _redis.execute<BulkString>(command);
		assert(result.isNull());
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSINTER()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("key1");
	delKey("key2");

	std::vector<std::string> values1;
	values1.push_back("a");
	values1.push_back("b");
	values1.push_back("c");
	Command sadd = Command::sadd("key1", values1);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values2;
	values2.push_back("c");
	values2.push_back("d");
	values2.push_back("e");
	sadd = Command::sadd("key2", values2);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sinter = Command::sinter("key1", "key2");
	try
	{
		Array result = _redis.execute<Array>(sinter);
		assert(result.size() == 1);
		assert(result.get<BulkString>(0).value().compare("c") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSINTERSTORE()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("key");
	delKey("key1");
	delKey("key2");

	std::vector<std::string> values1;
	values1.push_back("a");
	values1.push_back("b");
	values1.push_back("c");
	Command sadd = Command::sadd("key1", values1);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values2;
	values2.push_back("c");
	values2.push_back("d");
	values2.push_back("e");
	sadd = Command::sadd("key2", values2);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sinterstore = Command::sinterstore("key", "key1", "key2");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sinterstore);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("key");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 1);
		assert(result.get<BulkString>(0).value().compare("c") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSISMEMBER()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	Command sadd = Command::sadd("myset", "one");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sismember = Command::sismember("myset", "one");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sismember);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sismember = Command::sismember("myset", "two");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sismember);
		assert(result == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSMEMBERS()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	Command sadd = Command::sadd("myset", "Hello");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "World");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("myset");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSMOVE()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");
	delKey("myotherset");

	Command sadd = Command::sadd("myset", "one");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "two");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myotherset", "three");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smove = Command::smove("myset", "myotherset", "two");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(smove);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("myset");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 1);
		assert(result.get<BulkString>(0).value().compare("one") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	smembers = Command::smembers("myotherset");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSPOP()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	Command sadd = Command::sadd("myset", "one");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "two");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "three");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command spop = Command::spop("myset");
	try
	{
		BulkString result = _redis.execute<BulkString>(spop);
		assert(!result.isNull());
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("myset");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "four");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	sadd = Command::sadd("myset", "five");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
// Redis server 3.0.5 doesn't support this yet ..
/*
	spop = Command::spop("myset", 3);
	try
	{
		Array result = _redis.execute<Array>(spop);
		assert(result.size() == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
*/
}


void RedisTest::testSRANDMEMBER()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	std::vector<std::string> members;
	members.push_back("one");
	members.push_back("two");
	members.push_back("three");

	Command sadd = Command::sadd("myset", members);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command srandmember = Command::srandmember("myset");
	try
	{
		BulkString result = _redis.execute<BulkString>(srandmember);
		assert(!result.isNull());
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	srandmember = Command::srandmember("myset", 2);
	try
	{
		Array result = _redis.execute<Array>(srandmember);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	srandmember = Command::srandmember("myset", -5);
	try
	{
		Array result = _redis.execute<Array>(srandmember);
		assert(result.size() == 5);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSTRLEN()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Array command;
	command.add("SET").add("mykey").add("Hello World");

	// A set responds with a simple OK string
	try
	{
		std::string result = _redis.execute<std::string>(command);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	command.clear();
	command.add("STRLEN")
		.add("mykey");

	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(command);

		assert(result == 11);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSREM()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("myset");

	Command sadd = Command::sadd("myset", "one");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	sadd = Command::sadd("myset", "two");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	sadd = Command::sadd("myset", "three");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command srem = Command::srem("myset", "one");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(srem);
		assert(result == 1);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	srem = Command::srem("myset", "four");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(srem);
		assert(result == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("myset");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSUNION()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("key1");
	delKey("key2");

	std::vector<std::string> values1;
	values1.push_back("a");
	values1.push_back("b");
	values1.push_back("c");
	Command sadd = Command::sadd("key1", values1);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values2;
	values2.push_back("c");
	values2.push_back("d");
	values2.push_back("e");
	sadd = Command::sadd("key2", values2);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sunion = Command::sunion("key1", "key2");
	try
	{
		Array result = _redis.execute<Array>(sunion);
		assert(result.size() == 5);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testSUNIONSTORE()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	delKey("key");
	delKey("key1");
	delKey("key2");

	std::vector<std::string> values1;
	values1.push_back("a");
	values1.push_back("b");
	values1.push_back("c");
	Command sadd = Command::sadd("key1", values1);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	std::vector<std::string> values2;
	values2.push_back("c");
	values2.push_back("d");
	values2.push_back("e");
	sadd = Command::sadd("key2", values2);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sadd);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command sunionstore = Command::sunionstore("key", "key1", "key2");
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(sunionstore);
		assert(result == 5);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command smembers = Command::smembers("key");
	try
	{
		Array result = _redis.execute<Array>(smembers);
		assert(result.size() == 5);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testRENAME()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command set = Command::set("mykey", "Hello");
	try
	{
		std::string result = _redis.execute<std::string>(set);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command rename = Command::rename("mykey", "myotherkey");
	try
	{
		std::string result = _redis.execute<std::string>(rename);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command get = Command::get("myotherkey");
	try
	{
		BulkString result = _redis.execute<BulkString>(get);
		assert(result.value().compare("Hello") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testRENAMENX()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	Command set = Command::set("mykey", "Hello");
	try
	{
		std::string result = _redis.execute<std::string>(set);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	set = Command::set("myotherkey", "World");
	try
	{
		std::string result = _redis.execute<std::string>(set);
		assert(result.compare("OK") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command rename = Command::rename("mykey", "myotherkey", false);
	try
	{
		Poco::Int64 result = _redis.execute<Poco::Int64>(rename);
		assert(result == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command get = Command::get("myotherkey");
	try
	{
		BulkString result = _redis.execute<BulkString>(get);
		assert(result.value().compare("World") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testRPOP()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command rpush = Command::rpush("mylist", "one");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "two");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);

		rpush = Command::rpush("mylist", "three");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command rpop = Command::rpop("mylist");
	try
	{
		BulkString result = _redis.execute<BulkString>(rpop);
		assert(result.value().compare("three") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("one") == 0);
		assert(result.get<BulkString>(1).value().compare("two") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}

}


void RedisTest::testRPOPLPUSH()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the lists are not there yet ...
	std::vector<std::string> lists;
	lists.push_back("mylist");
	lists.push_back("myotherlist");
	Command delCommand = Command::del(lists);
	try
	{
		_redis.execute<Poco::Int64>(delCommand);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	try
	{
		Command rpush = Command::rpush("mylist", "one");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "two");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);

		rpush = Command::rpush("mylist", "three");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 3);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command rpoplpush = Command::rpoplpush("mylist", "myotherlist");
	try
	{
		BulkString result = _redis.execute<BulkString>(rpoplpush);
		assert(result.value().compare("three") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("one") == 0);
		assert(result.get<BulkString>(1).value().compare("two") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}

	lrange = Command::lrange("myotherlist");
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 1);
		assert(result.get<BulkString>(0).value().compare("three") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testRPUSH()
{
	if (!_connected)
	{
		std::cout << "Not connected, test skipped." << std::endl;
		return;
	}

	// Make sure the list is not there yet ...
	delKey("mylist");

	try
	{
		Command rpush = Command::rpush("mylist", "World");
		Poco::Int64 result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 1);

		rpush = Command::rpush("mylist", "Hello");
		result = _redis.execute<Poco::Int64>(rpush);
		assert(result == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}

	Command llen = Command::llen("mylist");
	try
	{
		Poco::Int64 n = _redis.execute<Poco::Int64>(llen);
		assert(n == 2);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}

	Command lrange = Command::lrange("mylist", 0, -1);
	try
	{
		Array result = _redis.execute<Array>(lrange);

		assert(result.size() == 2);
		assert(result.get<BulkString>(0).value().compare("World") == 0);
		assert(result.get<BulkString>(1).value().compare("Hello") == 0);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::NullValueException& e)
	{
		fail(e.message());
	}
}


void RedisTest::testPool()
{
	Poco::Net::SocketAddress sa(_host, _port);
	Poco::PoolableObjectFactory<Client, Client::Ptr> factory(sa);
	Poco::ObjectPool<Client, Client::Ptr> pool(factory, 10, 15);

	delKey("mypoolkey");

	PooledConnection pclient1(pool);
	PooledConnection pclient2(pool);
	assert(pool.size() == 2);

	Command set = Command::set("mypoolkey", "Hello");
	std::string result = ((Client::Ptr) pclient1)->execute<std::string>(set);
	assert(result.compare("OK") == 0);

	Array get;
	get << "GET" << "mypoolkey";
	BulkString keyValue = ((Client::Ptr) pclient2)->execute<BulkString>(get);
	assert(keyValue.value().compare("Hello") == 0);
}


void RedisTest::delKey(const std::string& key)
{
	Command delCommand = Command::del(key);
	try
	{
		_redis.execute<Poco::Int64>(delCommand);
	}
	catch (RedisException& e)
	{
		fail(e.message());
	}
	catch (Poco::BadCastException& e)
	{
		fail(e.message());
	}
}


CppUnit::Test* RedisTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("RedisTest");

	CppUnit_addTest(pSuite, RedisTest, testAPPEND);
	CppUnit_addTest(pSuite, RedisTest, testBLPOP);
	CppUnit_addTest(pSuite, RedisTest, testBRPOP);
	CppUnit_addTest(pSuite, RedisTest, testDECR);
	CppUnit_addTest(pSuite, RedisTest, testDECR);
	CppUnit_addTest(pSuite, RedisTest, testECHO);
	CppUnit_addTest(pSuite, RedisTest, testError);
	CppUnit_addTest(pSuite, RedisTest, testEVAL);
	CppUnit_addTest(pSuite, RedisTest, testHDEL);
	CppUnit_addTest(pSuite, RedisTest, testHEXISTS);
	CppUnit_addTest(pSuite, RedisTest, testHGETALL);
	CppUnit_addTest(pSuite, RedisTest, testHINCRBY);
	CppUnit_addTest(pSuite, RedisTest, testHKEYS);
	CppUnit_addTest(pSuite, RedisTest, testHMGET);
	CppUnit_addTest(pSuite, RedisTest, testHMSET);
	CppUnit_addTest(pSuite, RedisTest, testHSET);
	//CppUnit_addTest(pSuite, RedisTest, testHSTRLEN);
	CppUnit_addTest(pSuite, RedisTest, testHVALS);
	CppUnit_addTest(pSuite, RedisTest, testINCR);
	CppUnit_addTest(pSuite, RedisTest, testINCRBY);
	CppUnit_addTest(pSuite, RedisTest, testLINDEX);
	CppUnit_addTest(pSuite, RedisTest, testLINSERT);
	CppUnit_addTest(pSuite, RedisTest, testLPOP);
	CppUnit_addTest(pSuite, RedisTest, testLREM);
	CppUnit_addTest(pSuite, RedisTest, testLSET);
	CppUnit_addTest(pSuite, RedisTest, testLTRIM);
	CppUnit_addTest(pSuite, RedisTest, testMSET);
	CppUnit_addTest(pSuite, RedisTest, testMSETWithMap);
	CppUnit_addTest(pSuite, RedisTest, testMULTI);
	CppUnit_addTest(pSuite, RedisTest, testPING);
	CppUnit_addTest(pSuite, RedisTest, testPipeliningWithSendCommands);
	CppUnit_addTest(pSuite, RedisTest, testPipeliningWithWriteCommand);
	CppUnit_addTest(pSuite, RedisTest, testPubSub);
	CppUnit_addTest(pSuite, RedisTest, testSADD);
	CppUnit_addTest(pSuite, RedisTest, testSCARD);
	CppUnit_addTest(pSuite, RedisTest, testSDIFF);
	CppUnit_addTest(pSuite, RedisTest, testSDIFFSTORE);
	CppUnit_addTest(pSuite, RedisTest, testSET);
	CppUnit_addTest(pSuite, RedisTest, testSINTER);
	CppUnit_addTest(pSuite, RedisTest, testSINTERSTORE);
	CppUnit_addTest(pSuite, RedisTest, testSISMEMBER);
	CppUnit_addTest(pSuite, RedisTest, testSMEMBERS);
	CppUnit_addTest(pSuite, RedisTest, testSMOVE);
	CppUnit_addTest(pSuite, RedisTest, testSPOP);
	CppUnit_addTest(pSuite, RedisTest, testSRANDMEMBER);
	CppUnit_addTest(pSuite, RedisTest, testSREM);
	CppUnit_addTest(pSuite, RedisTest, testSTRLEN);
	CppUnit_addTest(pSuite, RedisTest, testSUNION);
	CppUnit_addTest(pSuite, RedisTest, testSUNIONSTORE);
	CppUnit_addTest(pSuite, RedisTest, testRENAME);
	CppUnit_addTest(pSuite, RedisTest, testRENAMENX);
	CppUnit_addTest(pSuite, RedisTest, testRPOP);
	CppUnit_addTest(pSuite, RedisTest, testRPOPLPUSH);
	CppUnit_addTest(pSuite, RedisTest, testRPUSH);
	CppUnit_addTest(pSuite, RedisTest, testPool);
	return pSuite;
}
