#pragma once
#include <DB/Common/Exception.h>
#include <zkutil/Types.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Core/ErrorCodes.h>

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

	const int32_t code;

private:
	static void incrementEventCounter()
	{
		ProfileEvents::increment(ProfileEvents::ZooKeeperExceptions);
	}

};

};
