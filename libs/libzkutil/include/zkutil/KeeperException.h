#pragma once
#include <statdaemons/Exception.h>
#include <zkutil/Types.h>
#include <DB/Common/ProfileEvents.h>


namespace zkutil
{

class KeeperException : public DB::Exception
{
public:
	KeeperException(const std::string & msg) : DB::Exception(msg), code(ZOK) { incrementEventCounter(); }
	KeeperException(const std::string & msg, int32_t code_)
		: DB::Exception(msg + " (" + zerror(code_) + ")"), code(code_) { incrementEventCounter(); }
	KeeperException(int32_t code_)
		: DB::Exception(zerror(code_)), code(code_) { incrementEventCounter(); }
	KeeperException(int32_t code_, const std::string & path_)
		: DB::Exception(std::string(zerror(code_)) + ", path: " + path_), code(code_) { incrementEventCounter(); }
	KeeperException(const KeeperException & exc) : DB::Exception(exc), code(exc.code) { incrementEventCounter(); }

	const char * name() const throw() { return "zkutil::KeeperException"; }
	const char * className() const throw() { return "zkutil::KeeperException"; }
	KeeperException * clone() const { return new KeeperException(*this); }

	/// при этих ошибках надо переинициализировать сессию с zookeeper
	bool isUnrecoverable() const
	{
		return code == ZINVALIDSTATE || code == ZSESSIONEXPIRED;
	}
	int32_t code;

private:
	void incrementEventCounter()
	{
		ProfileEvents::increment(ProfileEvents::ZooKeeperExceptions);
	}

};

};
