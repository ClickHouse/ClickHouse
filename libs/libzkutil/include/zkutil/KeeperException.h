#pragma once
#include <statdaemons/Exception.h>
#include <zkutil/Types.h>


namespace zkutil
{

class KeeperException : public DB::Exception
{
public:
	KeeperException(const std::string & msg) : DB::Exception(msg), code(ZOK) {}
	KeeperException(const std::string & msg, int32_t code_)
		: DB::Exception(msg + " (" + zerror(code_) + ")"), code(code_) {}
	KeeperException(int32_t code_)
		: DB::Exception(zerror(code_)), code(code_) {}
	KeeperException(int32_t code_, const std::string & path_)
		: DB::Exception(std::string(zerror(code_)) + ", path: " + path_), code(code_) {}
	KeeperException(const KeeperException & exc) : DB::Exception(exc), code(exc.code) {}

	const char * name() const throw() { return "zkutil::KeeperException"; }
	const char * className() const throw() { return "zkutil::KeeperException"; }
	KeeperException * clone() const { return new KeeperException(*this); }

	int32_t code;
};

};
