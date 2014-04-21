#pragma once
#include <statdaemons/Exception.h>
#include <zkutil/Types.h>


namespace zkutil
{

class KeeperException : public DB::Exception
{
public:
	KeeperException(const std::string & msg) : DB::Exception(msg), code(ReturnCode::Ok) {}
	KeeperException(const std::string & msg, ReturnCode::type code_)
		: DB::Exception(msg + " (" + ReturnCode::toString(code_) + ")"), code(code_) {}
	KeeperException(ReturnCode::type code_)
		: DB::Exception(ReturnCode::toString(code_)), code(code_) {}
	KeeperException(ReturnCode::type code_, const std::string & path_)
		: DB::Exception(ReturnCode::toString(code_) + " path: " + path_), code(code_) {}

	const char * name() const throw() { return "zkutil::KeeperException"; }
	const char * className() const throw() { return "zkutil::KeeperException"; }
	KeeperException * clone() const { return new KeeperException(message(), code); }

	ReturnCode::type code;
};

};
