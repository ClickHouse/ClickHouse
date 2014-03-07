#pragma once
#include <Poco/Exception.h>
#include <zkutil/Types.h>


namespace zkutil
{

class KeeperException : public Poco::Exception
{
public:
	KeeperException(const std::string & msg) : Poco::Exception(msg), code(ReturnCode::Ok) {}
	KeeperException(const std::string & msg, ReturnCode::type code_)
		: Poco::Exception(msg + " (" + ReturnCode::toString(code_) + ")"), code(code_) {}
	KeeperException(ReturnCode::type code_)
		: Poco::Exception(ReturnCode::toString(code_)), code(code_) {}

	ReturnCode::type code;
};

};
