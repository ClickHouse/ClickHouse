#ifndef MYSQLXX_CONNECTION_H
#define MYSQLXX_CONNECTION_H

#include <boost/noncopyable.hpp>

#include <Poco/Util/Application.h>

#include <mysqlxx/Query.h>


namespace mysqlxx
{

class Connection : private boost::noncopyable
{
public:
	Connection();

	Connection(
		const char* db,
		const char* server = 0,
		const char* user = 0,
		const char* password = 0,
		unsigned int port = 0);

	Connection(const std::string & config_name)
	{
		is_connected = false;
		Poco::Util::LayeredConfiguration & cfg = Poco::Util::Application::instance().config();

		std::string db 			= cfg.getString(config_name + ".db");
		std::string server 		= cfg.getString(config_name + ".host");
		std::string user 		= cfg.getString(config_name + ".user");
		std::string password	= cfg.getString(config_name + ".password");
		unsigned port			= cfg.getInt(config_name + ".port");

		connect(db.c_str(), server.c_str(), user.c_str(), password.c_str(), port);
	}

	virtual ~Connection();

	virtual void connect(const char* db,
		const char* server = 0,
		const char* user = 0,
		const char* password = 0,
		unsigned int port = 0);

	bool connected() const;
	void disconnect();
	bool ping();
	Query query(const std::string & str = "");
	MYSQL & getDriver();

private:
	MYSQL driver;
	bool is_connected;
};


}

#endif
