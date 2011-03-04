#ifndef MYSQLXX_CONNECTION_H
#define MYSQLXX_CONNECTION_H

#include <boost/noncopyable.hpp>

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

	Connection(const std::string & config_name);

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
