#pragma once

#include <Poco/NumberParser.h>
#include <Poco/Net/HTTPClientSession.h>

#include "StringRef.h"
#include "DiagnosticRecord.h"
#include "Environment.h"
#include "utils.h"


struct Connection
{
	Connection(Environment & env_)
		: environment(env_) {}

	Environment & environment;
	std::string host = "localhost";
	uint16_t port = 8123;
	std::string user = "default";
	std::string password;
	std::string database = "default";

	void init()
	{
		session.setHost(host);
		session.setPort(port);
		session.setKeepAlive(true);
		session.setKeepAliveTimeout(Poco::Timespan(86400, 0));

		/// TODO Timeout.
	}

	void init(
		const std::string & host_,
		const uint16_t port_,
		const std::string & user_,
		const std::string & password_,
		const std::string & database_)
	{
		if (session.connected())
			throw std::runtime_error("Already connected.");

		if (!host_.empty())
			host = host_;
		if (port_)
			port = port_;
		if (!user_.empty())
			user = user_;
		if (!password_.empty())
			password = password_;
		if (!database_.empty())
			database = database_;

		init();
	}

	void init(const std::string & connection_string)
	{
		/// connection_string - string of the form `DSN=ClickHouse;UID=default;PWD=password`

		const char * pos = connection_string.data();
		const char * end = pos + connection_string.size();

		StringRef current_key;
		StringRef current_value;

		while ((pos = nextKeyValuePair(pos, end, current_key, current_value)))
		{
			if (current_key == "UID")
				user = current_value.toString();
			else if (current_key == "PWD")
				password = current_value.toString();
			else if (current_key == "HOST")
				host = current_value.toString();
			else if (current_key == "PORT")
			{
				int int_port = 0;
				if (Poco::NumberParser::tryParse(current_value.toString(), int_port))
					port = int_port;
				else
					throw std::runtime_error("Cannot parse port number.");
			}
			else if (current_key == "DATABASE")
				database = current_value.toString();
		}

		init();
	}

	Poco::Net::HTTPClientSession session;

	DiagnosticRecord diagnostic_record;
};
