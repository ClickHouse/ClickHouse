#pragma once

#include <sstream>

#include <Poco/Base64Encoder.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include "Connection.h"
#include "ResultSet.h"


struct Statement
{
	Statement(Connection & conn_) : connection(conn_) {}

	void sendRequest()
	{
		if (connection.user.find(':') != std::string::npos)
			throw std::runtime_error("Username couldn't contain ':' (colon) symbol.");

		std::ostringstream user_password_base64;
		Poco::Base64Encoder base64_encoder(user_password_base64);
		base64_encoder << connection.user << ":" << connection.password;
		base64_encoder.close();

		request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
		request.setCredentials("Basic", user_password_base64.str());
		request.setURI("/?default_format=ODBC");	/// TODO Возможность передать настройки.

		connection.session.sendRequest(request) << query;
		in = &connection.session.receiveResponse(response);

		result.init(*this);
	}

	bool fetchRow()
	{
		current_row = result.fetch();
		return current_row;
	}

	Connection & connection;
	std::string query;
	Poco::Net::HTTPRequest request;
	Poco::Net::HTTPResponse response;
	std::istream * in;

	DiagnosticRecord diagnostic_record;

	ResultSet result;
	Row current_row;
};
