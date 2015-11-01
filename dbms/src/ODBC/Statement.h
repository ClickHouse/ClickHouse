#pragma once

#include <sstream>

#include <Poco/Base64Encoder.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/Net/HTTPResponse.h>

#include "Connection.h"
#include "ResultSet.h"


/// Информация, куда и как складывать значения при чтении.
struct Binding
{
	SQLSMALLINT target_type;
	PTR out_value;
	SQLLEN out_value_max_size;
	SQLLEN * out_value_size_or_indicator;
};


class Statement
{
public:
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
		request.setURI("/?database=" + connection.database + "&default_format=ODBC");	/// TODO Возможность передать настройки. TODO эскейпинг

		connection.session.sendRequest(request) << query;
		in = &connection.session.receiveResponse(response);

		Poco::Net::HTTPResponse::HTTPStatus status = response.getStatus();

		if (status != Poco::Net::HTTPResponse::HTTP_OK)
		{
			std::stringstream error_message;
			error_message
				<< "Received error:" << std::endl
				<< in->rdbuf() << std::endl
				<< "HTTP status code: " << status << ".";

			throw std::runtime_error(error_message.str());
		}

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

	std::map<SQLUSMALLINT, Binding> bindings;
};
