#pragma once

#include <sstream>
#include <memory>

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

		Poco::Net::HTTPRequest request;

		request.setMethod(Poco::Net::HTTPRequest::HTTP_POST);
		request.setVersion(Poco::Net::HTTPRequest::HTTP_1_1);
		request.setKeepAlive(true);
		request.setChunkedTransferEncoding(true);
		request.setCredentials("Basic", user_password_base64.str());
		request.setURI("/?database=" + connection.database + "&default_format=ODBC");	/// TODO Возможность передать настройки. TODO эскейпинг

//		if (in && in->peek() != EOF)
			connection.session.reset();

		connection.session.sendRequest(request) << query;

		LOG("Receiving !");
		response.reset(new Poco::Net::HTTPResponse);
		in = &connection.session.receiveResponse(*response);
		LOG("Receiving !!");

		Poco::Net::HTTPResponse::HTTPStatus status = response->getStatus();
		LOG("Receiving !!!");

		if (status != Poco::Net::HTTPResponse::HTTP_OK)
		{
			LOG("Receiving !!!!");

			std::stringstream error_message;
			error_message
				<< "Received error:" << std::endl
				<< in->rdbuf() << std::endl
				<< "HTTP status code: " << status << ".";

			throw std::runtime_error(error_message.str());
		}

		LOG("Receiving ! !");
		result.init(*this);
	}

	bool fetchRow()
	{
		current_row = result.fetch();
		return current_row;
	}

	void reset()
	{
		in = nullptr;
		response.reset();
		connection.session.reset();
		diagnostic_record.reset();
		result = ResultSet();
	}

	Connection & connection;
	std::string query;
	std::unique_ptr<Poco::Net::HTTPResponse> response;
	std::istream * in = nullptr;

	DiagnosticRecord diagnostic_record;

	ResultSet result;
	Row current_row;

	std::map<SQLUSMALLINT, Binding> bindings;
};
