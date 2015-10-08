#pragma once

#include <sstream>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>


/** Почему-то при методе POST, Poco::Net::HTMLForm не считывает параметры из URL, а считывает только из тела.
 * Этот помошник позволяет считывать параметры только из URL.
 */
struct HTMLForm : public Poco::Net::HTMLForm
{
	HTMLForm(Poco::Net::HTTPRequest & request)
	{
		Poco::URI uri(request.getURI());
		std::istringstream istr(uri.getRawQuery());
		readUrl(istr);
	}

	HTMLForm(Poco::URI & uri)
	{
		std::istringstream istr(uri.getRawQuery());
		readUrl(istr);
	}
};
