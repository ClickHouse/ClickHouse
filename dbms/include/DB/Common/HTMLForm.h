#pragma once

#include <sstream>
#include <Poco/Net/HTMLForm.h>
#include <Poco/Net/HTTPRequest.h>
#include <Poco/URI.h>


/** Somehow, in case of POST, Poco::Net::HTMLForm doesn't read parameters from URL, only from body.
  * This helper allows to read parameters just from URL.
  */
struct HTMLForm : public Poco::Net::HTMLForm
{
	HTMLForm(const Poco::Net::HTTPRequest & request)
	{
		Poco::URI uri(request.getURI());
		std::istringstream istr(uri.getRawQuery());
		readUrl(istr);
	}

	HTMLForm(const Poco::URI & uri)
	{
		std::istringstream istr(uri.getRawQuery());
		readUrl(istr);
	}
};
