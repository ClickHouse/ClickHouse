//
// Page.h
//
// $Id: //poco/1.4/PageCompiler/src/Page.h#2 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Page_INCLUDED
#define Page_INCLUDED


#include "Poco/Net/NameValueCollection.h"
#include <sstream>


class Page: public Poco::Net::NameValueCollection
	/// This class represents a server page consisting of
	/// handler code and declarations, as well as page attributes.
{
public:
	Page();
		/// Creates a Page.
	
	~Page();
		/// Destroys the Page.

	std::stringstream& headerDecls();
		/// Returns the user-specified declarations for the header file.

	const std::stringstream& headerDecls() const;
		/// Returns the user-specified declarations for the header file.

	std::stringstream& implDecls();
		/// Returns the user-specified declarations for the source file.

	const std::stringstream& implDecls() const;
		/// Returns the user-specified declarations for the source file.

	std::stringstream& handler();
		/// Returns the request handler code.

	const std::stringstream& handler() const;
		/// Returns the request prehandler code.

	std::stringstream& preHandler();
		/// Returns the request handler code.

	const std::stringstream& preHandler() const;
		/// Returns the request prehandler code.
		
	bool getBool(const std::string& property, bool deflt = false) const;
		/// Returns the boolean value of the given property.
		///
		/// The return value will be true if the property
		/// has one of the following values:
		///    - true
		///    - yes
		///    - on
		///
		/// Otherwise, the return value will be false.
		
	int getInt(const std::string& property, int deflt = 0) const;
		/// Returns the integer value of the given property.
		
private:
	Page(const Page&);
	Page& operator = (const Page&);

	std::stringstream _headerDecls;
	std::stringstream _implDecls;
	std::stringstream _handler;
	std::stringstream _preHandler;
};


//
// inlines
//
inline std::stringstream& Page::headerDecls()
{
	return _headerDecls;
}


inline const std::stringstream& Page::headerDecls() const
{
	return _headerDecls;
}


inline std::stringstream& Page::implDecls()
{
	return _implDecls;
}


inline const std::stringstream& Page::implDecls() const
{
	return _implDecls;
}


inline std::stringstream& Page::handler()
{
	return _handler;
}


inline const std::stringstream& Page::handler() const
{
	return _handler;
}


inline std::stringstream& Page::preHandler()
{
	return _preHandler;
}


inline const std::stringstream& Page::preHandler() const
{
	return _preHandler;
}


#endif // Page_INCLUDED
