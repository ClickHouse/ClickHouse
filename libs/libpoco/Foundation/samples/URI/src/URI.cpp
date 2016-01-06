//
// URI.cpp
//
// $Id: //poco/1.4/Foundation/samples/URI/src/URI.cpp#1 $
//
// This sample demonstrates the URI class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/URI.h"
#include <iostream>


using Poco::URI;


int main(int argc, char** argv)
{
	URI uri1("http://www.appinf.com:81/sample?example-query#somewhere");
	
	std::cout << "Scheme:    " << uri1.getScheme() << std::endl
	          << "Authority: " << uri1.getAuthority() << std::endl
	          << "Path:      " << uri1.getPath() << std::endl
	          << "Query:     " << uri1.getQuery() << std::endl
	          << "Fragment:  " << uri1.getFragment() << std::endl;
	          
	URI uri2;
	uri2.setScheme("https");
	uri2.setAuthority("www.appinf.com");
	uri2.setPath("/another sample");
	
	std::cout << uri2.toString() << std::endl;
	
	return 0;
}
