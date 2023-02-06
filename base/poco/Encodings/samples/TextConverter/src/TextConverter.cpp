//
// TextConverter.cpp
//
// This sample demonstrates the text encodings support in POCO.
//
// Copyright (c) 2018, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/StreamConverter.h"
#include "Poco/StreamCopier.h"
#include "Poco/TextEncoding.h"
#include "Poco/Encodings.h"
#include <iostream>


inline int usage()
{
	std::cout << "Usage: TextConverter <inEncoding> <outEncoding>" << std::endl;
	return 1;
}


int main(int argc, char** argv)
{
	if (argc < 3) return usage();

	try
	{
		Poco::registerExtraEncodings(); // register encodings from the PocoEncodings library

		std::string inEncodingName(argv[1]);
		std::string outEncodingName(argv[2]);

		Poco::TextEncoding& inEncoding = Poco::TextEncoding::byName(inEncodingName);
		Poco::TextEncoding& outEncoding = Poco::TextEncoding::byName(outEncodingName);

		Poco::OutputStreamConverter conv(std::cout, inEncoding, outEncoding);
		Poco::StreamCopier::copyStream(std::cin, conv);
	}
	catch (Poco::Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 2;
	}

	return 0;
}
