//
// LineEndingConverter.cpp
//
// $Id: //poco/1.4/Foundation/samples/LineEndingConverter/src/LineEndingConverter.cpp#1 $
//
// This sample demonstrates the line ending conversion from unix (LF) to windows (CRLF)
// and vice-versa.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/LineEndingConverter.h"
#include "Poco/FileStream.h"
#include "Poco/StreamCopier.h"
#include "Poco/String.h"
#include <sstream>
#include <iostream>


using Poco::InputLineEndingConverter;
using Poco::LineEnding;
using Poco::StreamCopier;
using Poco::FileInputStream;
using Poco::FileOutputStream;
using Poco::icompare;


inline void dosToUnix(std::istream& input, std::ostream& output)
{
	InputLineEndingConverter conv(input, LineEnding::NEWLINE_LF);
	StreamCopier::copyStream(conv, output);
}


inline void unixToDos(std::istream& input, std::ostream& output)
{
	InputLineEndingConverter conv(input, LineEnding::NEWLINE_CRLF);
	StreamCopier::copyStream(conv, output);
}


inline int usage()
{
	std::cout << "Usage: LineEndingConverter {u2d | d2u} filename" << std::endl;
	return -1;
}


int main(int argc, char** argv)
{
	if (argc < 3) return usage();
	if (strlen(argv[1]) != 3) return usage();

	std::string conv(argv[1]);

	FileInputStream fis(argv[2]);
	std::stringstream ss;
	StreamCopier::copyStream(fis, ss);
	fis.close();

	FileOutputStream fos(argv[2]);
	if (0 == icompare(conv, "u2d")) unixToDos(ss, fos);
	else if (0 == icompare(conv, "d2u")) dosToUnix(ss, fos);
	else return usage();
	fos.flush();

	return 0;
}
