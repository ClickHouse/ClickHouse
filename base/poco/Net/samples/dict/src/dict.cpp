//
// dict.cpp
//
// This sample demonstrates the StreamSocket and SocketStream classes.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/StreamSocket.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/StreamCopier.h"
#include "Poco/Path.h"
#include "Poco/Exception.h"
#include <iostream>


using Poco::Net::StreamSocket;
using Poco::Net::SocketStream;
using Poco::Net::SocketAddress;
using Poco::StreamCopier;
using Poco::Path;
using Poco::Exception;


int main(int argc, char** argv)
{
	const std::string HOST("dict.org");
	const unsigned short PORT = 2628;
	
	if (argc != 2)
	{
		Path p(argv[0]);
		std::cout << "usage: " << p.getBaseName() << " <term>" << std::endl;
		std::cout << "       looks up <term> in dict.org and prints the results" << std::endl;
		return 1;
	}
	std::string term(argv[1]);
	
	try
	{
		SocketAddress sa(HOST, PORT);
		StreamSocket sock(sa);
		SocketStream str(sock);
		
		str << "DEFINE ! " << term << "\r\n" << std::flush;
		str << "QUIT\r\n" << std::flush;

		sock.shutdownSend();
		StreamCopier::copyStream(str, std::cout);
	}
	catch (Exception& exc)
	{
		std::cerr << exc.displayText() << std::endl;
		return 1;
	}
	
	return 0;
}
