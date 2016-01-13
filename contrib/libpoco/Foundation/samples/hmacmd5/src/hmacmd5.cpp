//
// hmacmd5.cpp
//
// $Id: //poco/1.4/Foundation/samples/hmacmd5/src/hmacmd5.cpp#1 $
//
// This sample demonstrates the HMACEngine class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/HMACEngine.h"
#include "Poco/MD5Engine.h"
#include "Poco/DigestStream.h"
#include "Poco/StreamCopier.h"
#include <fstream>
#include <iostream>


using Poco::DigestEngine;
using Poco::HMACEngine;
using Poco::MD5Engine;
using Poco::DigestOutputStream;
using Poco::StreamCopier;


int main(int argc, char** argv)
{
	if (argc != 3)
	{
		std::cout << "usage: " << argv[0] << ": <passphrase> <input_file>" << std::endl
		          << "       create the HMAC-MD5 for <input_file>, using <passphrase>" << std::endl;
		return 1;
	}
	
	std::string passphrase(argv[1]);
	
	std::ifstream istr(argv[2], std::ios::binary);
	if (!istr)
	{
		std::cerr << "cannot open input file: " << argv[2] << std::endl;
		return 2;
	}
	
	HMACEngine<MD5Engine> hmac(passphrase);
	DigestOutputStream dos(hmac);
	
	StreamCopier::copyStream(istr, dos);
	dos.close();
	
	std::cout << DigestEngine::digestToHex(hmac.digest()) << std::endl;
	
	return 0;
}
