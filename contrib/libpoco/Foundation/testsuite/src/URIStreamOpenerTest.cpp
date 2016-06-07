//
// URIStreamOpenerTest.cpp
//
// $Id: //poco/1.4/Foundation/testsuite/src/URIStreamOpenerTest.cpp#1 $
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "URIStreamOpenerTest.h"
#include "CppUnit/TestCaller.h"
#include "CppUnit/TestSuite.h"
#include "Poco/URIStreamOpener.h"
#include "Poco/URIStreamFactory.h"
#include "Poco/URI.h"
#include "Poco/TemporaryFile.h"
#include "Poco/Path.h"
#include <fstream>
#include <sstream>


using Poco::URIStreamOpener;
using Poco::URIStreamFactory;
using Poco::URI;
using Poco::TemporaryFile;
using Poco::Path;


namespace
{
	class StringStreamFactory: public URIStreamFactory
	{
	public:
		StringStreamFactory()
		{
		}
		
		std::istream* open(const URI& uri)
		{
			return new std::istringstream(uri.toString());
		}
	};
}


URIStreamOpenerTest::URIStreamOpenerTest(const std::string& name): CppUnit::TestCase(name)
{
}


URIStreamOpenerTest::~URIStreamOpenerTest()
{
}


void URIStreamOpenerTest::testStreamOpenerFile()
{
	TemporaryFile tempFile;
	std::string path = tempFile.path();
	std::ofstream ostr(path.c_str());
	assert (ostr.good());
	ostr << "Hello, world!" << std::endl;
	ostr.close();
	
	URI uri;
	uri.setScheme("file");
	uri.setPath(Path(path).toString(Path::PATH_UNIX));
	std::string uriString = uri.toString();
	
	URIStreamOpener opener;
	std::istream* istr = opener.open(uri);
	assert (istr != 0);
	assert (istr->good());
	delete istr;
}


void URIStreamOpenerTest::testStreamOpenerRelative()
{
	TemporaryFile tempFile;
	std::string path = tempFile.path();
	std::ofstream ostr(path.c_str());
	assert (ostr.good());
	ostr << "Hello, world!" << std::endl;
	ostr.close();
	
	URI uri(Path(path).toString(Path::PATH_UNIX));
	std::string uriString = uri.toString();
	
	URIStreamOpener opener;
	std::istream* istr = opener.open(uri);
	assert (istr != 0);
	assert (istr->good());
	delete istr;
}


void URIStreamOpenerTest::testStreamOpenerURI()
{
	TemporaryFile tempFile;
	std::string path = tempFile.path();
	std::ofstream ostr(path.c_str());
	assert (ostr.good());
	ostr << "Hello, world!" << std::endl;
	ostr.close();
	
	URI uri;
	uri.setScheme("file");
	uri.setPath(Path(path).toString(Path::PATH_UNIX));
	std::string uriString = uri.toString();
	
	URIStreamOpener opener;
	std::istream* istr = opener.open(uriString);
	assert (istr != 0);
	assert (istr->good());
	delete istr;
}


void URIStreamOpenerTest::testStreamOpenerURIResolve()
{
	TemporaryFile tempFile;
	std::string path = tempFile.path();
	std::ofstream ostr(path.c_str());
	assert (ostr.good());
	ostr << "Hello, world!" << std::endl;
	ostr.close();
	
	Path p(path);
	p.makeAbsolute();
	Path parent(p.parent());
	
	URI uri;
	uri.setScheme("file");
	uri.setPath(parent.toString(Path::PATH_UNIX));
	std::string uriString = uri.toString();
	
	URIStreamOpener opener;
	std::istream* istr = opener.open(uriString, p.getFileName());
	assert (istr != 0);
	assert (istr->good());
	delete istr;
}


void URIStreamOpenerTest::testStreamOpenerPath()
{
	TemporaryFile tempFile;
	std::string path = tempFile.path();
	std::ofstream ostr(path.c_str());
	assert (ostr.good());
	ostr << "Hello, world!" << std::endl;
	ostr.close();
		
	URIStreamOpener opener;
	std::istream* istr = opener.open(path);
	assert (istr != 0);
	assert (istr->good());
	delete istr;
}


void URIStreamOpenerTest::testStreamOpenerPathResolve()
{
	TemporaryFile tempFile;
	std::string path = tempFile.path();
	std::ofstream ostr(path.c_str());
	assert (ostr.good());
	ostr << "Hello, world!" << std::endl;
	ostr.close();
	
	Path p(path);
	Path parent(p.parent());
	std::string base = parent.toString();
		
	URIStreamOpener opener;
	std::istream* istr = opener.open(base, p.getFileName());
	assert (istr != 0);
	assert (istr->good());
	delete istr;
}


void URIStreamOpenerTest::testRegisterUnregister()
{
	URIStreamOpener opener;
	assert (!opener.supportsScheme("string"));
	opener.registerStreamFactory("string", new StringStreamFactory);
	assert (opener.supportsScheme("string"));
	URI uri("string:foobar");
	std::istream* istr = opener.open(uri);
	assert (istr != 0);
	assert (istr->good());
	delete istr;
	opener.unregisterStreamFactory("string");
	assert (!opener.supportsScheme("string"));
}


void URIStreamOpenerTest::setUp()
{
}


void URIStreamOpenerTest::tearDown()
{
}


CppUnit::Test* URIStreamOpenerTest::suite()
{
	CppUnit::TestSuite* pSuite = new CppUnit::TestSuite("URIStreamOpenerTest");

	CppUnit_addTest(pSuite, URIStreamOpenerTest, testStreamOpenerFile);
	CppUnit_addTest(pSuite, URIStreamOpenerTest, testStreamOpenerRelative);
	CppUnit_addTest(pSuite, URIStreamOpenerTest, testStreamOpenerURI);
	CppUnit_addTest(pSuite, URIStreamOpenerTest, testStreamOpenerURIResolve);
	CppUnit_addTest(pSuite, URIStreamOpenerTest, testStreamOpenerPath);
	CppUnit_addTest(pSuite, URIStreamOpenerTest, testStreamOpenerPathResolve);
	CppUnit_addTest(pSuite, URIStreamOpenerTest, testRegisterUnregister);

	return pSuite;
}
