//
// main.cpp
//


#include "Poco/PDF/XMLTemplate.h"
#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/Exception.h"
#include <iostream>


#if defined(POCO_OS_FAMILY_UNIX)
const std::string pdfFileName = "${POCO_BASE}/PDF/samples/Template/Report.pdf";
const std::string xmlFileName = "${POCO_BASE}/PDF/samples/Template/Template.xml";
#elif defined(POCO_OS_FAMILY_WINDOWS)
const std::string pdfFileName = "%POCO_BASE%/PDF/samples/Template/Report.pdf";
const std::string xmlFileName = "%POCO_BASE%/PDF/samples/Template/Template.xml";
#endif


using Poco::Path;
using Poco::File;


int main(int argc, char** argv)
{
	try
	{
		Poco::PDF::XMLTemplate xmlTemplate(Path::expand(xmlFileName));
		xmlTemplate.create(Path::expand(pdfFileName));
	}
	catch (Poco::Exception& exc)
	{
		std::cerr << "ERROR: " << exc.displayText() << std::endl;
	}

	return 0;
}
