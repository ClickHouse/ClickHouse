// Text.cpp
//
// This sample demonstrates the generation and saving of a text PDF
// document, using external TTF font and UTF-8 encoding.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#include "Poco/PDF/Document.h"
#include "Poco/Path.h"
#include "Poco/File.h"


#if defined(POCO_OS_FAMILY_UNIX)
const std::string fileName = "${POCO_BASE}/PDF/samples/Text/Text.pdf";
#elif defined(POCO_OS_FAMILY_WINDOWS)
const std::string fileName = "%POCO_BASE%/PDF/samples/Text/Text.pdf";
#endif


using Poco::PDF::Document;
using Poco::PDF::Font;
using Poco::PDF::Page;
using Poco::Path;
using Poco::File;


int main(int argc, char** argv)
{
	File file(Path::expand(fileName));
	if (file.exists()) file.remove();

	Document document(file.path());

	Font font = document.font(document.loadTTFont("DejaVuLGCSans.ttf", true), "UTF-8");
	Page page = document[0];
	page.setFont(font, 24);
	std::string hello = "Hello PDF World from C++ Portable Components";
	float tw = page.textWidth(hello);
	page.writeOnce((page.getWidth() - tw) / 2, page.getHeight() - 50, hello);
	page.setFont(font, 14);
	hello = "~ Courtesy of G\xC3\xBCnter Obiltschnig & Aleksandar Fabijani\xC4\x87 ~";
	tw = page.textWidth(hello);
	page.writeOnce((page.getWidth() - tw) / 2, page.getHeight() - 100, hello);
	document.save();
	return 0;
}
