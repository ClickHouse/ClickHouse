//
// Image.cpp
//
// This sample demonstrates the generation and saving of a PDF
// document that contains an mbedded image loaded from external file.
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
const std::string pdfFileName = "${POCO_BASE}/PDF/samples/Image/Image.pdf";
const std::string pngFileName = "${POCO_BASE}/PDF/samples/Image/logo.png";
#elif defined(POCO_OS_FAMILY_WINDOWS)
const std::string pdfFileName = "%POCO_BASE%/PDF/samples/Image/Image.pdf";
const std::string pngFileName = "%POCO_BASE%/PDF/samples/Image/logo.PNG";
#endif


using Poco::PDF::Document;
using Poco::PDF::Font;
using Poco::PDF::Page;
using Poco::PDF::Image;
using Poco::Path;
using Poco::File;


int main(int argc, char** argv)
{
	File pdfFile(Path::expand(pdfFileName));
	if (pdfFile.exists()) pdfFile.remove();
	File pngFile(Path::expand(pngFileName));

	Document document(pdfFile.path());
	
	Page page = document[0];
	Font helv = document.font("Helvetica");
	page.setFont(helv, 24);

	std::string hello = "Hello PDF World from ";
	float textWidth = page.textWidth(hello);
	float textBegin = (page.getWidth() - textWidth) / 2;
	page.writeOnce(textBegin, page.getHeight() - 50, hello);

	Image image = document.loadPNGImage(pngFile.path());
	page.drawImage(image, textBegin + textWidth / 2 - image.width() / 2,
		page.getHeight() - 100 - image.height(),
		image.width(),
		image.height());

	document.save();
	return 0;
}
