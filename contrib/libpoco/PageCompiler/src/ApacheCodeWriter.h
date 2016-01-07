//
// ApacheCodeWriter.h
//
// $Id: //poco/1.4/PageCompiler/src/ApacheCodeWriter.h#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ApacheCodeWriter_INCLUDED
#define ApacheCodeWriter_INCLUDED


#include "CodeWriter.h"


class ApacheCodeWriter: public CodeWriter
	/// Code generator for ApacheConnector request handlers.
{
public:
	ApacheCodeWriter(const Page& page, const std::string& clazz);
		/// Creates the CodeWriter, using the given Page.

	~ApacheCodeWriter();
		/// Destroys the PageReader.

protected:
	virtual void writeHeaderIncludes(std::ostream& ostr);
	virtual void writeFactoryClass(std::ostream& ostr);
	virtual void writeImplIncludes(std::ostream& ostr);
	virtual void writeFactory(std::ostream& ostr);
	virtual void writeManifest(std::ostream& ostr);
};


#endif // CodeWriter_INCLUDED
