//
// OSPCodeWriter.h
//
// $Id: //poco/1.4/PageCompiler/src/OSPCodeWriter.h#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef OSPCodeWriter_INCLUDED
#define OSPCodeWriter_INCLUDED


#include "CodeWriter.h"


class OSPCodeWriter: public CodeWriter
	/// Code generator for OSP Web request handlers.
{
public:
	OSPCodeWriter(const Page& page, const std::string& clazz);
		/// Creates the CodeWriter, using the given Page.

	~OSPCodeWriter();
		/// Destroys the PageReader.

protected:
	virtual void writeHeaderIncludes(std::ostream& ostr);
	virtual void writeHandlerClass(std::ostream& ostr);
	virtual void writeHandlerMembers(std::ostream& ostr);
	virtual void writeFactoryClass(std::ostream& ostr);
	virtual void writeImplIncludes(std::ostream& ostr);
	virtual void writeConstructor(std::ostream& ostr);
	virtual void writeFactory(std::ostream& ostr);
	virtual void writeSession(std::ostream& ostr);
};


#endif // CodeWriter_INCLUDED
