//
// CodeWriter.h
//
// $Id: //poco/1.4/PageCompiler/src/CodeWriter.h#1 $
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef CodeWriter_INCLUDED
#define CodeWriter_INCLUDED


#include "Poco/Poco.h"
#include <ostream>


class Page;


class CodeWriter
	/// This class implements the code generator for
	/// generating C++ header and implementation files 
	/// from C++ Server Pages.
{
public:
	CodeWriter(const Page& page, const std::string& clazz);
		/// Creates the CodeWriter, using the given Page.

	virtual ~CodeWriter();
		/// Destroys the PageReader.

	virtual void writeHeader(std::ostream& ostr, const std::string& headerFileName);
		/// Writes the header file contents to the given stream.

	virtual void writeImpl(std::ostream& ostr, const std::string& headerFileName);
		/// Writes the implementation file contents to the given stream.

	const Page& page() const;
		/// Returns a const reference to the Page.
		
	const std::string& clazz() const;
		/// Returns the name of the handler class.

protected:
	virtual void writeHeaderIncludes(std::ostream& ostr);
	virtual void writeHandlerClass(std::ostream& ostr);
	virtual void writeHandlerMembers(std::ostream& ostr);
	virtual void writeFactoryClass(std::ostream& ostr);
	virtual void writeImplIncludes(std::ostream& ostr);
	virtual void writeConstructor(std::ostream& ostr);
	virtual void writeHandler(std::ostream& ostr);
	virtual void writeFactory(std::ostream& ostr);
	virtual void writeSession(std::ostream& ostr);
	virtual void writeForm(std::ostream& ostr);
	virtual void writeResponse(std::ostream& ostr);
	virtual void writeContent(std::ostream& ostr);
	virtual void writeManifest(std::ostream& ostr);
	
	void beginGuard(std::ostream& ostr, const std::string& headerFileName);
	void endGuard(std::ostream& ostr, const std::string& headerFileName);
	void beginNamespace(std::ostream& ostr);
	void endNamespace(std::ostream& ostr);
	void handlerClass(std::ostream& ostr, const std::string& base, const std::string& ctorArg);
	void factoryClass(std::ostream& ostr, const std::string& base);
	void factoryImpl(std::ostream& ostr, const std::string& arg);

private:
	CodeWriter();
	CodeWriter(const CodeWriter&);
	CodeWriter& operator = (const CodeWriter&);

	const Page& _page;
	std::string _class;
};


//
// inlines
//
inline const Page& CodeWriter::page() const
{
	return _page;
}


inline const std::string& CodeWriter::clazz() const
{
	return _class;
}


#endif // CodeWriter_INCLUDED
