//
// ErrorHandler.h
//
// $Id: //poco/1.4/XML/include/Poco/SAX/ErrorHandler.h#1 $
//
// Library: XML
// Package: SAX
// Module:  SAX
//
// SAX ErrorHandler Interface.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SAX_ErrorHandler_INCLUDED
#define SAX_ErrorHandler_INCLUDED


#include "Poco/XML/XML.h"


namespace Poco {
namespace XML {


class SAXException;


class XML_API ErrorHandler
	/// If a SAX application needs to implement customized error handling, it must 
	/// implement this interface and then register an instance with the XML reader 
	/// using the setErrorHandler method. The parser will then report all errors and 
	/// warnings through this interface.
	/// 
	/// WARNING: If an application does not register an ErrorHandler, XML parsing errors 
	/// will go unreported, except that SAXParseExceptions will be thrown for fatal errors. 
	/// In order to detect validity errors, an ErrorHandler that does something with error() 
	/// calls must be registered.
	/// 
	/// For XML processing errors, a SAX driver must use this interface in preference to 
	/// throwing an exception: it is up to the application to decide whether to throw an 
	/// exception for different types of errors and warnings. Note, however, that there is no 
	/// requirement that the parser continue to report additional errors after a call to 
	/// fatalError. In other words, a SAX driver class may throw an exception after reporting 
	/// any fatalError. Also parsers may throw appropriate exceptions for non-XML errors. For 
	/// example, XMLReader::parse() would throw an IOException for errors accessing entities or 
	/// the document.
{
public:
	virtual void warning(const SAXException& exc) = 0;
		/// Receive notification of a warning.
		/// 
		/// SAX parsers will use this method to report conditions that are not errors or fatal 
		/// errors as defined by the XML recommendation. The default behaviour is to take no action.
		/// 
		/// The SAX parser must continue to provide normal parsing events after invoking this method: 
		/// it should still be possible for the application to process the document through to the end.
		/// 
		/// Filters may use this method to report other, non-XML warnings as well.

	virtual void error(const SAXException& exc) = 0;
		/// Receive notification of a recoverable error.
		/// 
		/// This corresponds to the definition of "error" in section 1.2 of the W3C XML 1.0 
		/// Recommendation. For example, a validating parser would use this callback to report 
		/// the violation of a validity constraint. The default behaviour is to take no action.
		/// 
		/// The SAX parser must continue to provide normal parsing events after invoking this 
		/// method: it should still be possible for the application to process the document through 
		/// to the end. If the application cannot do so, then the parser should report a fatal error 
		/// even if the XML recommendation does not require it to do so.
		/// 
		/// Filters may use this method to report other, non-XML errors as well.

	virtual void fatalError(const SAXException& exc) = 0;
		/// Receive notification of a non-recoverable error.
		/// The application must assume that the document is unusable after the parser has 
		/// invoked this method, and should continue (if at all) only for the sake of collecting 
		/// additional error messages: in fact, SAX parsers are free to stop reporting any other 
		/// events once this method has been invoked.

protected:
	virtual ~ErrorHandler();
};


} } // namespace Poco::XML


#endif // SAX_ErrorHandler_INCLUDED
