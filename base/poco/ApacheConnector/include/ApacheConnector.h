//
// ApacheConnector.h
//
// Copyright (c) 2006-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef ApacheConnector_ApacheConnector_INCLUDED
#define ApacheConnector_ApacheConnector_INCLUDED


#include <string>


struct request_rec;
class ApacheServerRequest;


class ApacheRequestRec
	/// This class wraps an Apache request_rec.
{
public:
	ApacheRequestRec(request_rec* _pRec);
		/// Creates the ApacheRequestRec;
	
	bool haveRequestBody();
		/// Returns true if the request contains a body.

	int readRequest(char* buffer, int length);
		/// Read up to length bytes from request body into buffer.
		/// Returns the number of bytes read, 0 if eof or -1 if an error occured.

	void writeResponse(const char* buffer, int length);
		/// Writes the given characters as response to the given request_rec.

	void addHeader(const std::string& key, const std::string& value);
		/// Adds the given key / value pair to the outgoing headers of the
		/// http response.

	void setContentType(const std::string& mediaType);
		/// Sets the response content type.

	void redirect(const std::string& uri, int status);
		/// Redirects the response to the given uri.

	void sendErrorResponse(int status);
		/// Sends an error response with the given HTTP status code.
		
	int sendFile(const std::string& path, unsigned int fileSize, const std::string& mediaType);
		/// Sends the file given by fileName as response.

	void copyHeaders(ApacheServerRequest& request);
		/// Copies the request uri and header fields from the Apache request
		/// to the ApacheServerRequest.
		
private:
	request_rec* _pRec;
};


class ApacheConnector
	/// This class provides static methods wrapping the
	/// Apache API.
{
public:
	enum LogLevel
	{
		PRIO_FATAL = 1,   /// A fatal error. The application will most likely terminate. This is the highest priority.
		PRIO_CRITICAL,    /// A critical error. The application might not be able to continue running successfully.
		PRIO_ERROR,       /// An error. An operation did not complete successfully, but the application as a whole is not affected.
		PRIO_WARNING,     /// A warning. An operation completed with an unexpected result.
		PRIO_NOTICE,      /// A notice, which is an information with just a higher priority.
		PRIO_INFORMATION, /// An informational message, usually denoting the successful completion of an operation.
		PRIO_DEBUG,       /// A debugging message.
		PRIO_TRACE        /// A tracing message. This is the lowest priority.
	};

	static void log(const char* file, int line, int level, int status, const char* text);
		/// Log the given message.
};


#endif // ApacheConnector_ApacheConnector_INCLUDED
