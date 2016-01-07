//
// HTTPCookie.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/HTTPCookie.h#2 $
//
// Library: Net
// Package: HTTP
// Module:  HTTPCookie
//
// Definition of the HTTPCookie class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_HTTPCookie_INCLUDED
#define Net_HTTPCookie_INCLUDED


#include "Poco/Net/Net.h"


namespace Poco {
namespace Net {


class NameValueCollection;


class Net_API HTTPCookie
	/// This class represents a HTTP Cookie.
	///
	/// A cookie is a small amount of information sent by a Web 
	/// server to a Web browser, saved by the browser, and later sent back 
	/// to the server. A cookie's value can uniquely identify a client, so 
	/// cookies are commonly used for session management.
	///
	/// A cookie has a name, a single value, and optional attributes such 
	/// as a comment, path and domain qualifiers, a maximum age, and a 
	/// version number.
	///
	/// This class supports both the Version 0 (by Netscape) and Version 1 
	/// (by RFC 2109) cookie specifications. By default, cookies are created 
	/// using Version 0 to ensure the best interoperability.
{
public:
	HTTPCookie();
		/// Creates an empty HTTPCookie.
		
	explicit HTTPCookie(const std::string& name);
		/// Creates a cookie with the given name.	
		/// The cookie never expires.
		
	explicit HTTPCookie(const NameValueCollection& nvc);
		/// Creates a cookie from the given NameValueCollection.
		
	HTTPCookie(const std::string& name, const std::string& value);
		/// Creates a cookie with the given name and value.
		/// The cookie never expires.
		///
		/// Note: If value contains whitespace or non-alphanumeric
		/// characters, the value should be escaped by calling escape()
		/// before passing it to the constructor.
		
	HTTPCookie(const HTTPCookie& cookie);
		/// Creates the HTTPCookie by copying another one.

	~HTTPCookie();
		/// Destroys the HTTPCookie.
		
	HTTPCookie& operator = (const HTTPCookie& cookie);
		/// Assigns a cookie.
		
	void setVersion(int version);
		/// Sets the version of the cookie.
		///
		/// Version must be either 0 (denoting a Netscape cookie)
		/// or 1 (denoting a RFC 2109 cookie).
		
	int getVersion() const;
		/// Returns the version of the cookie, which is
		/// either 0 or 1.	
		
	void setName(const std::string& name);
		/// Sets the name of the cookie.
		
	const std::string& getName() const;
		/// Returns the name of the cookie.
		
	void setValue(const std::string& value);
		/// Sets the value of the cookie.
		///
		/// According to the cookie specification, the
		/// size of the value should not exceed 4 Kbytes.
		///
		/// Note: If value contains whitespace or non-alphanumeric
		/// characters, the value should be escaped by calling escape()
		/// prior to passing it to setName().
		
	const std::string& getValue() const;
		/// Returns the value of the cookie.
		
	void setComment(const std::string& comment);
		/// Sets the comment for the cookie.
		///
		/// Comments are only supported for version 1 cookies.

	const std::string& getComment() const;
		/// Returns the comment for the cookie.

	void setDomain(const std::string& domain);
		/// Sets the domain for the cookie.
		
	const std::string& getDomain() const;
		/// Returns the domain for the cookie.

	void setPath(const std::string& path);
		/// Sets the path for the cookie.

	void setPriority(const std::string& priority);
		/// Sets the priority for the cookie.
		
	const std::string& getPath() const;
		/// Returns the path for the cookie.

	const std::string& getPriority() const;
		/// Returns the priority for the cookie.

	void setSecure(bool secure);
		/// Sets the value of the secure flag for
		/// the cookie.
		
	bool getSecure() const;
		/// Returns the value of the secure flag
		/// for the cookie.

	void setMaxAge(int maxAge);
		/// Sets the maximum age in seconds for
		/// the cookie.
		///
		/// A value of -1 (default) causes the cookie 
		/// to become a session cookie, which will
		/// be deleted when the browser window
		/// is closed.
		///
		/// A value of 0 deletes the cookie on
		/// the client.

	int getMaxAge() const;
		/// Returns the maximum age in seconds for
		/// the cookie.
		
	void setHttpOnly(bool flag = true);
		/// Sets the HttpOnly flag for the cookie.
		
	bool getHttpOnly() const;
		/// Returns true iff the cookie's HttpOnly flag is set.
		
	std::string toString() const;
		/// Returns a string representation of the cookie,
		/// suitable for use in a Set-Cookie header.
		
	static std::string escape(const std::string& str);
		/// Escapes the given string by replacing all 
		/// non-alphanumeric characters with escape
		/// sequences in the form %xx, where xx is the
		/// hexadecimal character code.
		///
		/// The following characters will be replaced
		/// with escape sequences:
		///   - percent sign %
		///   - less-than and greater-than < and >
		///   - curly brackets { and }
		///   - square brackets [ and ]
		///   - parenthesis ( and )
		///   - solidus /
		///   - vertical line |
		///   - reverse solidus (backslash /)
		///   - quotation mark "
		///   - apostrophe '
		///   - circumflex accent ^
		///   - grave accent `
		///   - comma and semicolon , and ;
		///   - whitespace and control characters
		
	static std::string unescape(const std::string& str);
		/// Unescapes the given string by replacing all
		/// escape sequences in the form %xx with the
		/// respective characters.

private:
	int         _version;
	std::string _name;
	std::string _value;
	std::string _comment;
	std::string _domain;
	std::string _path;
	std::string _priority;
	bool        _secure;
	int         _maxAge;
	bool        _httpOnly;
};


//
// inlines
//
inline int HTTPCookie::getVersion() const
{
	return _version;
}


inline const std::string& HTTPCookie::getName() const
{
	return _name;
}


inline const std::string& HTTPCookie::getValue() const
{
	return _value;
}


inline const std::string& HTTPCookie::getComment() const
{
	return _comment;
}


inline const std::string& HTTPCookie::getDomain() const
{
	return _domain;
}


inline const std::string& HTTPCookie::getPath() const
{
	return _path;
}


inline const std::string& HTTPCookie::getPriority() const
{
	return _priority;
}


inline bool HTTPCookie::getSecure() const
{
	return _secure;
}


inline int HTTPCookie::getMaxAge() const
{
	return _maxAge;
}


inline bool HTTPCookie::getHttpOnly() const
{
	return _httpOnly;
}


} } // namespace Poco::Net


#endif // Net_HTTPCookie_INCLUDED
