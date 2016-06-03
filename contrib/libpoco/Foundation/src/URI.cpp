//
// URI.cpp
//
// $Id: //poco/1.4/Foundation/src/URI.cpp#5 $
//
// Library: Foundation
// Package: URI
// Module:  URI
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/URI.h"
#include "Poco/NumberFormatter.h"
#include "Poco/Exception.h"
#include "Poco/String.h"
#include "Poco/NumberParser.h"
#include "Poco/Path.h"


namespace Poco {


const std::string URI::RESERVED_PATH     = "?#";
const std::string URI::RESERVED_QUERY    = "?#/";
const std::string URI::RESERVED_FRAGMENT = "";
const std::string URI::ILLEGAL           = "%<>{}|\\\"^`";


URI::URI():
	_port(0)
{
}


URI::URI(const std::string& uri):
	_port(0)
{
	parse(uri);
}


URI::URI(const char* uri):
	_port(0)
{
	parse(std::string(uri));
}

	
URI::URI(const std::string& scheme, const std::string& pathEtc):
	_scheme(scheme),
	_port(0)
{
	toLowerInPlace(_scheme);
	_port = getWellKnownPort();
	std::string::const_iterator beg = pathEtc.begin();
	std::string::const_iterator end = pathEtc.end();
	parsePathEtc(beg, end);
}

	
URI::URI(const std::string& scheme, const std::string& authority, const std::string& pathEtc):
	_scheme(scheme)
{
	toLowerInPlace(_scheme);
	std::string::const_iterator beg = authority.begin();
	std::string::const_iterator end = authority.end();
	parseAuthority(beg, end);
	beg = pathEtc.begin();
	end = pathEtc.end();
	parsePathEtc(beg, end);
}


URI::URI(const std::string& scheme, const std::string& authority, const std::string& path, const std::string& query):
	_scheme(scheme),
	_path(path),
	_query(query)
{
	toLowerInPlace(_scheme);
	std::string::const_iterator beg = authority.begin();
	std::string::const_iterator end = authority.end();
	parseAuthority(beg, end);
}


URI::URI(const std::string& scheme, const std::string& authority, const std::string& path, const std::string& query, const std::string& fragment):
	_scheme(scheme),
	_path(path),
	_query(query),
	_fragment(fragment)
{
	toLowerInPlace(_scheme);
	std::string::const_iterator beg = authority.begin();
	std::string::const_iterator end = authority.end();
	parseAuthority(beg, end);
}


URI::URI(const URI& uri):
	_scheme(uri._scheme),
	_userInfo(uri._userInfo),
	_host(uri._host),
	_port(uri._port),
	_path(uri._path),
	_query(uri._query),
	_fragment(uri._fragment)
{
}

	
URI::URI(const URI& baseURI, const std::string& relativeURI):
	_scheme(baseURI._scheme),
	_userInfo(baseURI._userInfo),
	_host(baseURI._host),
	_port(baseURI._port),
	_path(baseURI._path),
	_query(baseURI._query),
	_fragment(baseURI._fragment)
{
	resolve(relativeURI);
}


URI::URI(const Path& path):
	_scheme("file"),
	_port(0)
{
	Path absolutePath(path);
	absolutePath.makeAbsolute();
	_path = absolutePath.toString(Path::PATH_UNIX);
}


URI::~URI()
{
}


URI& URI::operator = (const URI& uri)
{
	if (&uri != this)
	{
		_scheme   = uri._scheme;
		_userInfo = uri._userInfo;
		_host     = uri._host;
		_port     = uri._port;
		_path     = uri._path;
		_query    = uri._query;
		_fragment = uri._fragment;
	}
	return *this;
}

	
URI& URI::operator = (const std::string& uri)
{
	clear();
	parse(uri);
	return *this;
}


URI& URI::operator = (const char* uri)
{
	clear();
	parse(std::string(uri));
	return *this;
}


void URI::swap(URI& uri)
{
	std::swap(_scheme, uri._scheme);
	std::swap(_userInfo, uri._userInfo);
	std::swap(_host, uri._host);
	std::swap(_port, uri._port);
	std::swap(_path, uri._path);
	std::swap(_query, uri._query);
	std::swap(_fragment, uri._fragment);
}


void URI::clear()
{
	_scheme.clear();
	_userInfo.clear();
	_host.clear();
	_port = 0;
	_path.clear();
	_query.clear();
	_fragment.clear();
}


std::string URI::toString() const
{
	std::string uri;
	if (isRelative())
	{
		encode(_path, RESERVED_PATH, uri);
	}
	else
	{
		uri = _scheme;
		uri += ':';
		std::string auth = getAuthority();
		if (!auth.empty() || _scheme == "file")
		{
			uri.append("//");
			uri.append(auth);
		}
		if (!_path.empty())
		{
			if (!auth.empty() && _path[0] != '/')
				uri += '/';
			encode(_path, RESERVED_PATH, uri);
		}
		else if (!_query.empty() || !_fragment.empty())
		{
			uri += '/';
		}
	}
	if (!_query.empty())
	{
		uri += '?';
		uri.append(_query);
	}
	if (!_fragment.empty())
	{
		uri += '#';
		encode(_fragment, RESERVED_FRAGMENT, uri);
	}
	return uri;
}


void URI::setScheme(const std::string& scheme)
{
	_scheme = scheme;
	toLowerInPlace(_scheme);
	if (_port == 0)
		_port = getWellKnownPort();
}

	
void URI::setUserInfo(const std::string& userInfo)
{
	_userInfo.clear();
	decode(userInfo, _userInfo);
}

	
void URI::setHost(const std::string& host)
{
	_host = host;
}


unsigned short URI::getPort() const
{
	if (_port == 0)
		return getWellKnownPort();
	else
		return _port;
}


void URI::setPort(unsigned short port)
{
	_port = port;
}

	
std::string URI::getAuthority() const
{
	std::string auth;
	if (!_userInfo.empty())
	{
		auth.append(_userInfo);
		auth += '@';
	}
	if (_host.find(':') != std::string::npos)
	{
		auth += '[';
		auth += _host;
		auth += ']';
	}
	else auth.append(_host);
	if (_port && !isWellKnownPort())
	{
		auth += ':';
		NumberFormatter::append(auth, _port);
	}
	return auth;
}

	
void URI::setAuthority(const std::string& authority)
{
	_userInfo.clear();
	_host.clear();
	_port = 0;
	std::string::const_iterator beg = authority.begin();
	std::string::const_iterator end = authority.end();
	parseAuthority(beg, end);
}

	
void URI::setPath(const std::string& path)
{
	_path.clear();
	decode(path, _path);
}

	
void URI::setRawQuery(const std::string& query)
{
	_query = query;
}


void URI::setQuery(const std::string& query)
{
	_query.clear();
	encode(query, RESERVED_QUERY, _query);
}


void URI::addQueryParameter(const std::string& param, const std::string& val)
{
	std::string reserved(RESERVED_QUERY);
	reserved += "=&";
	if (!_query.empty()) _query += '&';
	encode(param, reserved, _query);
	_query += '=';
	encode(val, reserved, _query);
}


std::string URI::getQuery() const
{
	std::string query;
	decode(_query, query);
	return query;
}


URI::QueryParameters URI::getQueryParameters() const
{
	QueryParameters result;
	std::string::const_iterator it(_query.begin());
	std::string::const_iterator end(_query.end());
	while (it != end)
	{
		std::string name;
		std::string value;
		while (it != end && *it != '=' && *it != '&')
		{
			if (*it == '+') 
				name += ' ';
			else
				name += *it;
			++it;
		}
		if (it != end && *it == '=')
		{
			++it;
			while (it != end && *it != '&')
			{
				if (*it == '+') 
					value += ' ';
				else
					value += *it;
				++it;
			}
		}
		std::string decodedName;
		std::string decodedValue;
		URI::decode(name, decodedName);
		URI::decode(value, decodedValue);
		result.push_back(std::make_pair(decodedName, decodedValue));
		if (it != end && *it == '&') ++it;	
	}
	return result;
}


void URI::setQueryParameters(const QueryParameters& params)
{
	_query.clear();
	for (QueryParameters::const_iterator it = params.begin(); it != params.end(); ++it)
	{
		addQueryParameter(it->first, it->second);
	}
}


void URI::setFragment(const std::string& fragment)
{
	_fragment.clear();
	decode(fragment, _fragment);
}


void URI::setPathEtc(const std::string& pathEtc)
{
	_path.clear();
	_query.clear();
	_fragment.clear();
	std::string::const_iterator beg = pathEtc.begin();
	std::string::const_iterator end = pathEtc.end();
	parsePathEtc(beg, end);
}

	
std::string URI::getPathEtc() const
{
	std::string pathEtc;
	encode(_path, RESERVED_PATH, pathEtc);
	if (!_query.empty())
	{
		pathEtc += '?';
		pathEtc += _query;
	}
	if (!_fragment.empty())
	{
		pathEtc += '#';
		encode(_fragment, RESERVED_FRAGMENT, pathEtc);
	}
	return pathEtc;
}


std::string URI::getPathAndQuery() const
{
	std::string pathAndQuery;
	encode(_path, RESERVED_PATH, pathAndQuery);
	if (!_query.empty())
	{
		pathAndQuery += '?';
		pathAndQuery += _query;
	}
	return pathAndQuery;
}

	
void URI::resolve(const std::string& relativeURI)
{
	URI parsedURI(relativeURI);
	resolve(parsedURI);
}


void URI::resolve(const URI& relativeURI)
{
	if (!relativeURI._scheme.empty())
	{
		_scheme   = relativeURI._scheme;
		_userInfo = relativeURI._userInfo;
		_host     = relativeURI._host;
		_port     = relativeURI._port;
		_path     = relativeURI._path;
		_query    = relativeURI._query;
		removeDotSegments();
	}
	else
	{
		if (!relativeURI._host.empty())
		{
			_userInfo = relativeURI._userInfo;
			_host     = relativeURI._host;
			_port     = relativeURI._port;
			_path     = relativeURI._path;
			_query    = relativeURI._query;
			removeDotSegments();
		}
		else
		{
			if (relativeURI._path.empty())
			{
				if (!relativeURI._query.empty())
					_query = relativeURI._query;
			}
			else
			{
				if (relativeURI._path[0] == '/')
				{
					_path = relativeURI._path;
					removeDotSegments();
				}
				else
				{
					mergePath(relativeURI._path);
				}
				_query = relativeURI._query;
			}
		}
	}
	_fragment = relativeURI._fragment;      
}


bool URI::isRelative() const
{
	return _scheme.empty();
}


bool URI::empty() const
{
	return _scheme.empty() && _host.empty() && _path.empty() && _query.empty() && _fragment.empty();
}

	
bool URI::operator == (const URI& uri) const
{
	return equals(uri);
}


bool URI::operator == (const std::string& uri) const
{
	URI parsedURI(uri);
	return equals(parsedURI);
}


bool URI::operator != (const URI& uri) const
{
	return !equals(uri);
}


bool URI::operator != (const std::string& uri) const
{
	URI parsedURI(uri);
	return !equals(parsedURI);
}


bool URI::equals(const URI& uri) const
{
	return _scheme   == uri._scheme
	    && _userInfo == uri._userInfo
	    && _host     == uri._host
	    && getPort() == uri.getPort()
	    && _path     == uri._path
	    && _query    == uri._query
	    && _fragment == uri._fragment;
}

	
void URI::normalize()
{
	removeDotSegments(!isRelative());
}


void URI::removeDotSegments(bool removeLeading)
{
	if (_path.empty()) return;
	
	bool leadingSlash  = *(_path.begin()) == '/';
	bool trailingSlash = *(_path.rbegin()) == '/';
	std::vector<std::string> segments;
	std::vector<std::string> normalizedSegments;
	getPathSegments(segments);
	for (std::vector<std::string>::const_iterator it = segments.begin(); it != segments.end(); ++it)
	{
		if (*it == "..")
		{
			if (!normalizedSegments.empty())
			{
				if (normalizedSegments.back() == "..")
					normalizedSegments.push_back(*it);
				else
					normalizedSegments.pop_back();
			}
			else if (!removeLeading)
			{
				normalizedSegments.push_back(*it);
			}
		}
		else if (*it != ".")
		{
			normalizedSegments.push_back(*it);
		}
	}
	buildPath(normalizedSegments, leadingSlash, trailingSlash);
}


void URI::getPathSegments(std::vector<std::string>& segments)
{
	getPathSegments(_path, segments);
}


void URI::getPathSegments(const std::string& path, std::vector<std::string>& segments)
{
	std::string::const_iterator it  = path.begin();
	std::string::const_iterator end = path.end();
	std::string seg;
	while (it != end)
	{
		if (*it == '/')
		{
			if (!seg.empty())
			{
				segments.push_back(seg);
				seg.clear();
			}
		}
		else seg += *it;
		++it;
	}
	if (!seg.empty())
		segments.push_back(seg);
}


void URI::encode(const std::string& str, const std::string& reserved, std::string& encodedStr)
{
	for (std::string::const_iterator it = str.begin(); it != str.end(); ++it)
	{
		char c = *it;
		if ((c >= 'a' && c <= 'z') || 
		    (c >= 'A' && c <= 'Z') || 
		    (c >= '0' && c <= '9') ||
		    c == '-' || c == '_' || 
		    c == '.' || c == '~')
		{
			encodedStr += c;
		}
		else if (c <= 0x20 || c >= 0x7F || ILLEGAL.find(c) != std::string::npos || reserved.find(c) != std::string::npos)
		{
			encodedStr += '%';
			encodedStr += NumberFormatter::formatHex((unsigned) (unsigned char) c, 2);
		}
		else encodedStr += c;
	}
}

	
void URI::decode(const std::string& str, std::string& decodedStr, bool plusAsSpace)
{
	bool inQuery = false;
	std::string::const_iterator it  = str.begin();
	std::string::const_iterator end = str.end();
	while (it != end)
	{
		char c = *it++;
		if (c == '?') inQuery = true;
		// spaces may be encoded as plus signs in the query
		if (inQuery && plusAsSpace && c == '+') c = ' ';
		else if (c == '%')
		{
			if (it == end) throw SyntaxException("URI encoding: no hex digit following percent sign", str);
			char hi = *it++;
			if (it == end) throw SyntaxException("URI encoding: two hex digits must follow percent sign", str);
			char lo = *it++;
			if (hi >= '0' && hi <= '9')
				c = hi - '0';
			else if (hi >= 'A' && hi <= 'F')
				c = hi - 'A' + 10;
			else if (hi >= 'a' && hi <= 'f')
				c = hi - 'a' + 10;
			else throw SyntaxException("URI encoding: not a hex digit");
			c *= 16;
			if (lo >= '0' && lo <= '9')
				c += lo - '0';
			else if (lo >= 'A' && lo <= 'F')
				c += lo - 'A' + 10;
			else if (lo >= 'a' && lo <= 'f')
				c += lo - 'a' + 10;
			else throw SyntaxException("URI encoding: not a hex digit");
		}
		decodedStr += c;
	}
}


bool URI::isWellKnownPort() const
{
	return _port == getWellKnownPort();
}


unsigned short URI::getWellKnownPort() const
{
	if (_scheme == "ftp")
		return 21;
	else if (_scheme == "ssh")
		return 22;
	else if (_scheme == "telnet")
		return 23;
	else if (_scheme == "http")
		return 80;
	else if (_scheme == "nntp")
		return 119;
	else if (_scheme == "ldap")
		return 389;
	else if (_scheme == "https")
		return 443;
	else if (_scheme == "rtsp")
		return 554;
	else if (_scheme == "sip")
		return 5060;
	else if (_scheme == "sips")
		return 5061;
	else if (_scheme == "xmpp")
		return 5222;
	else
		return 0;
}


void URI::parse(const std::string& uri)
{
	std::string::const_iterator it  = uri.begin();
	std::string::const_iterator end = uri.end();
	if (it == end) return;
	if (*it != '/' && *it != '.' && *it != '?' && *it != '#')
	{
		std::string scheme;
		while (it != end && *it != ':' && *it != '?' && *it != '#' && *it != '/') scheme += *it++;
		if (it != end && *it == ':')
		{
			++it;
			if (it == end) throw SyntaxException("URI scheme must be followed by authority or path", uri);
			setScheme(scheme);
			if (*it == '/')
			{
				++it;
				if (it != end && *it == '/')
				{
					++it;
					parseAuthority(it, end);
				}
				else --it;
			}
			parsePathEtc(it, end);
		}
		else 
		{
			it = uri.begin();
			parsePathEtc(it, end);
		}
	}
	else parsePathEtc(it, end);
}


void URI::parseAuthority(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	std::string userInfo;
	std::string part;
	while (it != end && *it != '/' && *it != '?' && *it != '#')
	{
		if (*it == '@')
		{
			userInfo = part;
			part.clear();
		}
		else part += *it;
		++it;
	}
	std::string::const_iterator pbeg = part.begin();
	std::string::const_iterator pend = part.end();
	parseHostAndPort(pbeg, pend);
	_userInfo = userInfo;
}


void URI::parseHostAndPort(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	if (it == end) return;
	std::string host;
	if (*it == '[')
	{
		// IPv6 address
		++it;
		while (it != end && *it != ']') host += *it++;
		if (it == end) throw SyntaxException("unterminated IPv6 address");
		++it;
	}
	else
	{
		while (it != end && *it != ':') host += *it++;
	}
	if (it != end && *it == ':')
	{
		++it;
		std::string port;
		while (it != end) port += *it++;
		if (!port.empty())
		{
			int nport = 0;
			if (NumberParser::tryParse(port, nport) && nport > 0 && nport < 65536)
				_port = (unsigned short) nport;
			else
				throw SyntaxException("bad or invalid port number", port);
		}
		else _port = getWellKnownPort();
	}
	else _port = getWellKnownPort();
	_host = host;
	toLowerInPlace(_host);
}


void URI::parsePath(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	std::string path;
	while (it != end && *it != '?' && *it != '#') path += *it++;
	decode(path, _path);
}


void URI::parsePathEtc(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	if (it == end) return;
	if (*it != '?' && *it != '#')
		parsePath(it, end);
	if (it != end && *it == '?')
	{
		++it;
		parseQuery(it, end);
	}
	if (it != end && *it == '#')
	{
		++it;
		parseFragment(it, end);
	}	
}


void URI::parseQuery(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	_query.clear();
	while (it != end && *it != '#') _query += *it++;
}


void URI::parseFragment(std::string::const_iterator& it, const std::string::const_iterator& end)
{
	std::string fragment;
	while (it != end) fragment += *it++;
	decode(fragment, _fragment);
}


void URI::mergePath(const std::string& path)
{
	std::vector<std::string> segments;
	std::vector<std::string> normalizedSegments;
	bool addLeadingSlash = false;
	if (!_path.empty())
	{
		getPathSegments(segments);
		bool endsWithSlash = *(_path.rbegin()) == '/';
		if (!endsWithSlash && !segments.empty())
			segments.pop_back();
		addLeadingSlash = _path[0] == '/';
	}
	getPathSegments(path, segments);
	addLeadingSlash = addLeadingSlash || (!path.empty() && path[0] == '/');
	bool hasTrailingSlash = (!path.empty() && *(path.rbegin()) == '/');
	bool addTrailingSlash = false;
	for (std::vector<std::string>::const_iterator it = segments.begin(); it != segments.end(); ++it)
	{
		if (*it == "..")
		{
			addTrailingSlash = true;
			if (!normalizedSegments.empty())
				normalizedSegments.pop_back();
		}
		else if (*it != ".")
		{
			addTrailingSlash = false;
			normalizedSegments.push_back(*it);
		}
		else addTrailingSlash = true;
	}
	buildPath(normalizedSegments, addLeadingSlash, hasTrailingSlash || addTrailingSlash);
}


void URI::buildPath(const std::vector<std::string>& segments, bool leadingSlash, bool trailingSlash)
{
	_path.clear();
	bool first = true;
	for (std::vector<std::string>::const_iterator it = segments.begin(); it != segments.end(); ++it)
	{
		if (first)
		{
			first = false;
			if (leadingSlash)
				_path += '/';
			else if (_scheme.empty() && (*it).find(':') != std::string::npos)
				_path.append("./");
		}
		else _path += '/';
		_path.append(*it);
	}
	if (trailingSlash) 
		_path += '/';
}


} // namespace Poco
