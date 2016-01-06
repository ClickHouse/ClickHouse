//
// Path.cpp
//
// $Id: //poco/1.4/Foundation/src/Path.cpp#5 $
//
// Library: Foundation
// Package: Filesystem
// Module:  Path
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Path.h"
#include "Poco/File.h"
#include "Poco/Exception.h"
#include "Poco/StringTokenizer.h"
#if defined(_WIN32) && defined(POCO_WIN32_UTF8)
#include "Poco/UnicodeConverter.h"
#include "Poco/Buffer.h"
#endif
#include <algorithm>


#if defined(POCO_OS_FAMILY_VMS)
#include "Path_VMS.cpp"
#elif defined(POCO_OS_FAMILY_UNIX)
#include "Path_UNIX.cpp"
#elif defined(POCO_OS_FAMILY_WINDOWS) && defined(POCO_WIN32_UTF8)
#if defined(_WIN32_WCE)
#include "Path_WINCE.cpp"
#else
#include "Path_WIN32U.cpp"
#endif
#elif defined(POCO_OS_FAMILY_WINDOWS)
#include "Path_WIN32.cpp"
#endif


namespace Poco {


Path::Path(): _absolute(false)
{
}


Path::Path(bool absolute): _absolute(absolute)
{
}


Path::Path(const std::string& path)
{
	assign(path);
}


Path::Path(const std::string& path, Style style)
{
	assign(path, style);
}


Path::Path(const char* path)
{
	poco_check_ptr(path);
	assign(path);
}


Path::Path(const char* path, Style style)
{
	poco_check_ptr(path);
	assign(path, style);
}


Path::Path(const Path& path): 
	_node(path._node), 
	_device(path._device),
	_name(path._name),
	_version(path._version),
	_dirs(path._dirs),
	_absolute(path._absolute)
{	
}


Path::Path(const Path& parent, const std::string& fileName):
	_node(parent._node), 
	_device(parent._device),
	_name(parent._name),
	_version(parent._version),
	_dirs(parent._dirs),
	_absolute(parent._absolute)
{	
	makeDirectory();
	_name = fileName;
}


Path::Path(const Path& parent, const char* fileName):
	_node(parent._node), 
	_device(parent._device),
	_name(parent._name),
	_version(parent._version),
	_dirs(parent._dirs),
	_absolute(parent._absolute)
{	
	makeDirectory();
	_name = fileName;
}


Path::Path(const Path& parent, const Path& relative):
	_node(parent._node), 
	_device(parent._device),
	_name(parent._name),
	_version(parent._version),
	_dirs(parent._dirs),
	_absolute(parent._absolute)
{	
	resolve(relative);
}


Path::~Path()
{
}

	
Path& Path::operator = (const Path& path)
{
	return assign(path);
}

	
Path& Path::operator = (const std::string& path)
{
	return assign(path);
}


Path& Path::operator = (const char* path)
{
	poco_check_ptr(path);
	return assign(path);
}


void Path::swap(Path& path)
{
	std::swap(_node, path._node);
	std::swap(_device, path._device);
	std::swap(_name, path._name);
	std::swap(_version, path._version);
	std::swap(_dirs, path._dirs);
	std::swap(_absolute, path._absolute);
}


Path& Path::assign(const Path& path)
{
	if (&path != this)
	{
		_node     = path._node;
		_device   = path._device;
		_name     = path._name;
		_version  = path._version;
		_dirs     = path._dirs;
		_absolute = path._absolute;
	}
	return *this;
}


Path& Path::assign(const std::string& path)
{
#if defined(POCO_OS_FAMILY_VMS)
	parseVMS(path);
#elif defined(POCO_OS_FAMILY_WINDOWS)
	parseWindows(path);
#else
	parseUnix(path);
#endif
	return *this;
}

	
Path& Path::assign(const std::string& path, Style style)
{
	switch (style)
	{
	case PATH_UNIX:
		parseUnix(path);
		break;
	case PATH_WINDOWS:
		parseWindows(path);
		break;
	case PATH_VMS:
		parseVMS(path);
		break;
	case PATH_NATIVE:
		assign(path);
		break;
	case PATH_GUESS:
		parseGuess(path);
		break;
	default:
		poco_bugcheck();
	}
	return *this;
}


Path& Path::assign(const char* path)
{
	return assign(std::string(path));
}


std::string Path::toString() const
{
#if defined(POCO_OS_FAMILY_UNIX)
	return buildUnix();
#elif defined(POCO_OS_FAMILY_WINDOWS)
	return buildWindows();
#else
	return buildVMS();
#endif
}

	
std::string Path::toString(Style style) const
{
	switch (style)
	{
	case PATH_UNIX:
		return buildUnix();
	case PATH_WINDOWS:
		return buildWindows();
	case PATH_VMS:
		return buildVMS();
	case PATH_NATIVE:
	case PATH_GUESS:
		return toString();
	default:
		poco_bugcheck();
	}
	return std::string();
}


bool Path::tryParse(const std::string& path)
{
	try
	{
		Path p;
		p.parse(path);
		assign(p);
		return true;
	}
	catch (...)
	{
		return false;
	}
}


bool Path::tryParse(const std::string& path, Style style)
{
	try
	{
		Path p;
		p.parse(path, style);
		assign(p);
		return true;
	}
	catch (...)
	{
		return false;
	}
}


Path& Path::parseDirectory(const std::string& path)
{
	assign(path);
	return makeDirectory();
}


Path& Path::parseDirectory(const std::string& path, Style style)
{
	assign(path, style);
	return makeDirectory();
}


Path& Path::makeDirectory()
{
#if defined(POCO_OS_FAMILY_VMS)
	pushDirectory(getBaseName());
#else
	pushDirectory(_name);
#endif
	_name.clear();
	_version.clear();
	return *this;
}


Path& Path::makeFile()
{
	if (!_dirs.empty() && _name.empty())
	{
		_name = _dirs.back();
		_dirs.pop_back();
#if defined(POCO_OS_FAMILY_VMS)
		setExtension("DIR");
#endif
	}
	return *this;
}


Path& Path::makeAbsolute()
{
	return makeAbsolute(current());
}


Path& Path::makeAbsolute(const Path& base)
{
	if (!_absolute)
	{
		Path tmp = base;
		tmp.makeDirectory();
		for (StringVec::const_iterator it = _dirs.begin(); it != _dirs.end(); ++it)
		{
			tmp.pushDirectory(*it);
		}
		_node     = tmp._node;
		_device   = tmp._device;
		_dirs     = tmp._dirs;
		_absolute = base._absolute;
	}
	return *this;
}


Path Path::absolute() const
{
	Path result(*this);
	if (!result._absolute)
	{
		result.makeAbsolute();
	}
	return result;
}


Path Path::absolute(const Path& base) const
{
	Path result(*this);
	if (!result._absolute)
	{
		result.makeAbsolute(base);
	}
	return result;
}


Path Path::parent() const
{
	Path p(*this);
	return p.makeParent();
}


Path& Path::makeParent()
{
	if (_name.empty())
	{
		if (_dirs.empty())
		{
			if (!_absolute)
				_dirs.push_back("..");
		}
		else
		{
			if (_dirs.back() == "..")
				_dirs.push_back("..");
			else
				_dirs.pop_back();
		}
	}
	else
	{
		_name.clear();
		_version.clear();
	}
	return *this;
}


Path& Path::append(const Path& path)
{
	makeDirectory();
	_dirs.insert(_dirs.end(), path._dirs.begin(), path._dirs.end());
	_name = path._name;
	_version = path._version;
	return *this;
}


Path& Path::resolve(const Path& path)
{
	if (path.isAbsolute())
	{
		assign(path);
	}
	else
	{
		for (int i = 0; i < path.depth(); ++i)
			pushDirectory(path[i]);
		_name = path._name;
	}
	return *this;
}


Path& Path::setNode(const std::string& node)
{
	_node     = node;
	_absolute = _absolute || !node.empty();
	return *this;
}

	
Path& Path::setDevice(const std::string& device)
{
	_device   = device;
	_absolute = _absolute || !device.empty();
	return *this;
}

	
const std::string& Path::directory(int n) const
{
	poco_assert (0 <= n && n <= _dirs.size());
	
	if (n < _dirs.size())
		return _dirs[n];
	else
		return _name;	
}


const std::string& Path::operator [] (int n) const
{
	poco_assert (0 <= n && n <= _dirs.size());
	
	if (n < _dirs.size())
		return _dirs[n];
	else
		return _name;	
}

	
Path& Path::pushDirectory(const std::string& dir)
{
	if (!dir.empty() && dir != ".")
	{
#if defined(POCO_OS_FAMILY_VMS)
		if (dir == ".." || dir == "-")
		{
			if (!_dirs.empty() && _dirs.back() != ".." && _dirs.back() != "-")
				_dirs.pop_back();
			else if (!_absolute)
				_dirs.push_back(dir);
		}
		else _dirs.push_back(dir);
#else
		if (dir == "..")
		{
			if (!_dirs.empty() && _dirs.back() != "..")
				_dirs.pop_back();
			else if (!_absolute)
				_dirs.push_back(dir);
		}
		else _dirs.push_back(dir);
#endif
	}
	return *this;
}

	
Path& Path::popDirectory()
{
	poco_assert (!_dirs.empty());
	
	_dirs.pop_back();
	return *this;
}


Path& Path::popFrontDirectory()
{
	poco_assert (!_dirs.empty());
	
	StringVec::iterator it = _dirs.begin();
	_dirs.erase(it);
	return *this;
}

	
Path& Path::setFileName(const std::string& name)
{
	_name = name;
	return *this;
}


Path& Path::setBaseName(const std::string& name)
{
	std::string ext = getExtension();
	_name = name;
	if (!ext.empty())
	{
		_name.append(".");
		_name.append(ext);
	}
	return *this;
}


std::string Path::getBaseName() const
{
	std::string::size_type pos = _name.rfind('.');
	if (pos != std::string::npos)
		return _name.substr(0, pos);
	else
		return _name;
}


Path& Path::setExtension(const std::string& extension)
{
	_name = getBaseName();
	if (!extension.empty())
	{
		_name.append(".");
		_name.append(extension);
	}
	return *this;
}

			
std::string Path::getExtension() const
{
	std::string::size_type pos = _name.rfind('.');
	if (pos != std::string::npos)
		return _name.substr(pos + 1);
	else
		return std::string();
}


Path& Path::clear()
{
	_node.clear();
	_device.clear();
	_name.clear();
	_dirs.clear();
	_version.clear();
	_absolute = false;
	return *this;
}


std::string Path::current()
{
	return PathImpl::currentImpl();
}

	
std::string Path::home()
{
	return PathImpl::homeImpl();
}

	
std::string Path::temp()
{
	return PathImpl::tempImpl();
}


std::string Path::null()
{
	return PathImpl::nullImpl();
}

	
std::string Path::expand(const std::string& path)
{
	return PathImpl::expandImpl(path);
}


void Path::listRoots(std::vector<std::string>& roots)
{
	PathImpl::listRootsImpl(roots);
}


bool Path::find(StringVec::const_iterator it, StringVec::const_iterator end, const std::string& name, Path& path)
{
	while (it != end)
	{
#if defined(WIN32)
		std::string cleanPath(*it);
		if (cleanPath.size() > 1 && cleanPath[0] == '"' && cleanPath[cleanPath.size() - 1] == '"')
		{
			cleanPath = cleanPath.substr(1, cleanPath.size() - 2);
		}
		Path p(cleanPath);
#else
		Path p(*it);
#endif
		p.makeDirectory();
		p.resolve(Path(name));
		File f(p);
		if (f.exists())
		{
			path = p;
			return true;
		}
		++it;
	}
	return false;
}


bool Path::find(const std::string& pathList, const std::string& name, Path& path)
{
	StringTokenizer st(pathList, std::string(1, pathSeparator()), StringTokenizer::TOK_IGNORE_EMPTY + StringTokenizer::TOK_TRIM);
	return find(st.begin(), st.end(), name, path);
}


void Path::parseUnix(const std::string& path)
{
	clear();

	std::string::const_iterator it  = path.begin();
	std::string::const_iterator end = path.end();

	if (it != end)
	{
		if (*it == '/') 
		{
			_absolute = true; ++it;
		}
		else if (*it == '~')
		{
			++it;
			if (it == end || *it == '/')
			{
				Path cwd(home());
				_dirs = cwd._dirs;
				_absolute = true;
			}
			else --it;
		}

		while (it != end)
		{
			std::string name;
			while (it != end && *it != '/') name += *it++;
			if (it != end)
			{
				if (_dirs.empty())
				{
					if (!name.empty() && *(name.rbegin()) == ':')
					{
						_absolute = true;
						_device.assign(name, 0, name.length() - 1);
					}
					else
					{
						pushDirectory(name);
					}
				}
				else pushDirectory(name);
			}
			else _name = name;
			if (it != end) ++it;
		}
	}
}


void Path::parseWindows(const std::string& path)
{
	clear();

	std::string::const_iterator it  = path.begin();
	std::string::const_iterator end = path.end();

	if (it != end)
	{
		if (*it == '\\' || *it == '/') { _absolute = true; ++it; }
		if (_absolute && it != end && (*it == '\\' || *it == '/')) // UNC
		{
			++it;
			while (it != end && *it != '\\' && *it != '/') _node += *it++;
			if (it != end) ++it;
		}
		else if (it != end)
		{
			char d = *it++;
			if (it != end && *it == ':') // drive letter
			{
				if (_absolute || !((d >= 'a' && d <= 'z') || (d >= 'A' && d <= 'Z'))) throw PathSyntaxException(path);
				_absolute = true;
				_device += d;
				++it;
				if (it == end || (*it != '\\' && *it != '/')) throw PathSyntaxException(path);
				++it;
			}
			else --it;
		}
		while (it != end)
		{
			std::string name;
			while (it != end && *it != '\\' && *it != '/') name += *it++;
			if (it != end)
				pushDirectory(name);
			else
				_name = name;
			if (it != end) ++it;
		}
	}
	if (!_node.empty() && _dirs.empty() && !_name.empty())
		makeDirectory();
}


void Path::parseVMS(const std::string& path)
{
	clear();

	std::string::const_iterator it  = path.begin();
	std::string::const_iterator end = path.end();

	if (it != end)
	{
		std::string name;
		while (it != end && *it != ':' && *it != '[' && *it != ';') name += *it++;
		if (it != end)
		{
			if (*it == ':')
			{
				++it;
				if (it != end && *it == ':')
				{
					_node = name;
					++it;
				}
				else _device = name;
				_absolute = true;
				name.clear();
			}
			if (it != end)
			{
				if (_device.empty() && *it != '[')
				{
					while (it != end && *it != ':' && *it != ';') name += *it++;
					if (it != end)
					{
						if (*it == ':')
						{
							_device = name;
							_absolute = true;
							name.clear();
							++it;
						}
					}
				}
			}			
			if (name.empty())
			{
				if (it != end && *it == '[')
				{
					++it;
					if (it != end)
					{
						_absolute = true;
						if (*it == '.')
							{ _absolute = false; ++it; }
						else if (*it == ']' || *it == '-')
							_absolute = false;
						while (it != end && *it != ']')
						{
							name.clear();
							if (*it == '-')
								name = "-";
							else
								while (it != end && *it != '.' && *it != ']') name += *it++;
							if (!name.empty())
							{
								if (name == "-")
								{
									if (_dirs.empty() || _dirs.back() == "..")
										_dirs.push_back("..");
									else 
										_dirs.pop_back();
								}
								else _dirs.push_back(name);
							}
							if (it != end && *it != ']') ++it;
						}
						if (it == end) throw PathSyntaxException(path);
						++it;
						if (it != end && *it == '[')
						{
							if (!_absolute) throw PathSyntaxException(path);
							++it;
							if (it != end && *it == '.') throw PathSyntaxException(path);
							int d = int(_dirs.size());
							while (it != end && *it != ']')
							{
								name.clear();
								if (*it == '-')
									name = "-";
								else
									while (it != end && *it != '.' && *it != ']') name += *it++;
								if (!name.empty())
								{
									if (name == "-")
									{
										if (_dirs.size() > d)
											_dirs.pop_back();
									}
									else _dirs.push_back(name);
								}
								if (it != end && *it != ']') ++it;
							}
							if (it == end) throw PathSyntaxException(path);
							++it;
						}
					}
					_name.clear();
				}
				while (it != end && *it != ';') _name += *it++;
			}
			else _name = name;
			if (it != end && *it == ';')
			{
				++it;
				while (it != end) _version += *it++;
			}
		}
		else _name = name;
	}
}


void Path::parseGuess(const std::string& path)
{
	bool hasBackslash   = false;
	bool hasSlash       = false;
	bool hasOpenBracket = false;
	bool hasClosBracket = false;
	bool isWindows      = path.length() > 2 && path[1] == ':' && (path[2] == '/' || path[2] == '\\');
	std::string::const_iterator end    = path.end();
	std::string::const_iterator semiIt = end;
	if (!isWindows)
	{
		for (std::string::const_iterator it = path.begin(); it != end; ++it)
		{
			switch (*it)
			{
			case '\\': hasBackslash = true; break;
			case '/':  hasSlash = true; break;
			case '[':  hasOpenBracket = true;
			case ']':  hasClosBracket = hasOpenBracket; 
			case ';':  semiIt = it; break;
			}
		}
	}
	if (hasBackslash || isWindows)
	{
		parseWindows(path);
	}
	else if (hasSlash)
	{
		parseUnix(path);
	}
	else
	{
		bool isVMS = hasClosBracket;
		if (!isVMS && semiIt != end)
		{
			isVMS = true;
			++semiIt;
			while (semiIt != end)
			{
				if (*semiIt < '0' || *semiIt > '9')
				{
					isVMS = false; break;
				}
				++semiIt;
			}
		}
		if (isVMS)
			parseVMS(path);
		else
			parseUnix(path);
	}
}


std::string Path::buildUnix() const
{
	std::string result;
	if (!_device.empty())
	{
		result.append("/");
		result.append(_device);
		result.append(":/");
	}
	else if (_absolute)
	{
		result.append("/");
	}
	for (StringVec::const_iterator it = _dirs.begin(); it != _dirs.end(); ++it)
	{
		result.append(*it);
		result.append("/");
	}
	result.append(_name);
	return result;
}


std::string Path::buildWindows() const
{
	std::string result;
	if (!_node.empty())
	{
		result.append("\\\\");
		result.append(_node);
		result.append("\\");
	}
	else if (!_device.empty())
	{
		result.append(_device);
		result.append(":\\");
	}
	else if (_absolute)
	{
		result.append("\\");
	}
	for (StringVec::const_iterator it = _dirs.begin(); it != _dirs.end(); ++it)
	{
		result.append(*it);
		result.append("\\");
	}
	result.append(_name);
	return result;
}


std::string Path::buildVMS() const
{
	std::string result;
	if (!_node.empty())
	{
		result.append(_node);
		result.append("::");
	}
	if (!_device.empty())
	{
		result.append(_device);
		result.append(":");
	}
	if (!_dirs.empty())
	{
		result.append("[");
		if (!_absolute && _dirs[0] != "..")
			result.append(".");
		for (StringVec::const_iterator it = _dirs.begin(); it != _dirs.end(); ++it)
		{
			if (it != _dirs.begin() && *it != "..")
				result.append(".");
			if (*it == "..")
				result.append("-");
			else
				result.append(*it);
		}
		result.append("]");
	}
	result.append(_name);
	if (!_version.empty())
	{
		result.append(";");
		result.append(_version);
	}
	return result;
}


std::string Path::transcode(const std::string& path)
{
#if defined(_WIN32) && defined(POCO_WIN32_UTF8)
	std::wstring uniPath;
	UnicodeConverter::toUTF16(path, uniPath);
	DWORD len = WideCharToMultiByte(CP_ACP, WC_NO_BEST_FIT_CHARS, uniPath.c_str(), static_cast<int>(uniPath.length()), NULL, 0, NULL, NULL);
	if (len > 0)
	{
		Buffer<char> buffer(len);
		DWORD rc = WideCharToMultiByte(CP_ACP, WC_NO_BEST_FIT_CHARS, uniPath.c_str(), static_cast<int>(uniPath.length()), buffer.begin(), static_cast<int>(buffer.size()), NULL, NULL);
		if (rc)
		{
			return std::string(buffer.begin(), buffer.size());
		}
	}
#endif
	return path;
}


} // namespace Poco
