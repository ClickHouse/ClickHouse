//
// TemplateCache.h
//
// $Id$
//
// Library: JSON
// Package: JSON
// Module:  TemplateCache
//
// Definition of the TemplateCache class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//

#ifndef JSON_JSONTemplateCache_INCLUDED
#define JSON_JSONTemplateCache_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/JSON/Template.h"
#include "Poco/Path.h"
#include "Poco/SharedPtr.h"
#include "Poco/Logger.h"
#include <vector>
#include <map>


namespace Poco {
namespace JSON {


class JSON_API TemplateCache
	/// Use to cache parsed templates. Templates are
	/// stored in a map with the full path as key.
	/// When a template file has changed, the cache
	/// will remove the old template from the cache
	/// and load a new one.
{
public:
	TemplateCache();
		/// Constructor. The cache must be created
		/// and not destroyed as long as it is used.

	virtual ~TemplateCache();
		/// Destructor.

	void addPath(const Path& path);
		/// Add a path for resolving template paths.
		/// The order of check is FIFO.

	Template::Ptr getTemplate(const Path& path);
		/// Returns a template from the cache.
		/// When the template file is not yet loaded
		/// or when the file has changed, the template
		/// will be (re)loaded and parsed. A shared pointer
		/// is returned, so it is safe to use this template
		/// even when the template isn't stored anymore in
		/// the cache.

	static TemplateCache* instance();
		/// Returns the only instance of this cache

	void setLogger(Logger& logger);
		/// Sets the logger for the cache.

private:
	static TemplateCache*                _instance;
	std::vector<Path>                    _includePaths;
	std::map<std::string, Template::Ptr> _cache;
	Logger*                              _logger;
	
	void setup();
	Path resolvePath(const Path& path) const;
};


inline void TemplateCache::addPath(const Path& path)
{
	_includePaths.push_back(path);
}


inline TemplateCache* TemplateCache::instance()
{
	return _instance;
}


inline void TemplateCache::setLogger(Logger& logger)
{
	_logger = &logger;
}


}} // Namespace Poco::JSON


#endif // JSON_JSONTemplateCache_INCLUDED
