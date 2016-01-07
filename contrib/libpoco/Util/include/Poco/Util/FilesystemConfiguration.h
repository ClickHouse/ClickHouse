//
// FilesystemConfiguration.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/FilesystemConfiguration.h#1 $
//
// Library: Util
// Package: Configuration
// Module:  FilesystemConfiguration
//
// Definition of the FilesystemConfiguration class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_FilesystemConfiguration_INCLUDED
#define Util_FilesystemConfiguration_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/Path.h"


namespace Poco {
namespace Util {


class Util_API FilesystemConfiguration: public AbstractConfiguration
	/// An implementation of AbstractConfiguration that stores configuration data
	/// in a directory hierarchy in the filesystem.
	///
	/// Every period-separated part of a property name is represented
	/// as a directory in the filesystem, relative to the base directory.
	/// Values are stored in files named "data".
	///
	/// All changes to properties are immediately persisted in the filesystem.
	///
	/// For example, a configuration consisting of the properties
	/// 
	///   logging.loggers.root.channel.class = ConsoleChannel
	///   logging.loggers.app.name = Application
	///   logging.loggers.app.channel = c1
	///   logging.formatters.f1.class = PatternFormatter
	///   logging.formatters.f1.pattern = [%p] %t
	///
	/// is stored in the filesystem as follows:
	///
	///   logging/
	///           loggers/
	///                   root/
	///                        channel/
	///                                class/
	///                                      data ("ConsoleChannel")
	///                   app/
	///                       name/
	///                            data ("Application")
	///                       channel/
	///                               data ("c1")
	///           formatters/
	///                      f1/
	///                         class/
	///                               data ("PatternFormatter")
	///                         pattern/
	///                                 data ("[%p] %t")                      
{
public:
	FilesystemConfiguration(const std::string& path);
		/// Creates a FilesystemConfiguration using the given path.
		/// All directories are created as necessary.

	void clear();
		/// Clears the configuration by erasing the configuration
		/// directory and all its subdirectories and files.

protected:
	bool getRaw(const std::string& key, std::string& value) const;
	void setRaw(const std::string& key, const std::string& value);
	void enumerate(const std::string& key, Keys& range) const;
	void removeRaw(const std::string& key);
	Poco::Path keyToPath(const std::string& key) const;
	~FilesystemConfiguration();

private:
	Poco::Path _path;
};


} } // namespace Poco::Util


#endif // Util_FilesystemConfiguration_INCLUDED
