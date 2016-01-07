//
// SimpleFileChannel.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SimpleFileChannel.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  SimpleFileChannel
//
// Definition of the SimpleFileChannel class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SimpleFileChannel_INCLUDED
#define Foundation_SimpleFileChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Timestamp.h"
#include "Poco/Mutex.h"


namespace Poco {


class LogFile;


class Foundation_API SimpleFileChannel: public Channel
	/// A Channel that writes to a file. This class only
	/// supports simple log file rotation.
	///
	/// For more features, see the FileChannel class.
	///
	/// Only the message's text is written, followed
	/// by a newline.
	///
	/// Chain this channel to a FormattingChannel with an
	/// appropriate Formatter to control what is in the text. 
	///
	/// Log file rotation based on log file size is supported.
	///
	/// If rotation is enabled, the SimpleFileChannel will 
	/// alternate between two log files. If the size of
	/// the primary log file exceeds a specified limit,
	/// the secondary log file will be used, and vice
	/// versa. 
	///
	/// Log rotation is configured with the "rotation"
	/// property, which supports the following values:
	///   * never:         no log rotation
	///   * <n>:           the file is rotated when its size exceeds
	///                    <n> bytes.
	///   * <n> K:         the file is rotated when its size exceeds
	///                    <n> Kilobytes.
	///   * <n> M:         the file is rotated when its size exceeds
	///                    <n> Megabytes.
	///
	/// The path of the (primary) log file can be specified with
	/// the "path" property. Optionally, the path of the secondary
	/// log file can be specified with the "secondaryPath" property.
	///
	/// If no secondary path is specified, the secondary path will
	/// default to <primaryPath>.1.
	///
	/// The flush property specifies whether each log message is flushed
	/// immediately to the log file (which may hurt application performance,
	/// but ensures that everything is in the log in case of a system crash),
	//  or whether it's allowed to stay in the system's file buffer for some time.
	/// Valid values are:
	///
	///   * true:   Every essages is immediately flushed to the log file (default).
	///   * false:  Messages are not immediately flushed to the log file.
	///
{
public:
	SimpleFileChannel();
		/// Creates the FileChannel.

	SimpleFileChannel(const std::string& path);
		/// Creates the FileChannel for a file with the given path.

	void open();
		/// Opens the FileChannel and creates the log file if necessary.
		
	void close();
		/// Closes the FileChannel.

	void log(const Message& msg);
		/// Logs the given message to the file.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given name. 
		/// 
		/// The following properties are supported:
		///   * path:          The primary log file's path.
		///   * secondaryPath: The secondary log file's path.
		///   * rotation:      The log file's rotation mode. See the 
		///                    SimpleFileChannel class for details.
		///   * flush:         Specifies whether messages are immediately
		///                    flushed to the log file. See the SimpleFileChannel
		///                    class for details.

	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.
		/// See setProperty() for a description of the supported
		/// properties.

	Timestamp creationDate() const;
		/// Returns the log file's creation date.
		
	UInt64 size() const;
		/// Returns the log file's current size in bytes.

	const std::string& path() const;
		/// Returns the log file's primary path.

	const std::string& secondaryPath() const;
		/// Returns the log file's secondary path.

	static const std::string PROP_PATH;
	static const std::string PROP_SECONDARYPATH;
	static const std::string PROP_ROTATION;
	static const std::string PROP_FLUSH;

protected:
	~SimpleFileChannel();
	void setRotation(const std::string& rotation);
	void setFlush(const std::string& flush);
	void rotate();

private:
	std::string      _path;
	std::string      _secondaryPath;
	std::string      _rotation;
	UInt64           _limit;
	bool             _flush;
	LogFile*         _pFile;
	FastMutex        _mutex;
};


} // namespace Poco


#endif // Foundation_SimpleFileChannel_INCLUDED
