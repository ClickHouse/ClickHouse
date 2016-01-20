//
// PurgeStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/PurgeStrategy.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  FileChannel
//
// Definition of the PurgeStrategy class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PurgeStrategy_INCLUDED
#define Foundation_PurgeStrategy_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/File.h"
#include "Poco/Timespan.h"
#include <vector>


namespace Poco {


class Foundation_API PurgeStrategy
	/// The PurgeStrategy is used by FileChannel
	/// to purge archived log files.
{
public:
	PurgeStrategy();
	virtual ~PurgeStrategy();

	virtual void purge(const std::string& path) = 0;
		/// Purges archived log files. The path to the
		/// current "hot" log file is given.
		/// To find archived log files, look for files
		/// with a name consisting of the given path 
		/// plus any suffix (e.g., .1, .20050929081500, .1.gz).
		/// A list of archived files can be obtained by calling
		/// the list() method.

protected:
	void list(const std::string& path, std::vector<File>& files);
		/// Fills the given vector with a list of archived log
		/// files. The path of the current "hot" log file is
		/// given in path.
		///
		/// All files with the same name as the one given in path,
		/// plus some suffix (e.g., .1, .20050929081500, .1.gz) are
		/// considered archived files.

private:
	PurgeStrategy(const PurgeStrategy&);
	PurgeStrategy& operator = (const PurgeStrategy&);
};


class Foundation_API PurgeByAgeStrategy: public PurgeStrategy
	/// This purge strategy purges all files that have
	/// exceeded a given age (given in seconds).
{
public:
	PurgeByAgeStrategy(const Timespan& age);
	~PurgeByAgeStrategy();
	
	void purge(const std::string& path);
	
private:
	Timespan _age;
};


class Foundation_API PurgeByCountStrategy: public PurgeStrategy
	/// This purge strategy ensures that a maximum number
	/// of archived files is not exceeded. Files are deleted
	/// based on their age, with oldest files deleted first.
{
public:
	PurgeByCountStrategy(int count);
	~PurgeByCountStrategy();
	
	void purge(const std::string& path);
	
private:
	int _count;
};


} // namespace Poco


#endif // Foundation_PurgeStrategy_INCLUDED
