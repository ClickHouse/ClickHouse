//
// LogFile.h
//
// Library: Foundation
// Package: Logging
// Module:  LogFile
//
// Definition of the LogFile class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LogFile_INCLUDED
#define Foundation_LogFile_INCLUDED


#include "Poco/Foundation.h"


#    include "Poco/LogFile_STD.h"


namespace Poco
{


class Foundation_API LogFile : public LogFileImpl
/// This class is used by FileChannel to work
/// with a log file.
{
public:
    LogFile(const std::string & path);
    /// Creates the LogFile.

    virtual ~LogFile();
    /// Destroys the LogFile.

    virtual void write(const std::string & text, bool flush = true);
    /// Writes the given text to the log file.
    /// If flush is true, the text will be immediately
    /// flushed to the file.

    void writeBinary(const char * data, size_t size, bool flush = true);
    /// Writes the given bytes to the log file.
    /// If flush is true, the text will be immediately
    /// flushed to the file.

    UInt64 size() const;
    /// Returns the current size in bytes of the log file.

    Timestamp creationDate() const;
    /// Returns the date and time the log file was created.

    const std::string & path() const;
    /// Returns the path given in the constructor.
};


//
// inlines
//
inline void LogFile::write(const std::string & text, bool flush)
{
    writeImpl(text, flush);
}


inline void LogFile::writeBinary(const char * data, size_t data_size, bool flush)
{
    writeBinaryImpl(data, data_size, flush);
}


inline UInt64 LogFile::size() const
{
    return sizeImpl();
}


inline Timestamp LogFile::creationDate() const
{
    return creationDateImpl();
}


inline const std::string & LogFile::path() const
{
    return pathImpl();
}


} // namespace Poco


#endif // Foundation_LogFile_INCLUDED
