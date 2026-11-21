//
// ArchiveStrategy.h
//
// Library: Foundation
// Package: Logging
// Module:  FileChannel
//
// Definition of the ArchiveStrategy class and subclasses.
//
// Copyright (c) 2004-2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ArchiveStrategy_INCLUDED
#define Foundation_ArchiveStrategy_INCLUDED


#include "Poco/CompressedLogFile.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/File.h"
#include "Poco/Foundation.h"
#include "Poco/LogFile.h"
#include "Poco/NumberFormatter.h"

#include <functional>
#include <memory>


namespace Poco
{


class ArchiveCompressor;


class Foundation_API ArchiveStrategy
/// The ArchiveStrategy is used by FileChannel
/// to rename a rotated log file for archiving.
///
/// Archived files can be automatically compressed,
/// using the gzip file format.
{
public:
    ArchiveStrategy();
    virtual ~ArchiveStrategy();

    virtual void archive(std::unique_ptr<LogFile> & file, bool streamCompress = false) = 0;
    /// Renames `file` for archiving and replaces it with a new log file.
    /// If this method throws, `file` is still the current file under its original name
    /// (or null, if the file could not be renamed back).
    /// Creates CompressedLogFile if streamCompress flag is set.

    void compress(bool flag = true);
    /// Enables or disables compression of archived files.

protected:
    void moveFile(const std::string & oldName, const std::string & newName);
    bool exists(const std::string & name);
    static std::unique_ptr<LogFile> openFile(const std::string & base, bool streamCompress);
    void rotateFile(
        std::unique_ptr<LogFile> & file,
        const std::string & base,
        const std::string & ext,
        const std::string & archived,
        bool streamCompress,
        const std::function<void()> & makeRoom);
    /// Opens the new file under a temporary name, calls `makeRoom`, renames the current file to `archived`
    /// and the new file to the current name, then replaces `file`. Compresses `archived` after the old file is closed.

private:
    ArchiveStrategy(const ArchiveStrategy &);
    ArchiveStrategy & operator=(const ArchiveStrategy &);

    bool _compress;
    ArchiveCompressor * _pCompressor;
};


class Foundation_API ArchiveByNumberStrategy : public ArchiveStrategy
/// A monotonic increasing number is appended to the
/// log file name. The most recent archived file
/// always has the number zero.
{
public:
    ArchiveByNumberStrategy();
    ~ArchiveByNumberStrategy();
    void archive(std::unique_ptr<LogFile> & file, bool streamCompress = false);
};


template <class DT>
class ArchiveByTimestampStrategy : public ArchiveStrategy
/// A timestamp (YYYYMMDDhhmmssiii) is appended to archived
/// log files.
{
public:
    ArchiveByTimestampStrategy() { }

    ~ArchiveByTimestampStrategy() { }

    void archive(std::unique_ptr<LogFile> & file, bool streamCompress = false)
    /// Archives the file by appending the current timestamp to the
    /// file name. If the new file name exists, additionally a monotonic
    /// increasing number is appended to the log file name.
    {
        std::string base = file->path();
        std::string ext = "";

        if (base.ends_with(".lz4"))
        {
            base.resize(base.size() - 4);
            ext = ".lz4";
        }

        std::string archPath = base;
        archPath.append(".");
        DateTimeFormatter::append(archPath, DT().timestamp(), "%Y%m%d%H%M%S%i");
        archPath.append(ext);

        rotateFile(file, base, ext, archPath, streamCompress, [&]
        {
            if (exists(archPath))
                archiveByNumber(archPath);
        });
    }

private:
    void archiveByNumber(const std::string & basePath)
    /// A monotonic increasing number is appended to the
    /// log file name. The most recent archived file
    /// always has the number zero.
    {
        std::string base = basePath;
        std::string ext = "";

        if (base.ends_with(".lz4"))
        {
            base.resize(base.size() - 4);
            ext = ".lz4";
        }

        int n = -1;
        std::string path;
        do
        {
            path = base;
            path.append(".");
            NumberFormatter::append(path, ++n);
            path.append(ext);
        } while (exists(path));

        while (n >= 0)
        {
            std::string oldPath = base;
            if (n > 0)
            {
                oldPath.append(".");
                NumberFormatter::append(oldPath, n - 1);
            }
            oldPath.append(ext);

            std::string newPath = base;
            newPath.append(".");
            NumberFormatter::append(newPath, n);
            newPath.append(ext);
            moveFile(oldPath, newPath);
            --n;
        }
    }
};


} // namespace Poco


#endif // Foundation_ArchiveStrategy_INCLUDED
