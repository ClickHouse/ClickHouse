#include "Exception.h"

#include <string.h>
#include <cxxabi.h>
#include <Poco/String.h>
#include <common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <common/demangle.h>
#include <Common/config_version.h>
#include <Common/formatReadable.h>
#include <Common/filesystemHelpers.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int POCO_EXCEPTION;
    extern const int STD_EXCEPTION;
    extern const int UNKNOWN_EXCEPTION;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int NOT_IMPLEMENTED;
}

std::string errnoToString(int code, int e)
{
    const size_t buf_size = 128;
    char buf[buf_size];
#ifndef _GNU_SOURCE
    int rc = strerror_r(e, buf, buf_size);
#ifdef __APPLE__
    if (rc != 0 && rc != EINVAL)
#else
    if (rc != 0)
#endif
    {
        std::string tmp = std::to_string(code);
        const char * code_str = tmp.c_str();
        const char * unknown_message = "Unknown error ";
        strcpy(buf, unknown_message);
        strcpy(buf + strlen(unknown_message), code_str);
    }
    return "errno: " + toString(e) + ", strerror: " + std::string(buf);
#else
    (void)code;
    return "errno: " + toString(e) + ", strerror: " + std::string(strerror_r(e, buf, sizeof(buf)));
#endif
}

void throwFromErrno(const std::string & s, int code, int e)
{
    throw ErrnoException(s + ", " + errnoToString(code, e), code, e);
}

void throwFromErrnoWithPath(const std::string & s, const std::string & path, int code, int the_errno)
{
    throw ErrnoException(s + ", " + errnoToString(code, the_errno), code, the_errno, path);
}

void tryLogCurrentException(const char * log_name, const std::string & start_of_message)
{
    tryLogCurrentException(&Logger::get(log_name), start_of_message);
}

void tryLogCurrentException(Poco::Logger * logger, const std::string & start_of_message)
{
    try
    {
        LOG_ERROR(logger, start_of_message << (start_of_message.empty() ? "" : ": ") << getCurrentExceptionMessage(true));
    }
    catch (...)
    {
    }
}

void getNoSpaceLeftInfoMessage(std::filesystem::path path, std::string & msg)
{
    path = std::filesystem::absolute(path);
    /// It's possible to get ENOSPC for non existent file (e.g. if there are no free inodes and creat() fails)
    /// So try to get info for existent parent directory.
    while (!std::filesystem::exists(path) && path.has_relative_path())
        path = path.parent_path();

    auto fs = getStatVFS(path);
    msg += "\nTotal space: "      + formatReadableSizeWithBinarySuffix(fs.f_blocks * fs.f_bsize)
         + "\nAvailable space: "  + formatReadableSizeWithBinarySuffix(fs.f_bavail * fs.f_bsize)
         + "\nTotal inodes: "     + formatReadableQuantity(fs.f_files)
         + "\nAvailable inodes: " + formatReadableQuantity(fs.f_favail);

    auto mount_point = getMountPoint(path).string();
    msg += "\nMount point: " + mount_point;
#if defined(__linux__)
    msg += "\nFilesystem: " + getFilesystemName(mount_point);
#endif
}

std::string getExtraExceptionInfo(const std::exception & e)
{
    String msg;
    try
    {
        if (auto file_exception = dynamic_cast<const Poco::FileException *>(&e))
        {
            if (file_exception->code() == ENOSPC)
                getNoSpaceLeftInfoMessage(file_exception->message(), msg);
        }
        else if (auto errno_exception = dynamic_cast<const DB::ErrnoException *>(&e))
        {
            if (errno_exception->getErrno() == ENOSPC && errno_exception->getPath())
                getNoSpaceLeftInfoMessage(errno_exception->getPath().value(), msg);
        }
    }
    catch (...)
    {
        msg += "\nCannot print extra info: " + getCurrentExceptionMessage(false, false, false);
    }

    return msg;
}

std::string getCurrentExceptionMessage(bool with_stacktrace, bool check_embedded_stacktrace /*= false*/, bool with_extra_info /*= true*/)
{
    std::stringstream stream;

    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        stream << getExceptionMessage(e, with_stacktrace, check_embedded_stacktrace)
               << (with_extra_info ? getExtraExceptionInfo(e) : "")
               << " (version " << VERSION_STRING << VERSION_OFFICIAL << ")";
    }
    catch (const Poco::Exception & e)
    {
        try
        {
            stream << "Poco::Exception. Code: " << ErrorCodes::POCO_EXCEPTION << ", e.code() = " << e.code()
                << ", e.displayText() = " << e.displayText()
                << (with_extra_info ? getExtraExceptionInfo(e) : "")
                << " (version " << VERSION_STRING << VERSION_OFFICIAL;
        }
        catch (...) {}
    }
    catch (const std::exception & e)
    {
        try
        {
            int status = 0;
            auto name = demangle(typeid(e).name(), status);

            if (status)
                name += " (demangling status: " + toString(status) + ")";

            stream << "std::exception. Code: " << ErrorCodes::STD_EXCEPTION << ", type: " << name << ", e.what() = " << e.what()
                   << (with_extra_info ? getExtraExceptionInfo(e) : "")
                   << ", version = " << VERSION_STRING << VERSION_OFFICIAL;
        }
        catch (...) {}
    }
    catch (...)
    {
        try
        {
            int status = 0;
            auto name = demangle(abi::__cxa_current_exception_type()->name(), status);

            if (status)
                name += " (demangling status: " + toString(status) + ")";

            stream << "Unknown exception. Code: " << ErrorCodes::UNKNOWN_EXCEPTION << ", type: " << name << " (version " << VERSION_STRING << VERSION_OFFICIAL << ")";
        }
        catch (...) {}
    }

    return stream.str();
}


int getCurrentExceptionCode()
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        return e.code();
    }
    catch (const Poco::Exception &)
    {
        return ErrorCodes::POCO_EXCEPTION;
    }
    catch (const std::exception &)
    {
        return ErrorCodes::STD_EXCEPTION;
    }
    catch (...)
    {
        return ErrorCodes::UNKNOWN_EXCEPTION;
    }
}


void rethrowFirstException(const Exceptions & exceptions)
{
    for (size_t i = 0, size = exceptions.size(); i < size; ++i)
        if (exceptions[i])
            std::rethrow_exception(exceptions[i]);
}


void tryLogException(std::exception_ptr e, const char * log_name, const std::string & start_of_message)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (...)
    {
        tryLogCurrentException(log_name, start_of_message);
    }
}

void tryLogException(std::exception_ptr e, Poco::Logger * logger, const std::string & start_of_message)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (...)
    {
        tryLogCurrentException(logger, start_of_message);
    }
}

std::string getExceptionMessage(const Exception & e, bool with_stacktrace, bool check_embedded_stacktrace)
{
    std::stringstream stream;

    try
    {
        std::string text = e.displayText();

        bool has_embedded_stack_trace = false;
        if (check_embedded_stacktrace)
        {
            auto embedded_stack_trace_pos = text.find("Stack trace");
            has_embedded_stack_trace = embedded_stack_trace_pos != std::string::npos;
            if (!with_stacktrace && has_embedded_stack_trace)
            {
                text.resize(embedded_stack_trace_pos);
                Poco::trimRightInPlace(text);
            }
        }

        stream << "Code: " << e.code() << ", e.displayText() = " << text;

        if (with_stacktrace && !has_embedded_stack_trace)
            stream << ", Stack trace (when copying this message, always include the lines below):\n\n" << e.getStackTrace().toString();
    }
    catch (...) {}

    return stream.str();
}

std::string getExceptionMessage(std::exception_ptr e, bool with_stacktrace)
{
    try
    {
        std::rethrow_exception(std::move(e));
    }
    catch (...)
    {
        return getCurrentExceptionMessage(with_stacktrace);
    }
}


std::string ExecutionStatus::serializeText() const
{
    WriteBufferFromOwnString wb;
    wb << code << "\n" << escape << message;
    return wb.str();
}

void ExecutionStatus::deserializeText(const std::string & data)
{
    ReadBufferFromString rb(data);
    rb >> code >> "\n" >> escape >> message;
}

bool ExecutionStatus::tryDeserializeText(const std::string & data)
{
    try
    {
        deserializeText(data);
    }
    catch (...)
    {
        return false;
    }

    return true;
}

ExecutionStatus ExecutionStatus::fromCurrentException(const std::string & start_of_message)
{
    String msg = (start_of_message.empty() ? "" : (start_of_message + ": ")) + getCurrentExceptionMessage(false, true);
    return ExecutionStatus(getCurrentExceptionCode(), msg);
}


}
