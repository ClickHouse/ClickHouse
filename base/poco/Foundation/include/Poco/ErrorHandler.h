//
// ErrorHandler.h
//
// Library: Foundation
// Package: Threading
// Module:  ErrorHandler
//
// Definition of the ErrorHandler class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ErrorHandler_INCLUDED
#define Foundation_ErrorHandler_INCLUDED


#include "Poco/Exception.h"
#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include "Poco/Message.h"


namespace Poco
{


class Foundation_API ErrorHandler
/// This is the base class for thread error handlers.
///
/// An unhandled exception that causes a thread to terminate is usually
/// silently ignored, since the class library cannot do anything meaningful
/// about it.
///
/// The Thread class provides the possibility to register a
/// global ErrorHandler that is invoked whenever a thread has
/// been terminated by an unhandled exception.
/// The ErrorHandler must be derived from this class and can
/// provide implementations of all three exception() overloads.
///
/// The ErrorHandler is always invoked within the context of
/// the offending thread.
{
public:
    ErrorHandler();
    /// Creates the ErrorHandler.

    virtual ~ErrorHandler();
    /// Destroys the ErrorHandler.

    virtual void exception(const Exception & exc);
    /// Called when a Poco::Exception (or a subclass)
    /// caused the thread to terminate.
    ///
    /// This method should not throw any exception - it would
    /// be silently ignored.
    ///
    /// The default implementation just breaks into the debugger.

    virtual void exception(const std::exception & exc);
    /// Called when a std::exception (or a subclass)
    /// caused the thread to terminate.
    ///
    /// This method should not throw any exception - it would
    /// be silently ignored.
    ///
    /// The default implementation just breaks into the debugger.

    virtual void exception();
    /// Called when an exception that is neither a
    /// Poco::Exception nor a std::exception caused
    /// the thread to terminate.
    ///
    /// This method should not throw any exception - it would
    /// be silently ignored.
    ///
    /// The default implementation just breaks into the debugger.

    virtual void logMessageImpl(Message::Priority priority, const std::string & msg) {}
    /// Write a messages to the log
    /// Useful for logging from Poco

    static void handle(const Exception & exc);
    /// Invokes the currently registered ErrorHandler.

    static void handle(const std::exception & exc);
    /// Invokes the currently registered ErrorHandler.

    static void handle();
    /// Invokes the currently registered ErrorHandler.

    static void logMessage(Message::Priority priority, const std::string & msg);
    /// Invokes the currently registered ErrorHandler to log a message.

    static ErrorHandler * set(ErrorHandler * pHandler);
    /// Registers the given handler as the current error handler.
    ///
    /// Returns the previously registered handler.

    static ErrorHandler * get();
    /// Returns a pointer to the currently registered
    /// ErrorHandler.

protected:
    static ErrorHandler * defaultHandler();
    /// Returns the default ErrorHandler.

private:
    static ErrorHandler * _pHandler;
    static FastMutex _mutex;
};


//
// inlines
//
inline ErrorHandler * ErrorHandler::get()
{
    return _pHandler;
}


} // namespace Poco


#endif // Foundation_ErrorHandler_INCLUDED
