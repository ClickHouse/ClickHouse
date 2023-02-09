//
// Subsystem.h
//
// Library: Util
// Package: Application
// Module:  Subsystem
//
// Definition of the Subsystem class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_Subsystem_INCLUDED
#define Util_Subsystem_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/RefCountedObject.h"


namespace Poco {
namespace Util {


class Application;
class OptionSet;


class Util_API Subsystem: public Poco::RefCountedObject
	/// Subsystems extend an application in a modular way.
	///
	/// The Subsystem class provides a common interface
	/// for subsystems so that subsystems can be automatically
	/// initialized at startup and uninitialized at shutdown.
	///
	/// Subsystems should also support dynamic reconfiguration,
	/// so that they can be reconfigured anytime during the
	/// life of a running application.
	///
	/// The degree to which dynamic reconfiguration is supported
	/// is up to the actual subsystem implementation. It can
	/// range from ignoring the reconfiguration request (not
	/// recommended), to changing certain settings that affect
	/// the performance, to a complete reinitialization.
{
public:
	Subsystem();
		/// Creates the Subsystem.

	virtual const char* name() const = 0;
		/// Returns the name of the subsystem.
		/// Must be implemented by subclasses.

protected:
	virtual void initialize(Application& app) = 0;
		/// Initializes the subsystem.

	virtual void uninitialize() = 0;
		/// Uninitializes the subsystem.

	virtual void reinitialize(Application& app);
		/// Re-initializes the subsystem.
		///
		/// The default implementation just calls
		/// uninitialize() followed by initialize().
		/// Actual implementations might want to use a
		/// less radical and possibly more performant
		/// approach.

	virtual void defineOptions(OptionSet& options);
		/// Called before the Application's command line processing begins.
		/// If a subsystem wants to support command line arguments,
		/// it must override this method.
		/// The default implementation does not define any options.
		///
		/// To effectively handle options, a subsystem should either bind
		/// the option to a configuration property or specify a callback
		/// to handle the option.

	virtual ~Subsystem();
		/// Destroys the Subsystem.

	friend class Application;

private:
	Subsystem(const Subsystem&);
	Subsystem& operator = (const Subsystem&);
};


} } // namespace Poco::Util


#endif // Util_Subsystem_INCLUDED
