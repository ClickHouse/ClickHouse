//
// DirectoryWatcher.h
//
// $Id: //poco/1.4/Foundation/include/Poco/DirectoryWatcher.h#2 $
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryWatcher
//
// Definition of the DirectoryWatcher class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DirectoryWatcher_INCLUDED
#define Foundation_DirectoryWatcher_INCLUDED


#include "Poco/Foundation.h"


#ifndef POCO_NO_INOTIFY


#include "Poco/File.h"
#include "Poco/BasicEvent.h"
#include "Poco/Runnable.h"
#include "Poco/Thread.h"
#include "Poco/AtomicCounter.h"


namespace Poco {


class DirectoryWatcherStrategy;


class Foundation_API DirectoryWatcher: protected Runnable
	/// This class is used to get notifications about changes
	/// to the filesystem, more specifically, to a specific
	/// directory. Changes to a directory are reported via
	/// events.
	///
	/// A thread will be created that watches the specified
	/// directory for changes. Events are reported in the context
	/// of this thread. 
	///
	/// Note that changes to files in subdirectories of the watched
	/// directory are not reported. Separate DirectoryWatcher objects
	/// must be created for these directories if they should be watched.
	///
	/// Changes to file attributes are not reported.
	///
	/// On Windows, this class is implemented using FindFirstChangeNotification()/FindNextChangeNotification().
	/// On Linux, this class is implemented using inotify.
	/// On FreeBSD and Darwin (Mac OS X, iOS), this class uses kevent/kqueue.
	/// On all other platforms, the watched directory is periodically scanned
	/// for changes. This can negatively affect performance if done too often.
	/// Therefore, the interval in which scans are done can be specified in
	/// the constructor. Note that periodic scanning will also be done on FreeBSD
	/// and Darwin if events for changes to files (DW_ITEM_MODIFIED) are enabled.
	///
	/// DW_ITEM_MOVED_FROM and DW_ITEM_MOVED_TO events will only be reported
	/// on Linux. On other platforms, a file rename or move operation
	/// will be reported via a DW_ITEM_REMOVED and a DW_ITEM_ADDED event.
	/// The order of these two events is not defined.
	///
	/// An event mask can be specified to enable only certain events.
{
public:
	enum DirectoryEventType
	{
		DW_ITEM_ADDED = 1,
			/// A new item has been created and added to the directory.
			
		DW_ITEM_REMOVED = 2,
			/// An item has been removed from the directory.

		DW_ITEM_MODIFIED = 4,
			/// An item has been modified.

		DW_ITEM_MOVED_FROM = 8,
			/// An item has been renamed or moved. This event delivers the old name.

		DW_ITEM_MOVED_TO = 16
			/// An item has been renamed or moved. This event delivers the new name.
	};

	enum DirectoryEventMask
	{
		DW_FILTER_ENABLE_ALL = 31,
			/// Enables all event types.

		DW_FILTER_DISABLE_ALL = 0
			/// Disables all event types.
	};
	
	enum
	{
		DW_DEFAULT_SCAN_INTERVAL = 5 /// Default scan interval for platforms that don't provide a native notification mechanism.
	};
	
	struct DirectoryEvent
	{
		DirectoryEvent(const File& f, DirectoryEventType ev):
			item(f),
			event(ev)
		{
		}

		const File& item;          /// The directory or file that has been changed.
		DirectoryEventType event;  /// The kind of event.
	};
	
	BasicEvent<const DirectoryEvent> itemAdded;
		/// Fired when a file or directory has been created or added to the directory.
		
	BasicEvent<const DirectoryEvent> itemRemoved;
		/// Fired when a file or directory has been removed from the directory.
		
	BasicEvent<const DirectoryEvent> itemModified;
		/// Fired when a file or directory has been modified.

	BasicEvent<const DirectoryEvent> itemMovedFrom;
		/// Fired when a file or directory has been renamed. This event delivers the old name.

	BasicEvent<const DirectoryEvent> itemMovedTo;
		/// Fired when a file or directory has been moved. This event delivers the new name.
		
	BasicEvent<const Exception> scanError;
		/// Fired when an error occurs while scanning for changes.
	
	DirectoryWatcher(const std::string& path, int eventMask = DW_FILTER_ENABLE_ALL, int scanInterval = DW_DEFAULT_SCAN_INTERVAL);
		/// Creates a DirectoryWatcher for the directory given in path.
		/// To enable only specific events, an eventMask can be specified by
		/// OR-ing the desired event IDs (e.g., DW_ITEM_ADDED | DW_ITEM_MODIFIED).
		/// On platforms where no native filesystem notifications are available,
		/// scanInterval specifies the interval in seconds between scans
		/// of the directory.
		
	DirectoryWatcher(const File& directory, int eventMask = DW_FILTER_ENABLE_ALL, int scanInterval = DW_DEFAULT_SCAN_INTERVAL);
		/// Creates a DirectoryWatcher for the specified directory
		/// To enable only specific events, an eventMask can be specified by
		/// OR-ing the desired event IDs (e.g., DW_ITEM_ADDED | DW_ITEM_MODIFIED).
		/// On platforms where no native filesystem notifications are available,
		/// scanInterval specifies the interval in seconds between scans
		/// of the directory.

	~DirectoryWatcher();
		/// Destroys the DirectoryWatcher.
		
	void suspendEvents();
		/// Suspends sending of events. Can be called multiple times, but every
		/// call to suspendEvent() must be matched by a call to resumeEvents().
		
	void resumeEvents();
		/// Resumes events, after they have been suspended with a call to suspendEvents().
		
	bool eventsSuspended() const;
		/// Returns true iff events are suspended.
		
	int eventMask() const;
		/// Returns the value of the eventMask passed to the constructor.
		
	int scanInterval() const;
		/// Returns the scan interval in seconds.
		
	const File& directory() const;
		/// Returns the directory being watched.
		
	bool supportsMoveEvents() const;
		/// Returns true iff the platform supports DW_ITEM_MOVED_FROM/itemMovedFrom and
		/// DW_ITEM_MOVED_TO/itemMovedTo events.
	
protected:
	void init();
	void stop();
	void run();

private:
	DirectoryWatcher();
	DirectoryWatcher(const DirectoryWatcher&);
	DirectoryWatcher& operator = (const DirectoryWatcher&);

	Thread _thread;
	File _directory;
	int _eventMask;
	AtomicCounter _eventsSuspended;
	int _scanInterval;
	DirectoryWatcherStrategy* _pStrategy;
};


//
// inlines
//


inline bool DirectoryWatcher::eventsSuspended() const
{
	return _eventsSuspended.value() > 0;
}


inline int DirectoryWatcher::eventMask() const
{
	return _eventMask;
}


inline int DirectoryWatcher::scanInterval() const
{
	return _scanInterval;
}


inline const File& DirectoryWatcher::directory() const
{
	return _directory;
}


} // namespace Poco


#endif // POCO_NO_INOTIFY


#endif // Foundation_DirectoryWatcher_INCLUDED

