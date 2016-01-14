//
// DirectoryWatcher.cpp
//
// $Id: //poco/1.4/Foundation/src/DirectoryWatcher.cpp#4 $
//
// Library: Foundation
// Package: Filesystem
// Module:  DirectoryWatcher
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/DirectoryWatcher.h"


#ifndef POCO_NO_INOTIFY


#include "Poco/Path.h"
#include "Poco/Glob.h"
#include "Poco/DirectoryIterator.h"
#include "Poco/Event.h"
#include "Poco/Exception.h"
#include "Poco/Buffer.h"
#if defined(POCO_WIN32_UTF8)
	#include "Poco/UnicodeConverter.h"
#endif
#if POCO_OS == POCO_OS_LINUX
	#include <sys/inotify.h>
	#include <sys/select.h>
	#include <unistd.h>
#elif POCO_OS == POCO_OS_MAC_OS_X || POCO_OS == POCO_OS_FREE_BSD
	#include <fcntl.h>
	#include <sys/types.h>
	#include <sys/event.h>
	#include <sys/time.h>
	#include <unistd.h>
	#if (POCO_OS == POCO_OS_FREE_BSD) && !defined(O_EVTONLY)
		#define O_EVTONLY 0x8000
	#endif
#endif
#include <algorithm>
#include <map>


namespace Poco {


class DirectoryWatcherStrategy
{
public:
	DirectoryWatcherStrategy(DirectoryWatcher& owner):
		_owner(owner)
	{
	}

	virtual ~DirectoryWatcherStrategy()
	{
	}

	DirectoryWatcher& owner()
	{
		return _owner;
	}
	
	virtual void run() = 0;
	virtual void stop() = 0;
	virtual bool supportsMoveEvents() const = 0;

protected:
	struct ItemInfo
	{
		ItemInfo():
			size(0)
		{
		}
		
		ItemInfo(const ItemInfo& other):
			path(other.path),
			size(other.size),
			lastModified(other.lastModified)
		{
		}
		
		explicit ItemInfo(const File& f):
			path(f.path()),
			size(f.isFile() ? f.getSize() : 0),
			lastModified(f.getLastModified())
		{
		}
		
		std::string path;
		File::FileSize size;
		Timestamp lastModified;
	};
	typedef std::map<std::string, ItemInfo> ItemInfoMap;

	void scan(ItemInfoMap& entries)
	{
		DirectoryIterator it(owner().directory());
		DirectoryIterator end;
		while (it != end)
		{
			entries[it.path().getFileName()] = ItemInfo(*it);
			++it;
		}
	}
	
	void compare(ItemInfoMap& oldEntries, ItemInfoMap& newEntries)
	{
		for (ItemInfoMap::iterator itn = newEntries.begin(); itn != newEntries.end(); ++itn)
		{
			ItemInfoMap::iterator ito = oldEntries.find(itn->first);
			if (ito != oldEntries.end())
			{
				if ((owner().eventMask() & DirectoryWatcher::DW_ITEM_MODIFIED) && !owner().eventsSuspended())
				{
					if (itn->second.size != ito->second.size || itn->second.lastModified != ito->second.lastModified)
					{
						Poco::File f(itn->second.path);
						DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_MODIFIED);
						owner().itemModified(&owner(), ev);
					}
				}
				oldEntries.erase(ito);
			}
			else if ((owner().eventMask() & DirectoryWatcher::DW_ITEM_ADDED) && !owner().eventsSuspended())
			{
				Poco::File f(itn->second.path);
				DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_ADDED);
				owner().itemAdded(&owner(), ev);
			}
		}
		if ((owner().eventMask() & DirectoryWatcher::DW_ITEM_REMOVED) && !owner().eventsSuspended())
		{
			for (ItemInfoMap::iterator it = oldEntries.begin(); it != oldEntries.end(); ++it)
			{
				Poco::File f(it->second.path);
				DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_REMOVED);
				owner().itemRemoved(&owner(), ev);
			}
		}
	}

private:
	DirectoryWatcherStrategy();
	DirectoryWatcherStrategy(const DirectoryWatcherStrategy&);
	DirectoryWatcherStrategy& operator = (const DirectoryWatcherStrategy&);
	
	DirectoryWatcher& _owner;
};


#if POCO_OS == POCO_OS_WINDOWS_NT


class WindowsDirectoryWatcherStrategy: public DirectoryWatcherStrategy
{
public:
	WindowsDirectoryWatcherStrategy(DirectoryWatcher& owner):
		DirectoryWatcherStrategy(owner)
	{
		_hStopped = CreateEventW(NULL, FALSE, FALSE, NULL);
		if (!_hStopped)
			throw SystemException("cannot create event");
	}
	
	~WindowsDirectoryWatcherStrategy()
	{
		CloseHandle(_hStopped);
	}
	
	void run()
	{
		ItemInfoMap entries;
		scan(entries);
		
		DWORD filter = FILE_NOTIFY_CHANGE_FILE_NAME | FILE_NOTIFY_CHANGE_DIR_NAME;
		if (owner().eventMask() & DirectoryWatcher::DW_ITEM_MODIFIED)
			filter |= FILE_NOTIFY_CHANGE_SIZE | FILE_NOTIFY_CHANGE_LAST_WRITE;
		
		std::string path(owner().directory().path());
#if defined(POCO_WIN32_UTF8)
		std::wstring upath;
		Poco::UnicodeConverter::toUTF16(path.c_str(), upath);
		HANDLE hChange = FindFirstChangeNotificationW(upath.c_str(), FALSE, filter);
#else
		HANDLE hChange = FindFirstChangeNotificationA(path.c_str(), FALSE, filter);
#endif

		if (hChange == INVALID_HANDLE_VALUE)
		{
			try
			{
				FileImpl::handleLastErrorImpl(path);
			}
			catch (Poco::Exception& exc)
			{
				owner().scanError(&owner(), exc);
			}
			return;
		}
		
		bool stopped = false;
		while (!stopped)
		{
			try
			{
				HANDLE h[2];
				h[0] = _hStopped;
				h[1] = hChange;
				switch (WaitForMultipleObjects(2, h, FALSE, INFINITE))
				{
				case WAIT_OBJECT_0:
					stopped = true;
					break;
				case WAIT_OBJECT_0 + 1:
					{
						ItemInfoMap newEntries;
						scan(newEntries);
						compare(entries, newEntries);
						std::swap(entries, newEntries);
						if (FindNextChangeNotification(hChange) == FALSE)
						{
							FileImpl::handleLastErrorImpl(path);
						}
					}
					break;
				default:
					throw SystemException("failed to wait for directory changes");
				}
			}
			catch (Poco::Exception& exc)
			{
				owner().scanError(&owner(), exc);
			}			
		}
		FindCloseChangeNotification(hChange);
	}
	
	void stop()
	{
		SetEvent(_hStopped);
	}
	
	bool supportsMoveEvents() const
	{
		return false;
	}
	
private:
	HANDLE _hStopped;
};


#elif POCO_OS == POCO_OS_LINUX


class LinuxDirectoryWatcherStrategy: public DirectoryWatcherStrategy
{
public:
	LinuxDirectoryWatcherStrategy(DirectoryWatcher& owner):
		DirectoryWatcherStrategy(owner),
		_fd(-1),
		_stopped(false)
	{
		_fd = inotify_init();
		if (_fd == -1) throw Poco::IOException("cannot initialize inotify", errno);
	}
	
	~LinuxDirectoryWatcherStrategy()
	{
		close(_fd);
	}
	
	void run()
	{
		int mask = 0;
		if (owner().eventMask() & DirectoryWatcher::DW_ITEM_ADDED)
			mask |= IN_CREATE;
		if (owner().eventMask() & DirectoryWatcher::DW_ITEM_REMOVED)
			mask |= IN_DELETE;
		if (owner().eventMask() & DirectoryWatcher::DW_ITEM_MODIFIED)
			mask |= IN_MODIFY;
		if (owner().eventMask() & DirectoryWatcher::DW_ITEM_MOVED_FROM)
			mask |= IN_MOVED_FROM;
		if (owner().eventMask() & DirectoryWatcher::DW_ITEM_MOVED_TO)
			mask |= IN_MOVED_TO;
		int wd = inotify_add_watch(_fd, owner().directory().path().c_str(), mask);
		if (wd == -1)
		{
			try
			{
				FileImpl::handleLastErrorImpl(owner().directory().path());
			}
			catch (Poco::Exception& exc)
			{
				owner().scanError(&owner(), exc);
			}
		}
		
		Poco::Buffer<char> buffer(4096);
		while (!_stopped)
		{
			fd_set fds;
			FD_ZERO(&fds);
			FD_SET(_fd, &fds);

			struct timeval tv;
			tv.tv_sec  = 0;
			tv.tv_usec = 200000;

			if (select(_fd + 1, &fds, NULL, NULL, &tv) == 1)
			{
				int n = read(_fd, buffer.begin(), buffer.size());
				int i = 0;
				if (n > 0)
				{
					while (n > 0)
					{
						struct inotify_event* pEvent = reinterpret_cast<struct inotify_event*>(buffer.begin() + i);
						
						if (pEvent->len > 0)
						{						
							if (!owner().eventsSuspended())
							{
								Poco::Path p(owner().directory().path());
								p.makeDirectory();
								p.setFileName(pEvent->name);
								Poco::File f(p.toString());
	
								if ((pEvent->mask & IN_CREATE) && (owner().eventMask() & DirectoryWatcher::DW_ITEM_ADDED))
								{
									DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_ADDED);
									owner().itemAdded(&owner(), ev);
								}
								if ((pEvent->mask & IN_DELETE) && (owner().eventMask() & DirectoryWatcher::DW_ITEM_REMOVED))
								{
									DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_REMOVED);
									owner().itemRemoved(&owner(), ev);
								}
								if ((pEvent->mask & IN_MODIFY) && (owner().eventMask() & DirectoryWatcher::DW_ITEM_MODIFIED))
								{
									DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_MODIFIED);
									owner().itemModified(&owner(), ev);
								}
								if ((pEvent->mask & IN_MOVED_FROM) && (owner().eventMask() & DirectoryWatcher::DW_ITEM_MOVED_FROM))
								{
									DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_MOVED_FROM);
									owner().itemMovedFrom(&owner(), ev);
								}
								if ((pEvent->mask & IN_MOVED_TO) && (owner().eventMask() & DirectoryWatcher::DW_ITEM_MOVED_TO))
								{
									DirectoryWatcher::DirectoryEvent ev(f, DirectoryWatcher::DW_ITEM_MOVED_TO);
									owner().itemMovedTo(&owner(), ev);
								}
							}
						}
						
						i += sizeof(inotify_event) + pEvent->len;
						n -= sizeof(inotify_event) + pEvent->len;
					}
				}
			}
		}
	}
	
	void stop()
	{
		_stopped = true;
	}
	
	bool supportsMoveEvents() const
	{
		return true;
	}

private:
	int _fd;
	bool _stopped;
};


#elif POCO_OS == POCO_OS_MAC_OS_X || POCO_OS == POCO_OS_FREE_BSD


class BSDDirectoryWatcherStrategy: public DirectoryWatcherStrategy
{
public:
	BSDDirectoryWatcherStrategy(DirectoryWatcher& owner):
		DirectoryWatcherStrategy(owner),
		_queueFD(-1),
		_dirFD(-1),
		_stopped(false)
	{
		_dirFD = open(owner.directory().path().c_str(), O_EVTONLY);
		if (_dirFD < 0) throw Poco::FileNotFoundException(owner.directory().path());
		_queueFD = kqueue();
		if (_queueFD < 0)
		{
			close(_dirFD);
			throw Poco::SystemException("Cannot create kqueue", errno);
		}
	}

	~BSDDirectoryWatcherStrategy()
	{
		close(_dirFD);
		close(_queueFD);
	}

	void run()
	{
		Poco::Timestamp lastScan;
		ItemInfoMap entries;
		scan(entries);

		while (!_stopped)
		{
			struct timespec timeout;
			timeout.tv_sec = 0;
			timeout.tv_nsec = 200000000;
			unsigned eventFilter = NOTE_WRITE;
			struct kevent event;
			struct kevent eventData;
			EV_SET(&event, _dirFD, EVFILT_VNODE, EV_ADD | EV_CLEAR, eventFilter, 0, 0);
			int nEvents = kevent(_queueFD, &event, 1, &eventData, 1, &timeout);
			if (nEvents < 0 || eventData.flags == EV_ERROR)
			{
				try
				{
					FileImpl::handleLastErrorImpl(owner().directory().path());
				}
				catch (Poco::Exception& exc)
				{
					owner().scanError(&owner(), exc);
				}
			}
			else if (nEvents > 0 || ((owner().eventMask() & DirectoryWatcher::DW_ITEM_MODIFIED) && lastScan.isElapsed(owner().scanInterval()*1000000)))
			{
				ItemInfoMap newEntries;
				scan(newEntries);
				compare(entries, newEntries);
				std::swap(entries, newEntries);
				lastScan.update();
			}
		}
	}

	void stop()
	{
		_stopped = true;
	}

	bool supportsMoveEvents() const
	{
		return false;
	}

private:
	int _queueFD;
	int _dirFD;
	bool _stopped;
};


#else


class PollingDirectoryWatcherStrategy: public DirectoryWatcherStrategy
{
public:
	PollingDirectoryWatcherStrategy(DirectoryWatcher& owner):
		DirectoryWatcherStrategy(owner)
	{
	}
	
	~PollingDirectoryWatcherStrategy()
	{
	}
	
	void run()
	{
		ItemInfoMap entries;
		scan(entries);
		while (!_stopped.tryWait(1000*owner().scanInterval()))
		{
			try
			{
				ItemInfoMap newEntries;
				scan(newEntries);
				compare(entries, newEntries);
				std::swap(entries, newEntries);
			}
			catch (Poco::Exception& exc)
			{
				owner().scanError(&owner(), exc);
			}
		}
	}
	
	void stop()
	{
		_stopped.set();
	}

	bool supportsMoveEvents() const
	{
		return false;
	}

private:
	Poco::Event _stopped;
};


#endif


DirectoryWatcher::DirectoryWatcher(const std::string& path, int eventMask, int scanInterval):
	_directory(path),
	_eventMask(eventMask),
	_scanInterval(scanInterval)
{
	init();
}

	
DirectoryWatcher::DirectoryWatcher(const Poco::File& directory, int eventMask, int scanInterval):
	_directory(directory),
	_eventMask(eventMask),
	_scanInterval(scanInterval)
{
	init();
}


DirectoryWatcher::~DirectoryWatcher()
{
	try
	{
		stop();
		delete _pStrategy;
	}
	catch (...)
	{
		poco_unexpected();
	}
}

	
void DirectoryWatcher::suspendEvents()
{
	poco_assert (_eventsSuspended > 0);
	
	_eventsSuspended--;
}


void DirectoryWatcher::resumeEvents()
{
	_eventsSuspended++;
}


void DirectoryWatcher::init()
{
	if (!_directory.exists())
		throw Poco::FileNotFoundException(_directory.path());
		
	if (!_directory.isDirectory())
		throw Poco::InvalidArgumentException("not a directory", _directory.path());

#if POCO_OS == POCO_OS_WINDOWS_NT
	_pStrategy = new WindowsDirectoryWatcherStrategy(*this);
#elif POCO_OS == POCO_OS_LINUX
	_pStrategy = new LinuxDirectoryWatcherStrategy(*this);
#elif POCO_OS == POCO_OS_MAC_OS_X || POCO_OS == POCO_OS_FREE_BSD
	_pStrategy = new BSDDirectoryWatcherStrategy(*this);
#else
	_pStrategy = new PollingDirectoryWatcherStrategy(*this);
#endif
	_thread.start(*this);
}

	
void DirectoryWatcher::run()
{
	_pStrategy->run();
}


void DirectoryWatcher::stop()
{
	_pStrategy->stop();
	_thread.join();
}


bool DirectoryWatcher::supportsMoveEvents() const
{
	return _pStrategy->supportsMoveEvents();
}


} // namespace Poco


#endif // POCO_NO_INOTIFY
