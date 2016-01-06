//
// Socket.cpp
//
// $Id: //poco/1.4/Net/src/Socket.cpp#3 $
//
// Library: Net
// Package: Sockets
// Module:  Socket
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/Socket.h"
#include "Poco/Net/StreamSocketImpl.h"
#include "Poco/Timestamp.h"
#include <algorithm>
#include <string.h> // FD_SET needs memset on some platforms, so we can't use <cstring>
#if defined(POCO_HAVE_FD_EPOLL)
#include <sys/epoll.h>
#elif defined(POCO_HAVE_FD_POLL)
#include "Poco/SharedPtr.h"
#include <poll.h>
typedef Poco::SharedPtr<pollfd, 
	Poco::ReferenceCounter, 
	Poco::ReleaseArrayPolicy<pollfd> > SharedPollArray;
#endif


namespace Poco {
namespace Net {


Socket::Socket():
	_pImpl(new StreamSocketImpl)
{
}


Socket::Socket(SocketImpl* pImpl):
	_pImpl(pImpl)
{
	poco_check_ptr (_pImpl);
}


Socket::Socket(const Socket& socket):
	_pImpl(socket._pImpl)
{
	poco_check_ptr (_pImpl);

	_pImpl->duplicate();
}

	
Socket& Socket::operator = (const Socket& socket)
{
	if (&socket != this)
	{
		if (_pImpl) _pImpl->release();
		_pImpl = socket._pImpl;
		if (_pImpl) _pImpl->duplicate();
	}
	return *this;
}


Socket::~Socket()
{
	_pImpl->release();
}


int Socket::select(SocketList& readList, SocketList& writeList, SocketList& exceptList, const Poco::Timespan& timeout)
{
#if defined(POCO_HAVE_FD_EPOLL)

	int epollSize = readList.size() + writeList.size() + exceptList.size();
	if (epollSize == 0) return 0;

	int epollfd = -1;
	{
		struct epoll_event eventsIn[epollSize];
		memset(eventsIn, 0, sizeof(eventsIn));
		struct epoll_event* eventLast = eventsIn;
		for (SocketList::iterator it = readList.begin(); it != readList.end(); ++it)
		{
			poco_socket_t sockfd = it->sockfd();
			if (sockfd != POCO_INVALID_SOCKET)
			{
				struct epoll_event* e = eventsIn;
				for (; e != eventLast; ++e)
				{
					if (reinterpret_cast<Socket*>(e->data.ptr)->sockfd() == sockfd)
						break;
				}
				if (e == eventLast)
				{
					e->data.ptr = &(*it);
					++eventLast;
				}
				e->events |= EPOLLIN;
			}
		}

		for (SocketList::iterator it = writeList.begin(); it != writeList.end(); ++it)
		{
			poco_socket_t sockfd = it->sockfd();
			if (sockfd != POCO_INVALID_SOCKET)
			{
				struct epoll_event* e = eventsIn;
				for (; e != eventLast; ++e)
				{
					if (reinterpret_cast<Socket*>(e->data.ptr)->sockfd() == sockfd)
						break;
				}
				if (e == eventLast)
				{
					e->data.ptr = &(*it);
					++eventLast;
				}
				e->events |= EPOLLOUT;
			}
		}

		for (SocketList::iterator it = exceptList.begin(); it != exceptList.end(); ++it)
		{
			poco_socket_t sockfd = it->sockfd();
			if (sockfd != POCO_INVALID_SOCKET)
			{
				struct epoll_event* e = eventsIn;
				for (; e != eventLast; ++e)
				{
					if (reinterpret_cast<Socket*>(e->data.ptr)->sockfd() == sockfd)
						break;
				}
				if (e == eventLast)
				{
					e->data.ptr = &(*it);
					++eventLast;
				}
				e->events |= EPOLLERR;
			}
		}

		epollSize = eventLast - eventsIn;
		epollfd = epoll_create(epollSize);
		if (epollfd < 0)
		{
			char buf[1024];
			strerror_r(errno, buf, sizeof(buf));
			SocketImpl::error(std::string("Can't create epoll queue: ") + buf);
		}

		for (struct epoll_event* e = eventsIn; e != eventLast; ++e)
		{
			poco_socket_t sockfd = reinterpret_cast<Socket*>(e->data.ptr)->sockfd();
			if (sockfd != POCO_INVALID_SOCKET)
			{
				if (epoll_ctl(epollfd, EPOLL_CTL_ADD, sockfd, e) < 0)
				{
					char buf[1024];
					strerror_r(errno, buf, sizeof(buf));
					::close(epollfd);
					SocketImpl::error(std::string("Can't insert socket to epoll queue: ") + buf);
				}
			}
		}
	}

	struct epoll_event eventsOut[epollSize];
	memset(eventsOut, 0, sizeof(eventsOut));

	Poco::Timespan remainingTime(timeout);
	int rc;
	do
	{
		Poco::Timestamp start;
		rc = epoll_wait(epollfd, eventsOut, epollSize, remainingTime.totalMilliseconds());
		if (rc < 0 && SocketImpl::lastError() == POCO_EINTR)
		{
 			Poco::Timestamp end;
			Poco::Timespan waited = end - start;
			if (waited < remainingTime)
				remainingTime -= waited;
			else
				remainingTime = 0;
		}
	}
	while (rc < 0 && SocketImpl::lastError() == POCO_EINTR);

	::close(epollfd);
	if (rc < 0) SocketImpl::error();

 	SocketList readyReadList;
	SocketList readyWriteList;
	SocketList readyExceptList;
	for (int n = 0; n < rc; ++n)
	{
		if (eventsOut[n].events & EPOLLERR)
			readyExceptList.push_back(*reinterpret_cast<Socket*>(eventsOut[n].data.ptr));
		if (eventsOut[n].events & EPOLLIN)
			readyReadList.push_back(*reinterpret_cast<Socket*>(eventsOut[n].data.ptr));
		if (eventsOut[n].events & EPOLLOUT)
			readyWriteList.push_back(*reinterpret_cast<Socket*>(eventsOut[n].data.ptr));
	}
	std::swap(readList, readyReadList);
	std::swap(writeList, readyWriteList);
	std::swap(exceptList, readyExceptList);
	return readList.size() + writeList.size() + exceptList.size();

#elif defined(POCO_HAVE_FD_POLL)

	nfds_t nfd = readList.size() + writeList.size() + exceptList.size();
	if (0 == nfd) return 0;

	SharedPollArray pPollArr = new pollfd[nfd];

	int idx = 0;
	for (SocketList::iterator it = readList.begin(); it != readList.end(); ++it)
	{
		pPollArr[idx].fd = int(it->sockfd());
		pPollArr[idx++].events |= POLLIN;
	}

	SocketList::iterator begR = readList.begin();
	SocketList::iterator endR = readList.end();
	for (SocketList::iterator it = writeList.begin(); it != writeList.end(); ++it)
	{
		SocketList::iterator pos = std::find(begR, endR, *it);
		if (pos != endR) 
		{
			pPollArr[pos-begR].events |= POLLOUT;
			--nfd;
		}
		else
		{
			pPollArr[idx].fd = int(it->sockfd());
			pPollArr[idx++].events |= POLLOUT;
		}
	}

	SocketList::iterator begW = writeList.begin();
	SocketList::iterator endW = writeList.end();
	for (SocketList::iterator it = exceptList.begin(); it != exceptList.end(); ++it)
	{
		SocketList::iterator pos = std::find(begR, endR, *it);
		if (pos != endR) --nfd;
		else
		{
			SocketList::iterator pos = std::find(begW, endW, *it);
			if (pos != endW) --nfd;
			else pPollArr[idx++].fd = int(it->sockfd());
		}
	}

	Poco::Timespan remainingTime(timeout);
	int rc;
	do
	{
		Poco::Timestamp start;
		rc = ::poll(pPollArr, nfd, timeout.totalMilliseconds());
		if (rc < 0 && SocketImpl::lastError() == POCO_EINTR)
		{
			Poco::Timestamp end;
			Poco::Timespan waited = end - start;
			if (waited < remainingTime) remainingTime -= waited;
			else remainingTime = 0;
		}
	}
	while (rc < 0 && SocketImpl::lastError() == POCO_EINTR);
	if (rc < 0) SocketImpl::error();

	SocketList readyReadList;
	SocketList readyWriteList;
	SocketList readyExceptList;

	SocketList::iterator begE = exceptList.begin();
	SocketList::iterator endE = exceptList.end();
	for (int idx = 0; idx < nfd; ++idx)
	{
		SocketList::iterator slIt = std::find_if(begR, endR, Socket::FDCompare(pPollArr[idx].fd));
		if (POLLIN & pPollArr[idx].revents && slIt != endR) readyReadList.push_back(*slIt);
		slIt = std::find_if(begW, endW, Socket::FDCompare(pPollArr[idx].fd));
		if (POLLOUT & pPollArr[idx].revents && slIt != endW) readyWriteList.push_back(*slIt);
		slIt = std::find_if(begE, endE, Socket::FDCompare(pPollArr[idx].fd));
		if (POLLERR & pPollArr[idx].revents && slIt != endE) readyExceptList.push_back(*slIt);
	}
	std::swap(readList, readyReadList);
	std::swap(writeList, readyWriteList);
	std::swap(exceptList, readyExceptList);
	return readList.size() + writeList.size() + exceptList.size();

#else

	fd_set fdRead;
	fd_set fdWrite;
	fd_set fdExcept;
	int nfd = 0;
	FD_ZERO(&fdRead);
	for (SocketList::const_iterator it = readList.begin(); it != readList.end(); ++it)
	{
		poco_socket_t fd = it->sockfd();
		if (fd != POCO_INVALID_SOCKET)
		{
			if (int(fd) > nfd)
				nfd = int(fd);
			FD_SET(fd, &fdRead);
		}
	}
	FD_ZERO(&fdWrite);
	for (SocketList::const_iterator it = writeList.begin(); it != writeList.end(); ++it)
	{
		poco_socket_t fd = it->sockfd();
		if (fd != POCO_INVALID_SOCKET)
		{
			if (int(fd) > nfd)
				nfd = int(fd);
			FD_SET(fd, &fdWrite);
		}
	}
	FD_ZERO(&fdExcept);
	for (SocketList::const_iterator it = exceptList.begin(); it != exceptList.end(); ++it)
	{
		poco_socket_t fd = it->sockfd();
		if (fd != POCO_INVALID_SOCKET)
		{
			if (int(fd) > nfd)
				nfd = int(fd);
			FD_SET(fd, &fdExcept);
		}
	}
	if (nfd == 0) return 0;
	Poco::Timespan remainingTime(timeout);
	int rc;
	do
	{
		struct timeval tv;
		tv.tv_sec  = (long) remainingTime.totalSeconds();
		tv.tv_usec = (long) remainingTime.useconds();
		Poco::Timestamp start;
		rc = ::select(nfd + 1, &fdRead, &fdWrite, &fdExcept, &tv);
		if (rc < 0 && SocketImpl::lastError() == POCO_EINTR)
		{
			Poco::Timestamp end;
			Poco::Timespan waited = end - start;
			if (waited < remainingTime)
				remainingTime -= waited;
			else
				remainingTime = 0;
		}
	}
	while (rc < 0 && SocketImpl::lastError() == POCO_EINTR);
	if (rc < 0) SocketImpl::error();
	
	SocketList readyReadList;
	for (SocketList::const_iterator it = readList.begin(); it != readList.end(); ++it)
	{
		poco_socket_t fd = it->sockfd();
		if (fd != POCO_INVALID_SOCKET)
		{
			if (FD_ISSET(fd, &fdRead))
				readyReadList.push_back(*it);
		}
	}
	std::swap(readList, readyReadList);
	SocketList readyWriteList;
	for (SocketList::const_iterator it = writeList.begin(); it != writeList.end(); ++it)
	{
		poco_socket_t fd = it->sockfd();
		if (fd != POCO_INVALID_SOCKET)
		{
			if (FD_ISSET(fd, &fdWrite))
				readyWriteList.push_back(*it);
		}
	}
	std::swap(writeList, readyWriteList);
	SocketList readyExceptList;
	for (SocketList::const_iterator it = exceptList.begin(); it != exceptList.end(); ++it)
	{
		poco_socket_t fd = it->sockfd();
		if (fd != POCO_INVALID_SOCKET)
		{
			if (FD_ISSET(fd, &fdExcept))
				readyExceptList.push_back(*it);
		}
	}
	std::swap(exceptList, readyExceptList);	
	return rc; 

#endif // POCO_HAVE_FD_EPOLL
}


} } // namespace Poco::Net
