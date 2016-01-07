//
// Error.cpp
//
// $Id: //poco/1.4/Foundation/src/Error.cpp#3 $
//
// Library: Foundation
// Package: Core
// Module:  Error
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Foundation.h"
#include "Poco/UnicodeConverter.h"
#include "Poco/Error.h"
#include <string>
#include <string.h>
#include <errno.h>


namespace Poco {


#ifdef POCO_OS_FAMILY_WINDOWS
	DWORD Error::last()
	{
		return GetLastError();
	}


	std::string Error::getMessage(DWORD errorCode)
	{
		std::string errMsg;
		DWORD dwFlg = FORMAT_MESSAGE_ALLOCATE_BUFFER | FORMAT_MESSAGE_FROM_SYSTEM | FORMAT_MESSAGE_IGNORE_INSERTS;
	#if defined(POCO_WIN32_UTF8) && !defined(POCO_NO_WSTRING)
		LPWSTR lpMsgBuf = 0;
		if (FormatMessageW(dwFlg, 0, errorCode, 0, (LPWSTR) & lpMsgBuf, 0, NULL))
			UnicodeConverter::toUTF8(lpMsgBuf, errMsg);
	#else
		LPTSTR lpMsgBuf = 0;
		if (FormatMessageA(dwFlg, 0, errorCode, 0, (LPTSTR) & lpMsgBuf, 0, NULL))
			errMsg = lpMsgBuf;
	#endif
		LocalFree(lpMsgBuf);
		return errMsg;
	}

#else
	int Error::last()
	{
		return errno;
	}


	std::string Error::getMessage(int errorCode)
	{
		/* Reentrant version of `strerror'.
		   There are 2 flavors of `strerror_r', GNU which returns the string
		   and may or may not use the supplied temporary buffer and POSIX one
		   which fills the string into the buffer.
		   To use the POSIX version, -D_XOPEN_SOURCE=600 or -D_POSIX_C_SOURCE=200112L
		   without -D_GNU_SOURCE is needed, otherwise the GNU version is
		   preferred.
		*/
#if defined _GNU_SOURCE && !POCO_ANDROID
		char errmsg[256] = "";
		return std::string(strerror_r(errorCode, errmsg, 256));
#elif (_XOPEN_SOURCE >= 600) || POCO_ANDROID
		char errmsg[256] = "";
		strerror_r(errorCode, errmsg, 256);
		return errmsg;
#else
		return std::string(strerror(errorCode));
#endif
	}

#endif


} // namespace Poco
