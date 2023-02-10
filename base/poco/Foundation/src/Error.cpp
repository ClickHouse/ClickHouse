//
// Error.cpp
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


	class StrErrorHelper
		/// This little hack magically handles all variants
		/// of strerror_r() (POSIX and GLIBC) and strerror().
	{
	public:
		explicit StrErrorHelper(int err)
		{
			_buffer[0] = 0;

#if (_XOPEN_SOURCE >= 600) || POCO_OS == POCO_OS_ANDROID || __APPLE__
			setMessage(strerror_r(err, _buffer, sizeof(_buffer)));
#elif _GNU_SOURCE
			setMessage(strerror_r(err, _buffer, sizeof(_buffer)));
#else
			setMessage(strerror(err));
#endif		
		}
		
		~StrErrorHelper()
		{
		}
		
		const std::string& message() const
		{
			return _message;
		}
		
	protected:
		void setMessage(int rc)
			/// Handles POSIX variant
		{
			_message = _buffer;
		}
		
		void setMessage(const char* msg)
			/// Handles GLIBC variant
		{
			_message = msg;
		}
		
	private:
		char _buffer[256];
		std::string _message;
	};

	std::string Error::getMessage(int errorCode)
	{
		StrErrorHelper helper(errorCode);
		return helper.message();
	}


#endif


} // namespace Poco
