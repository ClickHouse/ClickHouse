//
// EnvironmentHandle.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/EnvironmentHandle.h#3 $
//
// Library: ODBC
// Package: ODBC
// Module:  EnvironmentHandle
//
// Definition of EnvironmentHandle.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_EnvironmentHandle_INCLUDED
#define Data_ODBC_EnvironmentHandle_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqltypes.h>


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API EnvironmentHandle
/// ODBC environment handle class
{
public:
	EnvironmentHandle();
		/// Creates the EnvironmentHandle.

	~EnvironmentHandle();
		/// Destroys the EnvironmentHandle.

	operator const SQLHENV& () const;
		/// Const conversion operator into reference to native type.

	const SQLHENV& handle() const;
		/// Returns const reference to handle.

private:
	operator SQLHENV& ();
		/// Conversion operator into reference to native type.

	SQLHENV& handle();
		/// Returns reference to handle.

	EnvironmentHandle(const EnvironmentHandle&);
	const EnvironmentHandle& operator=(const EnvironmentHandle&);

	SQLHENV _henv;
	bool    _isOwner;
};


///
/// inlines
///
inline EnvironmentHandle::operator const SQLHENV& () const
{
	return handle();
}


inline const SQLHENV& EnvironmentHandle::handle() const
{
	return _henv;
}


inline EnvironmentHandle::operator SQLHENV& ()
{
	return handle();
}


inline SQLHENV& EnvironmentHandle::handle()
{
	return _henv;
}


} } } // namespace Poco::Data::ODBC


#endif
