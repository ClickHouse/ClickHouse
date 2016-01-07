//
// Utility.cpp
//
// $Id: //poco/Main/Data/MySQL/src/Utility.cpp#5 $
//
// Library: MySQL
// Package: MySQL
// Module:  Utility
//
// Implementation of Utility
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/Utility.h"
#include <mysql.h>


namespace Poco {
namespace Data {
namespace MySQL {


std::string Utility::serverInfo(MYSQL* pHandle)
{
	std::string info(mysql_get_server_info(pHandle));
	return info;
}


std::string Utility::serverInfo(Session& session)
{
	std::string info(mysql_get_server_info(handle(session)));
	return info;
}


unsigned long Utility::serverVersion(MYSQL* pHandle)
{
	return mysql_get_server_version(pHandle);
}


unsigned long Utility::serverVersion(Session& session)
{
	return mysql_get_server_version(handle(session));
}


std::string Utility::hostInfo(MYSQL* pHandle)
{
	std::string info(mysql_get_host_info(pHandle));
	return info;
}


std::string Utility::hostInfo(Session& session)
{
	std::string info(mysql_get_host_info(handle(session)));
	return info;
}


} } } // namespace Poco::Data::MySQL
