//
// TypeInfo.h
//
// $Id: //poco/Main/Data/ODBC/include/Poco/Data/ODBC/TypeInfo.h#1 $
//
// Library: ODBC
// Package: ODBC
// Module:  TypeInfo
//
// Definition of TypeInfo.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_DataTypes_INCLUDED
#define Data_ODBC_DataTypes_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/NamedTuple.h"
#include "Poco/DynamicAny.h"
#include <vector>
#include <map>
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqlext.h>


namespace Poco {
namespace Data {
namespace ODBC {


class ODBC_API TypeInfo
	/// Datatypes mapping utility class.
	///
	/// This class provides mapping between C and SQL datatypes as well
	/// as datatypes supported by the underlying database. In order for database
	/// types to be available, a valid conection handle must be supplied at either
	/// object construction time, or at a later point in time, through call to
	/// fillTypeInfo member function.
	///
	/// Class also provides a convenient debugging function that prints
	/// tabulated data to an output stream.
{
public:
	typedef std::map<int, int> DataTypeMap;
	typedef DataTypeMap::value_type ValueType;
	typedef Poco::NamedTuple<std::string,
		SQLSMALLINT,
		SQLINTEGER,
		std::string,
		std::string,
		std::string,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLSMALLINT,
		std::string,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLSMALLINT,
		SQLINTEGER,
		SQLSMALLINT> TypeInfoTup;
	typedef std::vector<TypeInfoTup> TypeInfoVec;

	explicit TypeInfo(SQLHDBC* pHDBC=0);
		/// Creates the TypeInfo.

	~TypeInfo();
		/// Destroys the TypeInfo.

	int cDataType(int sqlDataType) const;
		/// Returns C data type corresponding to supplied SQL data type.

	int sqlDataType(int cDataType) const;
		/// Returns SQL data type corresponding to supplied C data type.

	void fillTypeInfo(SQLHDBC pHDBC);
		/// Fills the data type info structure for the database.

	DynamicAny getInfo(SQLSMALLINT type, const std::string& param) const;
		/// Returns information about specified data type as specified by parameter 'type'.
		/// The requested information is specified by parameter 'param'.
		/// Will fail with a Poco::NotFoundException thrown if the param is not found
		
	bool tryGetInfo(SQLSMALLINT type, const std::string& param, DynamicAny& result) const;
		/// Returns information about specified data type as specified by parameter 'type' in param result.
		/// The requested information is specified by parameter 'param'.
		/// Will return false if the param is not found. The value of result will be not changed in this case.
			

	void print(std::ostream& ostr);
		/// Prints all the types (as reported by the underlying database)
		/// to the supplied output stream.

private:
	void fillCTypes();
	void fillSQLTypes();

	DataTypeMap _cDataTypes; 
	DataTypeMap _sqlDataTypes; 
	TypeInfoVec _typeInfo;
	SQLHDBC*    _pHDBC;
};


} } } // namespace Poco::Data::ODBC


#endif
