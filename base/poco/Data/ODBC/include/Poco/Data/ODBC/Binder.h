//
// Binder.h
//
// Library: Data/ODBC
// Package: ODBC
// Module:  Binder
//
// Definition of the Binder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ODBC_Binder_INCLUDED
#define Data_ODBC_Binder_INCLUDED


#include "Poco/Data/ODBC/ODBC.h"
#include "Poco/Data/AbstractBinder.h"
#include "Poco/Data/LOB.h"
#include "Poco/Data/ODBC/Handle.h"
#include "Poco/Data/ODBC/Parameter.h"
#include "Poco/Data/ODBC/ODBCMetaColumn.h"
#include "Poco/Data/ODBC/Utility.h"
#include "Poco/Data/ODBC/TypeInfo.h"
#include "Poco/Exception.h"
#include <vector>
#include <deque>
#include <list>
#include <map>
#ifdef POCO_OS_FAMILY_WINDOWS
#include <windows.h>
#endif
#include <sqlext.h>


namespace Poco {


class DateTime;


namespace Data {


class Date;
class Time;


namespace ODBC {


class ODBC_API Binder: public Poco::Data::AbstractBinder
	/// Binds placeholders in the sql query to the provided values. Performs data types mapping.
{
public:
	typedef AbstractBinder::Direction Direction;
	typedef std::map<SQLPOINTER, SQLLEN> ParamMap;

	static const size_t DEFAULT_PARAM_SIZE = 1024;

	enum ParameterBinding
	{
		PB_IMMEDIATE,
		PB_AT_EXEC
	};

	Binder(const StatementHandle& rStmt,
		std::size_t maxFieldSize,
		ParameterBinding dataBinding = PB_IMMEDIATE,
		TypeInfo* pDataTypes = 0);
		/// Creates the Binder.

	~Binder();
		/// Destroys the Binder.

	void bind(std::size_t pos, const Poco::Int8& val, Direction dir);
		/// Binds an Int8.

	void bind(std::size_t pos, const std::vector<Poco::Int8>& val, Direction dir);
		/// Binds an Int8 vector.

	void bind(std::size_t pos, const std::deque<Poco::Int8>& val, Direction dir);
		/// Binds an Int8 deque.

	void bind(std::size_t pos, const std::list<Poco::Int8>& val, Direction dir);
		/// Binds an Int8 list.

	void bind(std::size_t pos, const Poco::UInt8& val, Direction dir);
		/// Binds an UInt8.

	void bind(std::size_t pos, const std::vector<Poco::UInt8>& val, Direction dir);
		/// Binds an UInt8 vector.

	void bind(std::size_t pos, const std::deque<Poco::UInt8>& val, Direction dir);
		/// Binds an UInt8 deque.

	void bind(std::size_t pos, const std::list<Poco::UInt8>& val, Direction dir);
		/// Binds an UInt8 list.

	void bind(std::size_t pos, const Poco::Int16& val, Direction dir);
		/// Binds an Int16.
	
	void bind(std::size_t pos, const std::vector<Poco::Int16>& val, Direction dir);
		/// Binds an Int16 vector.

	void bind(std::size_t pos, const std::deque<Poco::Int16>& val, Direction dir);
		/// Binds an Int16 deque.

	void bind(std::size_t pos, const std::list<Poco::Int16>& val, Direction dir);
		/// Binds an Int16 list.

	void bind(std::size_t pos, const Poco::UInt16& val, Direction dir);
		/// Binds an UInt16.

	void bind(std::size_t pos, const std::vector<Poco::UInt16>& val, Direction dir);
		/// Binds an UInt16 vector.

	void bind(std::size_t pos, const std::deque<Poco::UInt16>& val, Direction dir);
		/// Binds an UInt16 deque.

	void bind(std::size_t pos, const std::list<Poco::UInt16>& val, Direction dir);
		/// Binds an UInt16 list.

	void bind(std::size_t pos, const Poco::Int32& val, Direction dir);
		/// Binds an Int32.

	void bind(std::size_t pos, const std::vector<Poco::Int32>& val, Direction dir);
		/// Binds an Int32 vector.

	void bind(std::size_t pos, const std::deque<Poco::Int32>& val, Direction dir);
		/// Binds an Int32 deque.

	void bind(std::size_t pos, const std::list<Poco::Int32>& val, Direction dir);
		/// Binds an Int32 list.

	void bind(std::size_t pos, const Poco::UInt32& val, Direction dir);
		/// Binds an UInt32.

	void bind(std::size_t pos, const std::vector<Poco::UInt32>& val, Direction dir);
		/// Binds an UInt32 vector.

	void bind(std::size_t pos, const std::deque<Poco::UInt32>& val, Direction dir);
		/// Binds an UInt32 deque.

	void bind(std::size_t pos, const std::list<Poco::UInt32>& val, Direction dir);
		/// Binds an UInt32 list.

	void bind(std::size_t pos, const Poco::Int64& val, Direction dir);
		/// Binds an Int64.

	void bind(std::size_t pos, const std::vector<Poco::Int64>& val, Direction dir);
		/// Binds an Int64 vector.

	void bind(std::size_t pos, const std::deque<Poco::Int64>& val, Direction dir);
		/// Binds an Int64 deque.

	void bind(std::size_t pos, const std::list<Poco::Int64>& val, Direction dir);
		/// Binds an Int64 list.

	void bind(std::size_t pos, const Poco::UInt64& val, Direction dir);
		/// Binds an UInt64.

	void bind(std::size_t pos, const std::vector<Poco::UInt64>& val, Direction dir);
		/// Binds an UInt64 vector.

	void bind(std::size_t pos, const std::deque<Poco::UInt64>& val, Direction dir);
		/// Binds an UInt64 deque.

	void bind(std::size_t pos, const std::list<Poco::UInt64>& val, Direction dir);
		/// Binds an UInt64 list.

#ifndef POCO_LONG_IS_64_BIT
	void bind(std::size_t pos, const long& val, Direction dir);
		/// Binds a long.

	void bind(std::size_t pos, const unsigned long& val, Direction dir);
		/// Binds an unsigned long.

	void bind(std::size_t pos, const std::vector<long>& val, Direction dir);
		/// Binds a long vector.

	void bind(std::size_t pos, const std::deque<long>& val, Direction dir);
		/// Binds a long deque.

	void bind(std::size_t pos, const std::list<long>& val, Direction dir);
		/// Binds a long list.
#endif

	void bind(std::size_t pos, const bool& val, Direction dir);
		/// Binds a boolean.

	void bind(std::size_t pos, const std::vector<bool>& val, Direction dir);
		/// Binds a boolean vector.

	void bind(std::size_t pos, const std::deque<bool>& val, Direction dir);
		/// Binds a boolean deque.

	void bind(std::size_t pos, const std::list<bool>& val, Direction dir);
		/// Binds a boolean list.

	void bind(std::size_t pos, const float& val, Direction dir);
		/// Binds a float.

	void bind(std::size_t pos, const std::vector<float>& val, Direction dir);
		/// Binds a float vector.

	void bind(std::size_t pos, const std::deque<float>& val, Direction dir);
		/// Binds a float deque.

	void bind(std::size_t pos, const std::list<float>& val, Direction dir);
		/// Binds a float list.

	void bind(std::size_t pos, const double& val, Direction dir);
		/// Binds a double.

	void bind(std::size_t pos, const std::vector<double>& val, Direction dir);
		/// Binds a double vector.

	void bind(std::size_t pos, const std::deque<double>& val, Direction dir);
		/// Binds a double deque.

	void bind(std::size_t pos, const std::list<double>& val, Direction dir);
		/// Binds a double list.

	void bind(std::size_t pos, const char& val, Direction dir);
		/// Binds a single character.

	void bind(std::size_t pos, const std::vector<char>& val, Direction dir);
		/// Binds a character vector.

	void bind(std::size_t pos, const std::deque<char>& val, Direction dir);
		/// Binds a character deque.

	void bind(std::size_t pos, const std::list<char>& val, Direction dir);
		/// Binds a character list.

	void bind(std::size_t pos, const std::string& val, Direction dir);
		/// Binds a string.

	void bind(std::size_t pos, const std::vector<std::string>& val, Direction dir);
		/// Binds a string vector.

	void bind(std::size_t pos, const std::deque<std::string>& val, Direction dir);
		/// Binds a string deque.

	void bind(std::size_t pos, const std::list<std::string>& val, Direction dir);
		/// Binds a string list.

	void bind(std::size_t pos, const UTF16String& val, Direction dir);
		/// Binds a string.

	void bind(std::size_t pos, const std::vector<UTF16String>& val, Direction dir);
		/// Binds a string vector.

	void bind(std::size_t pos, const std::deque<UTF16String>& val, Direction dir);
		/// Binds a string deque.

	void bind(std::size_t pos, const std::list<UTF16String>& val, Direction dir);
		/// Binds a string list.

	void bind(std::size_t pos, const BLOB& val, Direction dir);
		/// Binds a BLOB. In-bound only.

	void bind(std::size_t pos, const CLOB& val, Direction dir);
		/// Binds a CLOB. In-bound only.

	void bind(std::size_t pos, const std::vector<BLOB>& val, Direction dir);
		/// Binds a BLOB vector.

	void bind(std::size_t pos, const std::deque<BLOB>& val, Direction dir);
		/// Binds a BLOB deque.

	void bind(std::size_t pos, const std::list<BLOB>& val, Direction dir);
		/// Binds a BLOB list.

	void bind(std::size_t pos, const std::vector<CLOB>& val, Direction dir);
		/// Binds a CLOB vector.

	void bind(std::size_t pos, const std::deque<CLOB>& val, Direction dir);
		/// Binds a CLOB deque.

	void bind(std::size_t pos, const std::list<CLOB>& val, Direction dir);
		/// Binds a CLOB list.

	void bind(std::size_t pos, const Date& val, Direction dir);
		/// Binds a Date.

	void bind(std::size_t pos, const std::vector<Date>& val, Direction dir);
		/// Binds a Date vector.

	void bind(std::size_t pos, const std::deque<Date>& val, Direction dir);
		/// Binds a Date deque.

	void bind(std::size_t pos, const std::list<Date>& val, Direction dir);
		/// Binds a Date list.

	void bind(std::size_t pos, const Time& val, Direction dir);
		/// Binds a Time.

	void bind(std::size_t pos, const std::vector<Time>& val, Direction dir);
		/// Binds a Time vector.

	void bind(std::size_t pos, const std::deque<Time>& val, Direction dir);
		/// Binds a Time deque.

	void bind(std::size_t pos, const std::list<Time>& val, Direction dir);
		/// Binds a Time list.

	void bind(std::size_t pos, const DateTime& val, Direction dir);
		/// Binds a DateTime.

	void bind(std::size_t pos, const std::vector<DateTime>& val, Direction dir);
		/// Binds a DateTime vector.

	void bind(std::size_t pos, const std::deque<DateTime>& val, Direction dir);
		/// Binds a DateTime deque.

	void bind(std::size_t pos, const std::list<DateTime>& val, Direction dir);
		/// Binds a DateTime list.

	void bind(std::size_t pos, const NullData& val, Direction dir);
		/// Binds a null. In-bound only.

	void bind(std::size_t pos, const std::vector<NullData>& val, Direction dir);
		/// Binds a null vector.

	void bind(std::size_t pos, const std::deque<NullData>& val, Direction dir);
		/// Binds a null deque.

	void bind(std::size_t pos, const std::list<NullData>& val, Direction dir);
		/// Binds a null list.

	void setDataBinding(ParameterBinding binding);
		/// Set data binding type.

	ParameterBinding getDataBinding() const;
		/// Return data binding type.

	std::size_t parameterSize(SQLPOINTER pAddr) const;
		/// Returns bound data size for parameter at specified position.

	void synchronize();
		/// Transfers the results of non-POD outbound parameters from internal 
		/// holders back into the externally supplied buffers.

	void reset();
		/// Clears the cached storage.

private:
	typedef std::vector<SQLLEN*>                             LengthPtrVec;
	typedef std::vector<SQLLEN>                              LengthVec;
	typedef std::vector<LengthVec*>                          LengthVecVec;
	typedef std::vector<char*>                               CharPtrVec;
	typedef std::vector<UTF16Char*>                          UTF16CharPtrVec;
	typedef std::vector<bool*>                               BoolPtrVec;
	typedef std::vector<SQL_DATE_STRUCT>                     DateVec;
	typedef std::vector<DateVec*>                            DateVecVec;
	typedef std::vector<SQL_TIME_STRUCT>                     TimeVec;
	typedef std::vector<TimeVec*>                            TimeVecVec;
	typedef std::vector<SQL_TIMESTAMP_STRUCT>                DateTimeVec;
	typedef std::vector<DateTimeVec*>                        DateTimeVecVec;
	typedef std::vector<Poco::Any>                           AnyVec;
	typedef std::vector<AnyVec>                              AnyVecVec;
	typedef std::map<char*, std::string*>                    StringMap;
	typedef std::map<UTF16String::value_type*, UTF16String*> UTF16StringMap;
	typedef std::map<SQL_DATE_STRUCT*, Date*>                DateMap;
	typedef std::map<SQL_TIME_STRUCT*, Time*>                TimeMap;
	typedef std::map<SQL_TIMESTAMP_STRUCT*, DateTime*>       TimestampMap;

	void describeParameter(std::size_t pos);
		/// Sets the description field for the parameter, if needed.

	void bind(std::size_t pos, const char* const& pVal, Direction dir);
		/// Binds a const char ptr. 
		/// This is a private no-op in this implementation
		/// due to security risk.

	SQLSMALLINT toODBCDirection(Direction dir) const;
		/// Returns ODBC parameter direction based on the parameter binding direction
		/// specified by user.

	template <typename T>
	void bindImpl(std::size_t pos, T& val, SQLSMALLINT cDataType, Direction dir)
	{
		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, cDataType, colSize, decDigits);

		_lengthIndicator.push_back(0);

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			cDataType, 
			Utility::sqlDataType(cDataType), 
			colSize,
			decDigits,
			(SQLPOINTER) &val, 0, 0)))
		{
			throw StatementException(_rStmt, "SQLBindParameter()");
		}
	}

	template <typename L>
	void bindImplLOB(std::size_t pos, const L& val, Direction dir)
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("LOB parameter type can only be inbound.");

		SQLPOINTER pVal = (SQLPOINTER) val.rawContent();
		SQLINTEGER size = (SQLINTEGER) val.size();
			
		_inParams.insert(ParamMap::value_type(pVal, size));

		SQLLEN* pLenIn = new SQLLEN;
		*pLenIn  = size;

		if (PB_AT_EXEC == _paramBinding)
			*pLenIn  = SQL_LEN_DATA_AT_EXEC(size);

		_lengthIndicator.push_back(pLenIn);

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			SQL_PARAM_INPUT, 
			SQL_C_BINARY, 
			SQL_LONGVARBINARY, 
			(SQLUINTEGER) size,
			0,
			pVal,
			(SQLINTEGER) size, 
			_lengthIndicator.back())))
		{
			throw StatementException(_rStmt, "SQLBindParameter(LOB)");
		}
	}

	template <typename T>
	void bindImplVec(std::size_t pos, const std::vector<T>& val, SQLSMALLINT cDataType, Direction dir)
	{
		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("std::vector can only be bound immediately.");

		std::size_t length = val.size();
		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, cDataType, colSize, decDigits);
		setParamSetSize(length);

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length);
		}

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			cDataType, 
			Utility::sqlDataType(cDataType), 
			colSize,
			decDigits,
			(SQLPOINTER) &val[0], 
			0, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter()");
		}
	}

	template <typename C>
	void bindImplContainer(std::size_t pos, const C& val, SQLSMALLINT cDataType, Direction dir)
		/// Utility function - a "stand-in" for non-vector containers.
		/// Creates, fills and stores the reference to the replacement std::vector container
		/// for std::deque and std::list. Calls std::vector binding.
	{
		typedef typename C::value_type Type;

		if (_containers.size() <= pos)
			_containers.resize(pos + 1);

		_containers[pos].push_back(std::vector<Type>());

		std::vector<Type>& cont = RefAnyCast<std::vector<Type> >(_containers[pos].back());
		cont.assign(val.begin(), val.end());
		bindImplVec(pos, cont, cDataType, dir);
	}

	template <typename C>
	void bindImplContainerBool(std::size_t pos, const C& val, SQLSMALLINT cDataType, Direction dir)
	{
		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("std::vector can only be bound immediately.");

		std::size_t length = val.size();
		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, cDataType, colSize, decDigits);

		setParamSetSize(val.size());

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length);
		}

		if (_boolPtrs.size() <= pos)
			_boolPtrs.resize(pos + 1);

		_boolPtrs[pos] = new bool[val.size()];

		typename C::const_iterator it = val.begin();
		typename C::const_iterator end = val.end();
		for (int i = 0; it != end; ++it, ++i) _boolPtrs[pos][i] = *it;

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			cDataType, 
			Utility::sqlDataType(cDataType), 
			colSize,
			decDigits,
			(SQLPOINTER) &_boolPtrs[pos][0], 
			0, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter()");
		}
	}

	template <typename C>
	void bindImplContainerString(std::size_t pos, const C& val, Direction dir)
		/// Utility function to bind containers of strings.
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("String container parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("Containers can only be bound immediately.");

		std::size_t length = val.size();

		if (0 == length)
			throw InvalidArgumentException("Empty container not allowed.");

		setParamSetSize(length);

		SQLINTEGER size = 0;
		getColumnOrParameterSize(pos, size);
		poco_assert (size > 0);

		if (size == _maxFieldSize)
		{
			getMinValueSize(val, size);
			// accomodate for terminating zero
			if (size != _maxFieldSize) ++size;
		}

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1, SQL_NTS);
		}

		if (_charPtrs.size() <= pos)
			_charPtrs.resize(pos + 1, 0);

		_charPtrs[pos] = (char*) std::calloc(val.size() * size, sizeof(char));
		
		std::size_t strSize;
		std::size_t offset = 0;
		typename C::const_iterator it = val.begin();
		typename C::const_iterator end = val.end();
		for (; it != end; ++it)
		{
			strSize = it->size();
			if (strSize > size)	
				throw LengthExceededException("SQLBindParameter(std::vector<std::string>)");
			std::memcpy(_charPtrs[pos] + offset, it->c_str(), strSize);
			offset += size;
		}

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			SQL_C_CHAR, 
			SQL_LONGVARCHAR, 
			(SQLUINTEGER) size - 1,
			0,
			_charPtrs[pos], 
			(SQLINTEGER) size, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter(std::vector<std::string>)");
		}
	}

	template <typename C>
	void bindImplContainerUTF16String(std::size_t pos, const C& val, Direction dir)
		/// Utility function to bind containers of strings.
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("String container parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("Containers can only be bound immediately.");

		std::size_t length = val.size();
		if (0 == length)
			throw InvalidArgumentException("Empty container not allowed.");

		setParamSetSize(val.size());

		SQLINTEGER size = 0;
		getColumnOrParameterSize(pos, size);
		poco_assert(size > 0);

		if (size == _maxFieldSize)
		{
			getMinValueSize(val, size);
			// accomodate for terminating zero
			if (size != _maxFieldSize) size += sizeof(UTF16Char);
		}

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1, SQL_NTS);
		}

		if (_utf16CharPtrs.size() <= pos)
			_utf16CharPtrs.resize(pos + 1, 0);

		_utf16CharPtrs[pos] = (UTF16Char*)std::calloc(val.size() * size, sizeof(UTF16Char));

		std::size_t strSize;
		std::size_t offset = 0;
		char* pBuf = (char*)_utf16CharPtrs[pos];
		typename C::const_iterator it = val.begin();
		typename C::const_iterator end = val.end();
		for (; it != end; ++it)
		{
			strSize = it->size() * sizeof(UTF16Char);
			if (strSize > size)
				throw LengthExceededException("SQLBindParameter(std::vector<UTF16String>)");
			std::memcpy(pBuf + offset, it->data(), strSize);
			offset += size;
		}

		if (Utility::isError(SQLBindParameter(_rStmt,
			(SQLUSMALLINT)pos + 1,
			toODBCDirection(dir),
			SQL_C_WCHAR,
			SQL_WLONGVARCHAR,
			(SQLUINTEGER)size - 1,
			0,
			_utf16CharPtrs[pos],
			(SQLINTEGER)size,
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter(std::vector<UTF16String>)");
		}
	}

	template <typename C>
	void bindImplContainerLOB(std::size_t pos, const C& val, Direction dir)
	{
		typedef typename C::value_type LOBType;
		typedef typename LOBType::ValueType CharType;

		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("BLOB container parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("Containers can only be bound immediately.");

		std::size_t length = val.size();
		if (0 == length)
			throw InvalidArgumentException("Empty container not allowed.");

		setParamSetSize(length);

		SQLINTEGER size = 0;

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1);
		}

		std::vector<SQLLEN>::iterator lIt = _vecLengthIndicator[pos]->begin();
		std::vector<SQLLEN>::iterator lEnd = _vecLengthIndicator[pos]->end();
		typename C::const_iterator cIt = val.begin();
		for (; lIt != lEnd; ++lIt, ++cIt) 
		{
			SQLLEN sz = static_cast<SQLLEN>(cIt->size());
			if (sz > size) size = static_cast<SQLINTEGER>(sz);
			*lIt = sz;
		}

		if (_charPtrs.size() <= pos)
			_charPtrs.resize(pos + 1, 0);

		_charPtrs[pos] = (char*) std::calloc(val.size() * size, sizeof(CharType));
		poco_check_ptr (_charPtrs[pos]);

		std::size_t blobSize;
		std::size_t offset = 0;
		cIt = val.begin();
		typename C::const_iterator cEnd = val.end();
		for (; cIt != cEnd; ++cIt)
		{
			blobSize = cIt->size();
			if (blobSize > size)	
				throw LengthExceededException("SQLBindParameter(std::vector<BLOB>)");
			std::memcpy(_charPtrs[pos] + offset, cIt->rawContent(), blobSize * sizeof(CharType));
			offset += size;
		}

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			SQL_PARAM_INPUT, 
			SQL_C_BINARY, 
			SQL_LONGVARBINARY, 
			(SQLUINTEGER) size,
			0,
			_charPtrs[pos], 
			(SQLINTEGER) size, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter(std::vector<BLOB>)");
		}
	}

	template<typename C>
	void bindImplContainerDate(std::size_t pos, const C& val, Direction dir)
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("Date vector parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("std::vector can only be bound immediately.");

		std::size_t length = val.size();

		if (0 == length)
			throw InvalidArgumentException("Empty vector not allowed.");

		setParamSetSize(length);

		SQLINTEGER size = (SQLINTEGER) sizeof(SQL_DATE_STRUCT);

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1);
		}

		if (_dateVecVec.size() <= pos)
		{
			_dateVecVec.resize(pos + 1, 0);
			_dateVecVec[pos] = new DateVec(length ? length : 1);
		}

		Utility::dateSync(*_dateVecVec[pos], val);

		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, SQL_TYPE_DATE, colSize, decDigits);

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			SQL_C_TYPE_DATE, 
			SQL_TYPE_DATE, 
			colSize,
			decDigits,
			(SQLPOINTER) &(*_dateVecVec[pos])[0], 
			0, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter(Date[])");
		}
	}

	template<typename C>
	void bindImplContainerTime(std::size_t pos, const C& val, Direction dir)
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("Time container parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("Containers can only be bound immediately.");

		std::size_t length = val.size();
		if (0 == length)
			throw InvalidArgumentException("Empty container not allowed.");

		setParamSetSize(val.size());

		SQLINTEGER size = (SQLINTEGER) sizeof(SQL_TIME_STRUCT);

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1);
		}

		if (_timeVecVec.size() <= pos)
		{
			_timeVecVec.resize(pos + 1, 0);
			_timeVecVec[pos] = new TimeVec(length ? length : 1);
		}

		Utility::timeSync(*_timeVecVec[pos], val);

		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, SQL_TYPE_TIME, colSize, decDigits);

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			SQL_C_TYPE_TIME, 
			SQL_TYPE_TIME, 
			colSize,
			decDigits,
			(SQLPOINTER) &(*_timeVecVec[pos])[0], 
			0, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter(Time[])");
		}
	}

	template<typename C>
	void bindImplContainerDateTime(std::size_t pos, const C& val, Direction dir)
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("DateTime container parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("Containers can only be bound immediately.");

		std::size_t length = val.size();

		if (0 == length)
			throw InvalidArgumentException("Empty Containers not allowed.");

		setParamSetSize(length);

		SQLINTEGER size = (SQLINTEGER) sizeof(SQL_TIMESTAMP_STRUCT);

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1);
		}

		if (_dateTimeVecVec.size() <= pos)
		{
			_dateTimeVecVec.resize(pos + 1, 0);
			_dateTimeVecVec[pos] = new DateTimeVec(length ? length : 1);
		}

		Utility::dateTimeSync(*_dateTimeVecVec[pos], val);

		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, SQL_TYPE_TIMESTAMP, colSize, decDigits);

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			toODBCDirection(dir), 
			SQL_C_TYPE_TIMESTAMP, 
			SQL_TYPE_TIMESTAMP, 
			colSize,
			decDigits,
			(SQLPOINTER) &(*_dateTimeVecVec[pos])[0], 
			0, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter(Time[])");
		}
	}

	template<typename C>
	void bindImplNullContainer(std::size_t pos, const C& val, Direction dir)
	{
		if (isOutBound(dir) || !isInBound(dir))
			throw NotImplementedException("Null container parameter type can only be inbound.");

		if (PB_IMMEDIATE != _paramBinding)
			throw InvalidAccessException("Container can only be bound immediately.");

		std::size_t length = val.size();

		if (0 == length)
			throw InvalidArgumentException("Empty container not allowed.");

		setParamSetSize(length);

		SQLINTEGER size = SQL_NULL_DATA;

		if (_vecLengthIndicator.size() <= pos)
		{
			_vecLengthIndicator.resize(pos + 1, 0);
			_vecLengthIndicator[pos] = new LengthVec(length ? length : 1);
		}

		SQLINTEGER colSize = 0;
		SQLSMALLINT decDigits = 0;
		getColSizeAndPrecision(pos, SQL_C_STINYINT, colSize, decDigits);

		if (Utility::isError(SQLBindParameter(_rStmt, 
			(SQLUSMALLINT) pos + 1, 
			SQL_PARAM_INPUT, 
			SQL_C_STINYINT, 
			Utility::sqlDataType(SQL_C_STINYINT), 
			colSize,
			decDigits,
			0, 
			0, 
			&(*_vecLengthIndicator[pos])[0])))
		{
			throw StatementException(_rStmt, "SQLBindParameter()");
		}
	}

	void getColSizeAndPrecision(std::size_t pos, 
		SQLSMALLINT cDataType, 
		SQLINTEGER& colSize, 
		SQLSMALLINT& decDigits,
		std::size_t actualSize = 0);
		/// Used to retrieve column size and precision.
		/// Not all drivers cooperate with this inquiry under all circumstances
		/// This function runs for query and stored procedure parameters (in and 
		/// out-bound). Some drivers, however, do not care about knowing this 
		/// information to start with. For that reason, after all the attempts 
		/// to discover the required values are unsuccesfully exhausted, the values 
		/// are both set to zero and no exception is thrown.
		/// However, if the colSize is succesfully retrieved and it is greater than
		/// session-wide maximum allowed field size, LengthExceededException is thrown.

	void setParamSetSize(std::size_t length);
		/// Sets the parameter set size. Used for column-wise binding.

	void getColumnOrParameterSize(std::size_t pos, SQLINTEGER& size);
		/// Fills the column or parameter size into the 'size' argument.
		/// Does nothing if neither can be obtained from the driver, so
		/// size should be set to some default value prior to calling this 
		/// function in order to avoid undefined size value.

	void freeMemory();
		/// Frees all dynamically allocated memory resources.

	template<typename T>
	void getMinValueSize(T& val, SQLINTEGER& size)
		/// Some ODBC drivers return DB-wide maximum allowed size for variable size columns,
		/// rather than the allowed size for the actual column. In such cases, the length is 
		/// automatically resized to the maximum field size allowed by the session.
		/// This function, in order to prevent unnecessary memory allocation, does further 
		/// optimization, looking for the maximum length within supplied data container and
		/// uses the smaller of maximum found and maximum predefined data length.
	{
		typedef typename T::value_type ContainedValType;
		typedef typename ContainedValType::value_type BaseValType;
		std::size_t typeSize = sizeof(BaseValType);
		std::size_t maxSize = 0;
		typename T::const_iterator it = val.begin();
		typename T::const_iterator end = val.end();
		for (; it != end; ++it)
		{
			std::size_t sz = it->size() * typeSize;
			if (sz > _maxFieldSize)
				throw LengthExceededException();

			if (sz == _maxFieldSize)
			{
				maxSize = 0;
				break;
			}

			if (sz < _maxFieldSize && sz > maxSize)
				maxSize = sz;
		}
		if (maxSize) size = static_cast<SQLINTEGER>(maxSize);
	}

	const StatementHandle& _rStmt;

	LengthPtrVec     _lengthIndicator;
	LengthVecVec     _vecLengthIndicator;

	ParamMap         _inParams;
	ParamMap         _outParams;
	ParameterBinding _paramBinding;
	
	DateMap          _dates;
	TimeMap          _times;
	TimestampMap     _timestamps;
	StringMap        _strings;
	UTF16StringMap   _utf16Strings;

	DateVecVec       _dateVecVec;
	TimeVecVec       _timeVecVec;
	DateTimeVecVec   _dateTimeVecVec;
	CharPtrVec       _charPtrs;
	UTF16CharPtrVec  _utf16CharPtrs;
	BoolPtrVec       _boolPtrs;
	const TypeInfo*  _pTypeInfo;
	SQLINTEGER       _paramSetSize;
	std::size_t      _maxFieldSize;
	AnyVecVec        _containers;
};


//
// inlines
//
inline void Binder::bind(std::size_t pos, const Poco::Int8& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::Int8>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::Int8>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::Int8>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::UInt8& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_UTINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::UInt8>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_UTINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::UInt8>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_UTINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::UInt8>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_UTINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::Int16& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_SSHORT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::Int16>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_SSHORT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::Int16>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SSHORT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::Int16>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SSHORT, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::UInt16& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_USHORT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::UInt16>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_USHORT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::UInt16>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_USHORT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::UInt16>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_USHORT, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::Int32& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::Int32>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::Int32>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::Int32>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::UInt32& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_ULONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::UInt32>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_ULONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::UInt32>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_ULONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::UInt32>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_ULONG, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::Int64& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_SBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::Int64>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_SBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::Int64>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::Int64>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const Poco::UInt64& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_UBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Poco::UInt64>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_UBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Poco::UInt64>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_UBIGINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Poco::UInt64>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_UBIGINT, dir);
}


#ifndef POCO_LONG_IS_64_BIT
inline void Binder::bind(std::size_t pos, const long& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const unsigned long& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<long>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<long>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SLONG, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<long>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_SLONG, dir);
}
#endif


inline void Binder::bind(std::size_t pos, const float& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_FLOAT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<float>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_FLOAT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<float>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_FLOAT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<float>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_FLOAT, dir);
}


inline void Binder::bind(std::size_t pos, const double& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_DOUBLE, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<double>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_DOUBLE, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<double>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_DOUBLE, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<double>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_DOUBLE, dir);
}


inline void Binder::bind(std::size_t pos, const bool& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_BIT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<bool>& val, Direction dir)
{
	bindImplContainerBool(pos, val, SQL_C_BIT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<bool>& val, Direction dir)
{
	bindImplContainerBool(pos, val, SQL_C_BIT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<bool>& val, Direction dir)
{
	bindImplContainerBool(pos, val, SQL_C_BIT, dir);
}


inline void Binder::bind(std::size_t pos, const char& val, Direction dir)
{
	bindImpl(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<char>& val, Direction dir)
{
	bindImplVec(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<char>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<char>& val, Direction dir)
{
	bindImplContainer(pos, val, SQL_C_STINYINT, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<std::string>& val, Direction dir)
{
	bindImplContainerString(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<std::string>& val, Direction dir)
{
	bindImplContainerString(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<std::string>& val, Direction dir)
{
	bindImplContainerString(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<UTF16String>& val, Direction dir)
{
	bindImplContainerUTF16String(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<UTF16String>& val, Direction dir)
{
	bindImplContainerUTF16String(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<UTF16String>& val, Direction dir)
{
	bindImplContainerUTF16String(pos, val, dir);
}

inline void Binder::bind(std::size_t pos, const BLOB& val, Direction dir)
{
	bindImplLOB<BLOB>(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const CLOB& val, Direction dir)
{
	bindImplLOB<CLOB>(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<BLOB>& val, Direction dir)
{
	bindImplContainerLOB(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<BLOB>& val, Direction dir)
{
	bindImplContainerLOB(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<BLOB>& val, Direction dir)
{
	bindImplContainerLOB(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<CLOB>& val, Direction dir)
{
	bindImplContainerLOB(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<CLOB>& val, Direction dir)
{
	bindImplContainerLOB(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<CLOB>& val, Direction dir)
{
	bindImplContainerLOB(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Date>& val, Direction dir)
{
	bindImplContainerDate(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Date>& val, Direction dir)
{
	bindImplContainerDate(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Date>& val, Direction dir)
{
	bindImplContainerDate(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<Time>& val, Direction dir)
{
	bindImplContainerTime(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<Time>& val, Direction dir)
{
	bindImplContainerTime(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<Time>& val, Direction dir)
{
	bindImplContainerTime(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<DateTime>& val, Direction dir)
{
	bindImplContainerDateTime(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<DateTime>& val, Direction dir)
{
	bindImplContainerDateTime(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<DateTime>& val, Direction dir)
{
	bindImplContainerDateTime(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::vector<NullData>& val, Direction dir)
{
	bindImplNullContainer(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::deque<NullData>& val, Direction dir)
{
	bindImplNullContainer(pos, val, dir);
}


inline void Binder::bind(std::size_t pos, const std::list<NullData>& val, Direction dir)
{
	bindImplNullContainer(pos, val, dir);
}


inline void Binder::setDataBinding(Binder::ParameterBinding binding)
{
	_paramBinding = binding;
}


inline Binder::ParameterBinding Binder::getDataBinding() const
{
	return _paramBinding;
}


} } } // namespace Poco::Data::ODBC


#endif // Data_ODBC_Binder_INCLUDED
