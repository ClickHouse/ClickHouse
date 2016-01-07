//
// Extractor.h
//
// $Id: //poco/1.4/Data/MySQL/include/Poco/Data/MySQL/Extractor.h#1 $
//
// Library: Data
// Package: MySQL
// Module:  Extractor
//
// Definition of the Extractor class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MySQL_Extractor_INCLUDED
#define Data_MySQL_Extractor_INCLUDED


#include "Poco/Data/MySQL/MySQL.h"
#include "Poco/Data/MySQL/StatementExecutor.h"
#include "Poco/Data/MySQL/ResultMetadata.h"
#include "Poco/Data/AbstractExtractor.h"
#include "Poco/Data/LOB.h"


namespace Poco {

namespace Dynamic {
	class Var;
}

namespace Data {
namespace MySQL {


class MySQL_API Extractor: public Poco::Data::AbstractExtractor
	/// Extracts and converts data values from the result row returned by MySQL.
	/// If NULL is received, the incoming val value is not changed and false is returned
{
public:
	typedef SharedPtr<Extractor> Ptr;

	Extractor(StatementExecutor& st, ResultMetadata& md);
		/// Creates the Extractor.

	virtual ~Extractor();
		/// Destroys the Extractor.

	virtual bool extract(std::size_t pos, Poco::Int8& val);
		/// Extracts an Int8.
		
	virtual bool extract(std::size_t pos, Poco::UInt8& val);
		/// Extracts an UInt8.
		
	virtual bool extract(std::size_t pos, Poco::Int16& val);
		/// Extracts an Int16.
		
	virtual bool extract(std::size_t pos, Poco::UInt16& val);
		/// Extracts an UInt16.
		
	virtual bool extract(std::size_t pos, Poco::Int32& val);
		/// Extracts an Int32.
		
	virtual bool extract(std::size_t pos, Poco::UInt32& val);
		/// Extracts an UInt32.
		
	virtual bool extract(std::size_t pos, Poco::Int64& val);
		/// Extracts an Int64.
		
	virtual bool extract(std::size_t pos, Poco::UInt64& val);
		/// Extracts an UInt64.
		
#ifndef POCO_LONG_IS_64_BIT
	virtual bool extract(std::size_t pos, long& val);
		/// Extracts a long. Returns false if null was received.

	virtual bool extract(std::size_t pos, unsigned long& val);
		/// Extracts an unsigned long. Returns false if null was received.
#endif

	virtual bool extract(std::size_t pos, bool& val);
		/// Extracts a boolean.
		
	virtual bool extract(std::size_t pos, float& val);
		/// Extracts a float.
		
	virtual bool extract(std::size_t pos, double& val);
		/// Extracts a double.

	virtual bool extract(std::size_t pos, char& val);
		/// Extracts a single character.

	virtual bool extract(std::size_t pos, std::string& val);
		/// Extracts a string.

	virtual bool extract(std::size_t pos, Poco::Data::BLOB& val);
		/// Extracts a BLOB.

	virtual bool extract(std::size_t pos, Poco::Data::CLOB& val);
		/// Extracts a CLOB.

	virtual bool extract(std::size_t pos, DateTime& val);
		/// Extracts a DateTime. Returns false if null was received.

	virtual bool extract(std::size_t pos, Date& val);
		/// Extracts a Date. Returns false if null was received.

	virtual bool extract(std::size_t pos, Time& val);
		/// Extracts a Time. Returns false if null was received.

	virtual bool extract(std::size_t pos, Any& val);
		/// Extracts an Any. Returns false if null was received.

	virtual bool extract(std::size_t pos, Dynamic::Var& val);
		/// Extracts a Dynamic::Var. Returns false if null was received.

	virtual bool isNull(std::size_t col, std::size_t row);
		/// Returns true if the value at [col,row] position is null.

	virtual void reset();
		/// Resets any information internally cached by the extractor.

	////////////
	// Not implemented extract functions
	////////////
	
	virtual bool extract(std::size_t pos, std::vector<Poco::Int8>& val);
		/// Extracts an Int8 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int8>& val);
		/// Extracts an Int8 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int8>& val);
		/// Extracts an Int8 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt8>& val);
		/// Extracts an UInt8 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt8>& val);
		/// Extracts an UInt8 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt8>& val);
		/// Extracts an UInt8 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int16>& val);
		/// Extracts an Int16 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int16>& val);
		/// Extracts an Int16 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int16>& val);
		/// Extracts an Int16 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt16>& val);
		/// Extracts an UInt16 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt16>& val);
		/// Extracts an UInt16 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt16>& val);
		/// Extracts an UInt16 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int32>& val);
		/// Extracts an Int32 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int32>& val);
		/// Extracts an Int32 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int32>& val);
		/// Extracts an Int32 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt32>& val);
		/// Extracts an UInt32 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt32>& val);
		/// Extracts an UInt32 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt32>& val);
		/// Extracts an UInt32 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::Int64>& val);
		/// Extracts an Int64 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::Int64>& val);
		/// Extracts an Int64 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::Int64>& val);
		/// Extracts an Int64 list.

	virtual bool extract(std::size_t pos, std::vector<Poco::UInt64>& val);
		/// Extracts an UInt64 vector.

	virtual bool extract(std::size_t pos, std::deque<Poco::UInt64>& val);
		/// Extracts an UInt64 deque.

	virtual bool extract(std::size_t pos, std::list<Poco::UInt64>& val);
		/// Extracts an UInt64 list.

#ifndef POCO_LONG_IS_64_BIT
	virtual bool extract(std::size_t pos, std::vector<long>& val);
		/// Extracts a long vector.

	virtual bool extract(std::size_t pos, std::deque<long>& val);
		/// Extracts a long deque.

	virtual bool extract(std::size_t pos, std::list<long>& val);
		/// Extracts a long list.
#endif

	virtual bool extract(std::size_t pos, std::vector<bool>& val);
		/// Extracts a boolean vector.

	virtual bool extract(std::size_t pos, std::deque<bool>& val);
		/// Extracts a boolean deque.

	virtual bool extract(std::size_t pos, std::list<bool>& val);
		/// Extracts a boolean list.

	virtual bool extract(std::size_t pos, std::vector<float>& val);
		/// Extracts a float vector.

	virtual bool extract(std::size_t pos, std::deque<float>& val);
		/// Extracts a float deque.

	virtual bool extract(std::size_t pos, std::list<float>& val);
		/// Extracts a float list.

	virtual bool extract(std::size_t pos, std::vector<double>& val);
		/// Extracts a double vector.

	virtual bool extract(std::size_t pos, std::deque<double>& val);
		/// Extracts a double deque.

	virtual bool extract(std::size_t pos, std::list<double>& val);
		/// Extracts a double list.

	virtual bool extract(std::size_t pos, std::vector<char>& val);
		/// Extracts a character vector.

	virtual bool extract(std::size_t pos, std::deque<char>& val);
		/// Extracts a character deque.

	virtual bool extract(std::size_t pos, std::list<char>& val);
		/// Extracts a character list.

	virtual bool extract(std::size_t pos, std::vector<std::string>& val);
		/// Extracts a string vector.

	virtual bool extract(std::size_t pos, std::deque<std::string>& val);
		/// Extracts a string deque.

	virtual bool extract(std::size_t pos, std::list<std::string>& val);
		/// Extracts a string list.

	virtual bool extract(std::size_t pos, std::vector<BLOB>& val);
		/// Extracts a BLOB vector.

	virtual bool extract(std::size_t pos, std::deque<BLOB>& val);
		/// Extracts a BLOB deque.

	virtual bool extract(std::size_t pos, std::list<BLOB>& val);
		/// Extracts a BLOB list.

	virtual bool extract(std::size_t pos, std::vector<CLOB>& val);
		/// Extracts a CLOB vector.

	virtual bool extract(std::size_t pos, std::deque<CLOB>& val);
		/// Extracts a CLOB deque.

	virtual bool extract(std::size_t pos, std::list<CLOB>& val);
		/// Extracts a CLOB list.

	virtual bool extract(std::size_t pos, std::vector<DateTime>& val);
		/// Extracts a DateTime vector.

	virtual bool extract(std::size_t pos, std::deque<DateTime>& val);
		/// Extracts a DateTime deque.

	virtual bool extract(std::size_t pos, std::list<DateTime>& val);
		/// Extracts a DateTime list.

	virtual bool extract(std::size_t pos, std::vector<Date>& val);
		/// Extracts a Date vector.

	virtual bool extract(std::size_t pos, std::deque<Date>& val);
		/// Extracts a Date deque.

	virtual bool extract(std::size_t pos, std::list<Date>& val);
		/// Extracts a Date list.

	virtual bool extract(std::size_t pos, std::vector<Time>& val);
		/// Extracts a Time vector.

	virtual bool extract(std::size_t pos, std::deque<Time>& val);
		/// Extracts a Time deque.

	virtual bool extract(std::size_t pos, std::list<Time>& val);
		/// Extracts a Time list.

	virtual bool extract(std::size_t pos, std::vector<Any>& val);
		/// Extracts an Any vector.

	virtual bool extract(std::size_t pos, std::deque<Any>& val);
		/// Extracts an Any deque.

	virtual bool extract(std::size_t pos, std::list<Any>& val);
		/// Extracts an Any list.

	virtual bool extract(std::size_t pos, std::vector<Dynamic::Var>& val);
		/// Extracts a Dynamic::Var vector.

	virtual bool extract(std::size_t pos, std::deque<Dynamic::Var>& val);
		/// Extracts a Dynamic::Var deque.

	virtual bool extract(std::size_t pos, std::list<Dynamic::Var>& val);
		/// Extracts a Dynamic::Var list.
	
private:

	bool realExtractFixed(std::size_t pos, enum_field_types type, void* buffer, bool isUnsigned = false);

	// Prevent VC8 warning "operator= could not be generated"
	Extractor& operator=(const Extractor&);

private:

	StatementExecutor& _stmt;
	ResultMetadata& _metadata;
};

} } } // namespace Poco::Data::MySQL


#endif // Data_MySQL_Extractor_INCLUDED
