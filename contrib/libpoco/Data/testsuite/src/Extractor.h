//
// Extractor.h
//
// $Id: //poco/Main/Data/testsuite/src/Extractor.h#5 $
//
// Definition of the Extractor class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Test_Extractor_INCLUDED
#define Data_Test_Extractor_INCLUDED


#include "Poco/Data/AbstractExtractor.h"


namespace Poco {
namespace Data {
namespace Test {


class Extractor: public Poco::Data::AbstractExtractor
	/// A no-op implementation of AbstractExtractor for testing.
{
public:
	Extractor();
		/// Creates the Extractor.

	~Extractor();
		/// Destroys the Extractor.

	bool extract(std::size_t pos, Poco::Int8& val);
		/// Extracts an Int8.

	bool extract(std::size_t pos, Poco::UInt8& val);
		/// Extracts an UInt8.

	bool extract(std::size_t pos, Poco::Int16& val);
		/// Extracts an Int16.

	bool extract(std::size_t pos, Poco::UInt16& val);
		/// Extracts an UInt16.

	bool extract(std::size_t pos, Poco::Int32& val);
		/// Extracts an Int32.

	bool extract(std::size_t pos, Poco::UInt32& val);
		/// Extracts an UInt32.

	bool extract(std::size_t pos, Poco::Int64& val);
		/// Extracts an Int64.

	bool extract(std::size_t pos, Poco::UInt64& val);
		/// Extracts an UInt64.

	bool extract(std::size_t pos, Poco::Any& val);
		/// Extracts an Any.

	bool extract(std::size_t pos, Poco::Dynamic::Var& val);
		/// Extracts a Var.

#ifndef POCO_LONG_IS_64_BIT
	bool extract(std::size_t pos, long& val);
		/// Extracts a long.

	bool extract(std::size_t pos, unsigned long& val);
		/// Extracts an unsigned long.
#endif

	bool extract(std::size_t pos, bool& val);
		/// Extracts a boolean.

	bool extract(std::size_t pos, float& val);
		/// Extracts a float.

	bool extract(std::size_t pos, double& val);
		/// Extracts a double.

	bool extract(std::size_t pos, char& val);
		/// Extracts a single character.

	bool extract(std::size_t pos, std::string& val);
		/// Extracts a string.

	bool extract(std::size_t pos, Poco::UTF16String& val);
		/// Extracts a UTF16String.

	bool extract(std::size_t pos, Poco::Data::BLOB& val);
		/// Extracts a BLOB.

	bool extract(std::size_t pos, Poco::Data::CLOB& val);
		/// Extracts a CLOB.

	bool extract(std::size_t pos, Date& val);
		/// Extracts a Date.

	bool extract(std::size_t pos, Time& val);
		/// Extracts a Time.

	bool extract(std::size_t pos, Poco::DateTime& val);
		/// Extracts a DateTime.

	bool isNull(std::size_t col, std::size_t row = -1);
		/// Returns true if the current row value at pos column is null.

	void reset();
};


inline void Extractor::reset()
{
}


inline bool Extractor::isNull(std::size_t col, std::size_t row)
{
	return false;
}


} } } // namespace Poco::Data::Test


#endif // Data_Test_Extractor_INCLUDED
