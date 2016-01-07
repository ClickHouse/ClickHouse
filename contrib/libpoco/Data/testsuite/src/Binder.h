//
// Binder.h
//
// $Id: //poco/Main/Data/testsuite/src/Binder.h#4 $
//
// Definition of the Binder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Test_Binder_INCLUDED
#define Data_Test_Binder_INCLUDED


#include "Poco/Data/AbstractBinder.h"


namespace Poco {
namespace Data {
namespace Test {


class Binder: public Poco::Data::AbstractBinder
	/// A no-op implementation of AbstractBinder for testing.
{
public:
	Binder();
		/// Creates the Binder.

	~Binder();
		/// Destroys the Binder.

	void bind(std::size_t pos, const Poco::Int8 &val, Direction dir);
		/// Binds an Int8.

	void bind(std::size_t pos, const Poco::UInt8 &val, Direction dir);
		/// Binds an UInt8.

	void bind(std::size_t pos, const Poco::Int16 &val, Direction dir);
		/// Binds an Int16.

	void bind(std::size_t pos, const Poco::UInt16 &val, Direction dir);
		/// Binds an UInt16.

	void bind(std::size_t pos, const Poco::Int32 &val, Direction dir);
		/// Binds an Int32.

	void bind(std::size_t pos, const Poco::UInt32 &val, Direction dir);
		/// Binds an UInt32.

	void bind(std::size_t pos, const Poco::Int64 &val, Direction dir);
		/// Binds an Int64.

	void bind(std::size_t pos, const Poco::UInt64 &val, Direction dir);
		/// Binds an UInt64.

#ifndef POCO_LONG_IS_64_BIT
	void bind(std::size_t pos, const long& val, Direction dir);
		/// Binds a long.

	void bind(std::size_t pos, const unsigned long& val, Direction dir);
		/// Binds an unsigned long.
#endif

	void bind(std::size_t pos, const bool &val, Direction dir);
		/// Binds a boolean.

	void bind(std::size_t pos, const float &val, Direction dir);
		/// Binds a float.

	void bind(std::size_t pos, const double &val, Direction dir);
		/// Binds a double.

	void bind(std::size_t pos, const char &val, Direction dir);
		/// Binds a single character.

	void bind(std::size_t pos, const char* const &pVal, Direction dir);
		/// Binds a const char ptr.

	void bind(std::size_t pos, const std::string& val, Direction dir);
		/// Binds a string.

	void bind(std::size_t pos, const Poco::UTF16String& val, Direction dir);
		/// Binds a UTF16String.

	void bind(std::size_t pos, const BLOB& val, Direction dir);
		/// Binds a BLOB.

	void bind(std::size_t pos, const CLOB& val, Direction dir);
		/// Binds a CLOB.

	void bind(std::size_t pos, const Date& val, Direction dir);
		/// Binds a Date.

	void bind(std::size_t pos, const Time& val, Direction dir);
		/// Binds a Time.

	void bind(std::size_t pos, const DateTime& val, Direction dir);
		/// Binds a DateTime.

	void bind(std::size_t pos, const NullData& val, Direction dir);
		/// Binds a DateTime.

	void reset();
};


} } } // namespace Poco::Data::Test


#endif // Data_Test_Binder_INCLUDED
