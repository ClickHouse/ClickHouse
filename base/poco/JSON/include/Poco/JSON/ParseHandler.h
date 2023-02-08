//
// ParseHandler.h
//
// Library: JSON
// Package: JSON
// Module:  ParseHandler
//
// Definition of the ParseHandler class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_ParseHandler_INCLUDED
#define JSON_ParseHandler_INCLUDED


#include "Poco/JSON/Handler.h"
#include <stack>


namespace Poco {
namespace JSON {


class JSON_API ParseHandler: public Handler
	/// ParseHandler is the default handler for the JSON Parser.
	///
	/// This handler will construct an Object or Array based
	/// on the handlers called by the Parser.
{
public:
	ParseHandler(bool preserveObjectOrder = false);
		/// Creates the ParseHandler.
		///
		/// If preserveObjectOrder is true, the order of properties
		/// inside objects is preserved. Otherwise, items
		/// will be sorted by keys.

	virtual ~ParseHandler();
		/// Destroys the ParseHandler.

	virtual void reset();
		/// Resets the handler state.
		
	void startObject();
		/// Handles a '{'; a new object is started.

	void endObject();
		/// Handles a '}'; the object is closed.

	void startArray();
		/// Handles a '['; a new array is started.

	void endArray();
		/// Handles a ']'; the array is closed.

	void key(const std::string& k);
		/// A key is read

	Dynamic::Var asVar() const;
		/// Returns the result of the parser (an object or an array).

	virtual void value(int v);
		/// An integer value is read

	virtual void value(unsigned v);
		/// An unsigned value is read. This will only be triggered if the
		/// value cannot fit into a signed int.

#if defined(POCO_HAVE_INT64)
	virtual void value(Int64 v);
		/// A 64-bit integer value is read

	virtual void value(UInt64 v);
		/// An unsigned 64-bit integer value is read. This will only be
		/// triggered if the value cannot fit into a signed 64-bit integer.
#endif

	virtual void value(const std::string& s);
		/// A string value is read.

	virtual void value(double d);
		/// A double value is read.

	virtual void value(bool b);
		/// A boolean value is read.

	virtual void null();
		/// A null value is read.

private:
	void setValue(const Poco::Dynamic::Var& value);
	typedef std::stack<Dynamic::Var> Stack;

	Stack        _stack;
	std::string  _key;
	Dynamic::Var _result;
	bool         _preserveObjectOrder;
};


//
// inlines
//
inline Dynamic::Var ParseHandler::asVar() const
{
	return _result;
}


inline void ParseHandler::value(int v)
{
	setValue(v);
}


inline void ParseHandler::value(unsigned v)
{
	setValue(v);
}


#if defined(POCO_HAVE_INT64)
inline void ParseHandler::value(Int64 v)
{
	setValue(v);
}


inline void ParseHandler::value(UInt64 v)
{
	setValue(v);
}
#endif


inline void ParseHandler::value(const std::string& s)
{
	setValue(s);
}


inline void ParseHandler::value(double d)
{
	setValue(d);
}


inline void ParseHandler::value(bool b)
{
	setValue(b);
}


inline void ParseHandler::null()
{
	Poco::Dynamic::Var empty;
	setValue(empty);
}


} } // namespace Poco::JSON


#endif // JSON_ParseHandler_INCLUDED
