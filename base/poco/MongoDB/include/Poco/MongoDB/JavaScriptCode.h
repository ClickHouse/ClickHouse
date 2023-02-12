//
// JavaScriptCode.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  JavaScriptCode
//
// Definition of the JavaScriptCode class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_JavaScriptCode_INCLUDED
#define MongoDB_JavaScriptCode_INCLUDED


#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/BSONReader.h"
#include "Poco/MongoDB/BSONWriter.h"
#include "Poco/MongoDB/Element.h"
#include "Poco/SharedPtr.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API JavaScriptCode
	/// Represents JavaScript type in BSON.
{
public:
	typedef SharedPtr<JavaScriptCode> Ptr;

	JavaScriptCode();
		/// Creates an empty JavaScriptCode object.

	virtual ~JavaScriptCode();
		/// Destroys the JavaScriptCode.

	void setCode(const std::string& code);
		/// Sets the JavaScript code.

	std::string getCode() const;
		/// Returns the JavaScript code.

private:
	std::string _code;
};


//
// inlines
//
inline void JavaScriptCode::setCode(const std::string& code)
{
	_code = code;
}


inline std::string JavaScriptCode::getCode() const
{
	return _code;
}


// BSON JavaScript code
// spec: string
template<>
struct ElementTraits<JavaScriptCode::Ptr>
{
	enum { TypeId = 0x0D };

	static std::string toString(const JavaScriptCode::Ptr& value, int indent = 0)
	{
		return value.isNull() ? "" : value->getCode();
	}
};


template<>
inline void BSONReader::read<JavaScriptCode::Ptr>(JavaScriptCode::Ptr& to)
{
	std::string code;
	BSONReader(_reader).read(code);
	to = new JavaScriptCode();
	to->setCode(code);
}


template<>
inline void BSONWriter::write<JavaScriptCode::Ptr>(JavaScriptCode::Ptr& from)
{
	std::string code = from->getCode();
	BSONWriter(_writer).write(code);
}


} } // namespace Poco::MongoDB


#endif // MongoDB_JavaScriptCode_INCLUDED
