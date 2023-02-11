//
// RegularExpression.h
//
// Library: MongoDB
// Package: MongoDB
// Module:  RegularExpression
//
// Definition of the RegularExpression class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef MongoDB_RegularExpression_INCLUDED
#define MongoDB_RegularExpression_INCLUDED


#include "Poco/RegularExpression.h"
#include "Poco/MongoDB/MongoDB.h"
#include "Poco/MongoDB/Element.h"


namespace Poco {
namespace MongoDB {


class MongoDB_API RegularExpression
	/// Represents a regular expression in BSON format.
{
public:
	typedef SharedPtr<RegularExpression> Ptr;

	RegularExpression();
		/// Creates an empty RegularExpression.

	RegularExpression(const std::string& pattern, const std::string& options);
		/// Creates a RegularExpression using the given pattern and options.

	virtual ~RegularExpression();
		/// Destroys the RegularExpression.

	SharedPtr<Poco::RegularExpression> createRE() const;
		/// Tries to create a Poco::RegularExpression from the MongoDB regular expression.

	std::string getOptions() const;
		/// Returns the options string.

	void setOptions(const std::string& options);
		/// Sets the options string.

	std::string getPattern() const;
		/// Returns the pattern.

	void setPattern(const std::string& pattern);
		/// Sets the pattern.

private:
	std::string _pattern;
	std::string _options;
};


///
/// inlines
///
inline std::string RegularExpression::getPattern() const
{
	return _pattern;
}


inline void RegularExpression::setPattern(const std::string& pattern)
{
	_pattern = pattern;
}


inline std::string RegularExpression::getOptions() const
{
	return _options;
}


inline void RegularExpression::setOptions(const std::string& options)
{
	_options = options;
}


// BSON Regex
// spec: cstring cstring
template<>
struct ElementTraits<RegularExpression::Ptr>
{
	enum { TypeId = 0x0B };

	static std::string toString(const RegularExpression::Ptr& value, int indent = 0)
	{
		//TODO
		return "RE: not implemented yet";
	}
};


template<>
inline void BSONReader::read<RegularExpression::Ptr>(RegularExpression::Ptr& to)
{
	std::string pattern = readCString();
	std::string options = readCString();

	to = new RegularExpression(pattern, options);
}


template<>
inline void BSONWriter::write<RegularExpression::Ptr>(RegularExpression::Ptr& from)
{
	writeCString(from->getPattern());
	writeCString(from->getOptions());
}


} } // namespace Poco::MongoDB


#endif // MongoDB_RegularExpression_INCLUDED
