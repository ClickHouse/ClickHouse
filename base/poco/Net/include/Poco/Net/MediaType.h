//
// MediaType.h
//
// Library: Net
// Package: Messages
// Module:  MediaType
//
// Definition of the MediaType class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_MediaType_INCLUDED
#define Net_MediaType_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/NameValueCollection.h"


namespace Poco {
namespace Net {


class Net_API MediaType
	/// This class represents a MIME media type, consisting of
	/// a top-level type, a subtype and an optional set of
	/// parameters.
	///
	/// The implementation conforms with RFC 2045 and RFC 2046.
{
public:
	MediaType(const std::string& mediaType);
		/// Creates the MediaType from the given string, which
		/// must have the format <type>/<subtype>{;<parameter>=<value>}.
		
	MediaType(const std::string& type, const std::string& subType);
		/// Creates the MediaType, using the given type and subtype.

	MediaType(const MediaType& mediaType);
		/// Creates a MediaType from another one.

	~MediaType();
		/// Destroys the MediaType.

	MediaType& operator = (const MediaType& mediaType);
		/// Assigns another media type.
		
	MediaType& operator = (const std::string& mediaType);
		/// Assigns another media type.
		
	void swap(MediaType& mediaType);
		/// Swaps the MediaType with another one.
		
	void setType(const std::string& type);
		/// Sets the top-level type.
		
	const std::string& getType() const;
		/// Returns the top-level type.
		
	void setSubType(const std::string& subType);
		/// Sets the sub type.
		
	const std::string& getSubType() const;
		/// Returns the sub type.
		
	void setParameter(const std::string& name, const std::string& value);
		/// Sets the parameter with the given name.
		
	const std::string& getParameter(const std::string& name) const;
		/// Returns the parameter with the given name.
		///
		/// Throws a NotFoundException if the parameter does not exist.
		
	bool hasParameter(const std::string& name) const;
		/// Returns true iff a parameter with the given name exists.
		
	void removeParameter(const std::string& name);
		/// Removes the parameter with the given name.	
		
	const NameValueCollection& parameters() const;
		/// Returns the parameters.
		
	std::string toString() const;
		/// Returns the string representation of the media type
		/// which is <type>/<subtype>{;<parameter>=<value>}
		
	bool matches(const MediaType& mediaType) const;
		/// Returns true iff the type and subtype match
		/// the type and subtype of the given media type.
		/// Matching is case insensitive.
		
	bool matches(const std::string& type, const std::string& subType) const;
		/// Returns true iff the type and subtype match
		/// the given type and subtype.
		/// Matching is case insensitive.

	bool matches(const std::string& type) const;
		/// Returns true iff the type matches the given type.
		/// Matching is case insensitive.

	bool matchesRange(const MediaType& mediaType) const;
		/// Returns true if the type and subtype match
		/// the type and subtype of the given media type.
		/// If the MIME type is a range of types it matches
		/// any media type within the range (e.g. "image/*" matches
		/// any image media type, "*/*" matches anything).
		/// Matching is case insensitive.

	bool matchesRange(const std::string& type, const std::string& subType) const;
		/// Returns true if the type and subtype match
		/// the given type and subtype.
		/// If the MIME type is a range of types it matches
		/// any media type within the range (e.g. "image/*" matches
		/// any image media type, "*/*" matches anything).
		/// Matching is case insensitive.

	bool matchesRange(const std::string& type) const;
		/// Returns true if the type matches the given type or
		/// the type is a range of types denoted by "*".
		/// Matching is case insensitive.

protected:
	void parse(const std::string& mediaType);
		
private:
	MediaType();
	
	std::string         _type;
	std::string         _subType;
	NameValueCollection _parameters;
};


//
// inlines
//
inline const std::string& MediaType::getType() const
{
	return _type;
}


inline const std::string& MediaType::getSubType() const
{
	return _subType;
}


inline const NameValueCollection& MediaType::parameters() const
{
	return _parameters;
}


inline void swap(MediaType& m1, MediaType& m2)
{
	m1.swap(m2);
}


} } // namespace Poco::Net


#endif // Net_MediaType_INCLUDED
