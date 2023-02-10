//
// AbstractExtraction.h
//
// Library: Data
// Package: DataCore
// Module:  AbstractExtraction
//
// Definition of the AbstractExtraction class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_AbstractExtraction_INCLUDED
#define Data_AbstractExtraction_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/AbstractExtractor.h"
#include "Poco/Data/AbstractPreparation.h"
#include "Poco/Data/Limit.h"
#include "Poco/RefCountedObject.h"
#include "Poco/UTFString.h"
#include "Poco/AutoPtr.h"
#include <vector>
#include <deque>
#include <list>
#include <cstddef>


namespace Poco {
namespace Data {


class AbstractPreparator;


class Data_API AbstractExtraction
	/// AbstractExtraction is the interface class that connects output positions to concrete values
	/// retrieved via an AbstractExtractor.
{
public:
	typedef SharedPtr<AbstractExtraction> Ptr;
	typedef SharedPtr<AbstractExtractor>  ExtractorPtr;
	typedef SharedPtr<AbstractPreparator> PreparatorPtr;

	AbstractExtraction(Poco::UInt32 limit = Limit::LIMIT_UNLIMITED,
		Poco::UInt32 position = 0, bool bulk = false);
		/// Creates the AbstractExtraction. A limit value equal to EXTRACT_UNLIMITED (0xffffffffu) 
		/// means that we extract as much data as possible during one execute.
		/// Otherwise the limit value is used to partition data extracting to a limited amount of rows.

	virtual ~AbstractExtraction();
		/// Destroys the AbstractExtraction.

	void setExtractor(ExtractorPtr pExtractor);
		/// Sets the class used for extracting the data. Does not take ownership of the pointer.

	ExtractorPtr getExtractor() const;
		/// Retrieves the extractor object

	Poco::UInt32 position() const;
		/// Returns the extraction position.

	virtual std::size_t numOfColumnsHandled() const = 0;
		/// Returns the number of columns that the extraction handles.
		///
		/// The trivial case will be one single column but when
		/// complex types are used this value can be larger than one.

	virtual std::size_t numOfRowsHandled() const = 0;
		/// Returns the number of rows that the extraction handles.
		///
		/// The trivial case will be one single row but 
		/// for collection data types (ie vector) it can be larger.

	virtual std::size_t numOfRowsAllowed() const = 0;
		/// Returns the upper limit on number of rows that the extraction will handle.

	virtual std::size_t extract(std::size_t pos) = 0;
		/// Extracts a value from the param, starting at the given column position.
		/// Returns the number of rows extracted.

	virtual void reset();
		/// Resets the extractor so that it can be re-used.
		/// Does nothing in this implementation.
		/// Implementations should override it for different behavior.

	virtual bool canExtract() const;
		/// Returns true. Implementations should override it for different behavior.

	virtual AbstractPreparation::Ptr createPreparation(PreparatorPtr& pPrep, std::size_t pos) = 0;
		/// Creates and returns shared pointer to Preparation object for the extracting object.

	void setLimit(Poco::UInt32 limit);
		/// Sets the limit.

	Poco::UInt32 getLimit() const;
		/// Gets the limit.

	virtual bool isNull(std::size_t row) const;
		/// In implementations, this function returns true if value at row is null, 
		/// false otherwise. 
		/// Normal behavior is to replace nulls with default values.
		/// However, extraction implementations may remember the underlying database
		/// null values and be able to later provide information about them.
		/// Here, this function throws NotImplementedException.

	bool isBulk() const;
		/// Returns true if this is bulk extraction.

	void setEmptyStringIsNull(bool emptyStringIsNull);
		/// Sets the empty string handling flag.

	bool getEmptyStringIsNull() const;
		/// Returns the empty string handling flag.

	void setForceEmptyString(bool forceEmptyString);
		/// Sets the force empty string flag.

	bool getForceEmptyString() const;
		/// Returns the force empty string flag.

	template <typename T>
	bool isValueNull(const T& str, bool deflt)
		/// Utility function to determine the nullness of the value.
		/// This generic version always returns default value
		/// (i.e. does nothing). The std::string overload does
		/// the actual work.
		///
	{
		return deflt;
	}

	bool isValueNull(const std::string& str, bool deflt);
		/// Overload for const reference to std::string.
		///
		/// Returns true when folowing conditions are met:
		///
		/// - string is empty 
		/// - getEmptyStringIsNull() returns true

	bool isValueNull(const Poco::UTF16String& str, bool deflt);
		/// Overload for const reference to UTF16String.
		///
		/// Returns true when folowing conditions are met:
		///
		/// - string is empty 
		/// - getEmptyStringIsNull() returns true

private:
	template <typename S>
	bool isStringNull(const S& str, bool deflt)
	{
		if (getForceEmptyString()) return false;

		if (getEmptyStringIsNull() && str.empty())
			return true;

		return deflt;
	}

	ExtractorPtr _pExtractor;
	Poco::UInt32 _limit;
	Poco::UInt32 _position;
	bool         _bulk;
	bool         _emptyStringIsNull;
	bool         _forceEmptyString;
};


typedef std::vector<AbstractExtraction::Ptr> AbstractExtractionVec;
typedef std::vector<AbstractExtractionVec>   AbstractExtractionVecVec;
typedef std::deque<AbstractExtraction::Ptr>  AbstractExtractionDeq;
typedef std::vector<AbstractExtractionDeq>   AbstractExtractionDeqVec;
typedef std::list<AbstractExtraction::Ptr>   AbstractExtractionLst;
typedef std::vector<AbstractExtractionLst>   AbstractExtractionLstVec;


//
// inlines
//
inline void AbstractExtraction::setExtractor(ExtractorPtr pExtractor)
{
	_pExtractor = pExtractor;
}


inline AbstractExtraction::ExtractorPtr AbstractExtraction::getExtractor() const
{
	return _pExtractor;
}


inline void AbstractExtraction::setLimit(Poco::UInt32 limit)
{
	_limit = limit;
}


inline Poco::UInt32 AbstractExtraction::getLimit() const
{
	return _limit;
}


inline bool AbstractExtraction::isNull(std::size_t row) const
{
	throw NotImplementedException("Check for null values not implemented.");
}


inline Poco::UInt32 AbstractExtraction::position() const
{
	return _position;
}


inline bool AbstractExtraction::isBulk() const
{
	return _bulk;
}


inline void AbstractExtraction::reset()
{
}


inline bool AbstractExtraction::canExtract() const
{
	return true;
}


inline void AbstractExtraction::setEmptyStringIsNull(bool emptyStringIsNull)
{
	_emptyStringIsNull = emptyStringIsNull;
}


inline bool AbstractExtraction::getEmptyStringIsNull() const
{
	return _emptyStringIsNull;
}


inline void AbstractExtraction::setForceEmptyString(bool forceEmptyString)
{
	_forceEmptyString = forceEmptyString;
}


inline bool AbstractExtraction::getForceEmptyString() const
{
	return _forceEmptyString;
}


inline bool AbstractExtraction::isValueNull(const std::string& str, bool deflt)
{
	return isStringNull(str, deflt);
}


inline bool AbstractExtraction::isValueNull(const Poco::UTF16String& str, bool deflt)
{
	return isStringNull(str, deflt);
}


} } // namespace Poco::Data


#endif // Data_AbstractExtraction_INCLUDED
