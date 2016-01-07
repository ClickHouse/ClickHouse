//
// RowFormatter.h
//
// $Id: //poco/Main/Data/include/Poco/Data/RowFormatter.h#1 $
//
// Library: Data
// Package: DataCore
// Module:  RowFormatter
//
// Definition of the RowFormatter class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_RowFormatter_INCLUDED
#define Data_RowFormatter_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/SharedPtr.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Dynamic/Var.h"
#include <sstream>
#include <vector>


namespace Poco {
namespace Data {


class Data_API RowFormatter
	/// Row formatter is an abstract class providing definition for row formatting functionality.
	/// For custom formatting strategies, inherit from this class and override formatNames()
	/// and formatValues() member functions.
	///
	/// Row formatter can be either passed to the RecordSet at construction time,
	/// like in the following example:
	///
	/// RecordSet rs(session. "SELECT * FROM Table", new MyRowFormater);
	///
	/// or it can be supplied to the statement as in the following example:
	///
	/// MyRowFormatter rf
	/// session << "SELECT * FROM Table", format(rf);
	///
	/// If no formatter is externally supplied to the statement, the SimpleRowFormatter is used.
	/// Statement always has the ownership of the row formatter and shares
	/// it with rows through RecordSet.
	///
	/// To accomodate for various formatting needs, a formatter can operate in two modes:
	/// 
	///	  - progressive: formatted individual row strings are gemerated and returned from each 
	///     call to formatValues;
	///     std::string& formatNames(const NameVecPtr, std::string&) and
	///     std::string& formatValues(const ValueVec&, std::string&) member calls should be
	///     used in this case; this is the default mode
	///
	///   - bulk: formatted resulting string is accumulated internally and obtained at
	///     the end of iteration via toString() member function;
	///     void formatNames(const NameVecPtr) and
	///     void formatValues(const ValueVec&) member calls should be used in this case
	///
	/// When formatter is used in conjunction with Row/RecordSet, the formatting members corresponding
	/// to the formater mode are expected to be implemented. If a call is propagated to this parent
	/// class, the functions do nothing or silently return empty string respectively.
	///
{
public:
	typedef SharedPtr<RowFormatter>              Ptr;
	typedef std::vector<std::string>             NameVec;
	typedef SharedPtr<std::vector<std::string> > NameVecPtr;
	typedef std::vector<Poco::Dynamic::Var>      ValueVec;

	static const int INVALID_ROW_COUNT = -1;

	enum Mode
	{
		FORMAT_PROGRESSIVE,
		FORMAT_BULK
	};

	RowFormatter(const std::string& prefix = "",
		const std::string& postfix = "",
		Mode mode = FORMAT_PROGRESSIVE);
		/// Creates the RowFormatter and sets the prefix and postfix to specified values.

	virtual ~RowFormatter();
		/// Destroys the RowFormatter.

	virtual std::string& formatNames(const NameVecPtr pNames, std::string& formattedNames);
		/// Should be implemented to format the row fields names and return the formatted string.
		/// The default implementation clears the names string and returns it.

	virtual void formatNames(const NameVecPtr pNames);
		/// Should be implemented to format the row fields names.
		/// The default implementation does nothing.

	virtual std::string& formatValues(const ValueVec& vals, std::string& formattedValues);
		/// Should be implemented to format the row fields values and return the formatted string.
		/// The default implementation clears the values string and returns it.

	virtual void formatValues(const ValueVec& vals);
		/// Should be implemented to format the row fields values.
		/// The default implementation does nothing.

	virtual const std::string& toString();
		/// Throws NotImplementedException. Formatters operating in bulk mode should 
		/// implement this member function to return valid pointer to the formatted result.

	virtual int rowCount() const;
		/// Returns INVALID_ROW_COUNT. Must be implemented by inheriting classes
		/// which maintain count of processed rows.

	int getTotalRowCount() const;
		/// Returns zero. Must be implemented by inheriting classes.
		/// Typically, total row count shall be set up front through
		/// setTotalRowCount() call.

	void setTotalRowCount(int count);
		/// Sets total row count.

	virtual const std::string& prefix() const;
		/// Returns prefix string;

	virtual const std::string& postfix() const;
		/// Returns postfix string;

	void reset();
		/// Resets the formatter by setting prefix and postfix
		/// to empty strings and row count to INVALID_ROW_COUNT.

	Mode getMode() const;
		/// Returns the formater mode.

	void setMode(Mode mode);
		/// Sets the fromatter mode.

protected:

	void setPrefix(const std::string& prefix);
		/// Sets the prefix for the formatter.

	void setPostfix(const std::string& postfix);
		/// Sets the postfix for the formatter

private:

	mutable std::string _prefix;
	mutable std::string _postfix;
	Mode                _mode;
	int                 _totalRowCount;
};


///
/// inlines
///
inline int RowFormatter::rowCount() const
{
	return INVALID_ROW_COUNT;
}


inline int RowFormatter::getTotalRowCount() const
{
	return _totalRowCount;
}


inline void RowFormatter::setTotalRowCount(int count)
{
	_totalRowCount = count;
}


inline void RowFormatter::setPrefix(const std::string& prefix)
{
	_prefix = prefix;
}


inline void RowFormatter::setPostfix(const std::string& postfix)
{
	_postfix = postfix;
}


inline const std::string& RowFormatter::prefix() const
{
	return _prefix;
}


inline const std::string& RowFormatter::postfix() const
{
	return _postfix;
}


inline RowFormatter::Mode RowFormatter::getMode() const
{
	return _mode;
}


inline void RowFormatter::setMode(Mode mode)
{
	_mode = mode;
}


namespace Keywords {


template <typename T>
inline RowFormatter::Ptr format(const T& formatter)
	/// Utility function used to pass formatter to the statement.
{
	return new T(formatter);
}


} // namespace Keywords


} } // namespace Poco::Data


#endif // Data_RowFormatter_INCLUDED
