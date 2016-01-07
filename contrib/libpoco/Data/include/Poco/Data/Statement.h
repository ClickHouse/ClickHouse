//
// Statement.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Statement.h#18 $
//
// Library: Data
// Package: DataCore
// Module:  Statement
//
// Definition of the Statement class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Statement_INCLUDED
#define Data_Statement_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/StatementImpl.h"
#include "Poco/Data/Binding.h"
#include "Poco/Data/Range.h"
#include "Poco/Data/Bulk.h"
#include "Poco/Data/Row.h"
#include "Poco/Data/SimpleRowFormatter.h"
#include "Poco/SharedPtr.h"
#include "Poco/Mutex.h"
#include "Poco/ActiveMethod.h"
#include "Poco/ActiveResult.h"
#include "Poco/Format.h"
#include <algorithm>


namespace Poco {
namespace Data {


class AbstractBinding;
class AbstractExtraction;
class Session;
class Limit;


class Data_API Statement
	/// A Statement is used to execute SQL statements. 
	/// It does not contain code of its own.
	/// Its main purpose is to forward calls to the concrete StatementImpl stored inside.
	/// Statement execution can be synchronous or asynchronous.
	/// Synchronous ececution is achieved through execute() call, while asynchronous is
	/// achieved through executeAsync() method call.
	/// An asynchronously executing statement should not be copied during the execution. 
	///
	/// Note:
	///
	/// Once set as asynchronous through 'async' manipulator, statement remains
	/// asynchronous for all subsequent execution calls, both execute() and executeAsync().
	/// However, calling executAsync() on a synchronous statement shall execute 
	/// asynchronously but without altering the underlying statement's synchronous nature.
	///
	/// Once asynchronous, a statement can be reverted back to synchronous state in two ways:
	/// 
	///   1) By calling setAsync(false)
	///   2) By means of 'sync' or 'reset' manipulators
	///
	/// See individual functions documentation for more details.
	///
	/// Statement owns the RowFormatter, which can be provided externaly through setFormatter()
	/// member function.
	/// If no formatter is externally supplied to the statement, the SimpleRowFormatter is lazy
	/// created and used.
{
public:
	typedef void (*Manipulator)(Statement&);

	typedef ActiveResult<std::size_t>                      Result;
	typedef SharedPtr<Result>                              ResultPtr;
	typedef ActiveMethod<std::size_t, bool, StatementImpl> AsyncExecMethod;
	typedef SharedPtr<AsyncExecMethod>                     AsyncExecMethodPtr;

	static const int WAIT_FOREVER = -1;

	enum Storage
	{
		STORAGE_DEQUE   = StatementImpl::STORAGE_DEQUE_IMPL,
		STORAGE_VECTOR  = StatementImpl::STORAGE_VECTOR_IMPL,
		STORAGE_LIST    = StatementImpl::STORAGE_LIST_IMPL,
		STORAGE_UNKNOWN = StatementImpl::STORAGE_UNKNOWN_IMPL
	};

	Statement(StatementImpl::Ptr pImpl);
		/// Creates the Statement.

	explicit Statement(Session& session);
		/// Creates the Statement for the given Session.
		///
		/// The following:
		///
		///     Statement stmt(sess);
		///     stmt << "SELECT * FROM Table", ...
		///
		/// is equivalent to:
		/// 
		///     Statement stmt(sess << "SELECT * FROM Table", ...);
		///
		/// but in some cases better readable.

	~Statement();
		/// Destroys the Statement.

	Statement(const Statement& stmt);
		/// Copy constructor. 
		/// If the statement has been executed asynchronously and has not been
		/// synchronized prior to copy operation (i.e. is copied while executing), 
		/// this constructor shall synchronize it.

	Statement& operator = (const Statement& stmt);
		/// Assignment operator.

	void swap(Statement& other);
		/// Swaps the statement with another one.

	template <typename T> 
	Statement& operator << (const T& t)
		/// Concatenates data with the SQL statement string.
	{
		_pImpl->add(t);
		return *this;
	}

	Statement& operator , (Manipulator manip);
		/// Handles manipulators, such as now, async, etc.

	Statement& operator , (AbstractBinding::Ptr pBind);
		/// Registers the Binding with the Statement by calling addBind().

	Statement& addBind(AbstractBinding::Ptr pBind);
		/// Registers a single binding with the statement.

	void removeBind(const std::string& name);
		/// Removes the all the bindings with specified name from the statement.

	Statement& operator , (AbstractBindingVec& bindVec);
		/// Registers the Binding vector with the Statement.

	template <typename C>
	Statement& addBinding(C& bindingCont, bool reset)
		/// Registers binding container with the Statement.
	{
		if (reset) _pImpl->resetBinding();
		typename C::iterator itAB = bindingCont.begin();
		typename C::iterator itABEnd = bindingCont.end();
		for (; itAB != itABEnd; ++itAB) addBind(*itAB);
		return *this;
	}

	Statement& operator , (AbstractExtraction::Ptr extract);
		/// Registers objects used for extracting data with the Statement by 
		/// calling addExtract().

	Statement& operator , (AbstractExtractionVec& extVec);
		/// Registers the extraction vector with the Statement.
		/// The vector is registered at position 0 (i.e. for the first returned data set).

	Statement& operator , (AbstractExtractionVecVec& extVecVec);
		/// Registers the vector of extraction vectors with the Statement.

	template <typename C>
	Statement& addExtraction(C& val, bool reset)
		/// Registers extraction container with the Statement.
	{
		if (reset) _pImpl->resetExtraction();
		typename C::iterator itAE = val.begin();
		typename C::iterator itAEEnd = val.end();
		for (; itAE != itAEEnd; ++itAE) addExtract(*itAE);
		return *this;
	}

	template <typename C>
	Statement& addExtractions(C& val)
		/// Registers container of extraction containers with the Statement.
	{
		_pImpl->resetExtraction();
		typename C::iterator itAEV = val.begin();
		typename C::iterator itAEVEnd = val.end();
		for (; itAEV != itAEVEnd; ++itAEV) addExtraction(*itAEV, false);
		return *this;
	}

	Statement& addExtract(AbstractExtraction::Ptr pExtract);
		/// Registers a single extraction with the statement.

	Statement& operator , (const Bulk& bulk);
		/// Sets the bulk execution mode (both binding and extraction) for this 
		/// statement.Statement must not have any extractors or binders set at the 
		/// time when this operator is applied. 
		/// Failure to adhere to the above constraint shall result in 
		/// InvalidAccessException.

	Statement& operator , (BulkFnType);
		/// Sets the bulk execution mode (both binding and extraction) for this 
		/// statement.Statement must not have any extractors or binders set at the 
		/// time when this operator is applied. 
		/// Additionally, this function requires limit to be set in order to
		/// determine the bulk size.
		/// Failure to adhere to the above constraints shall result in 
		/// InvalidAccessException.

	Statement& operator , (const Limit& extrLimit);
		/// Sets a limit on the maximum number of rows a select is allowed to return.
		///
		/// Set per default to zero to Limit::LIMIT_UNLIMITED, which disables the limit.

	Statement& operator , (RowFormatter::Ptr pRowFformatter);
		/// Sets the row formatter for the statement.

	Statement& operator , (const Range& extrRange);
		/// Sets a an extraction range for the maximum number of rows a select is allowed to return.
		///
		/// Set per default to Limit::LIMIT_UNLIMITED which disables the range.

	Statement& operator , (char value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::UInt8 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::Int8 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::UInt16 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::Int16 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::UInt32 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::Int32 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

#ifndef POCO_LONG_IS_64_BIT
	Statement& operator , (long value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (unsigned long value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.
#endif
	Statement& operator , (Poco::UInt64 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (Poco::Int64 value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (double value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (float value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (bool value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (const std::string& value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	Statement& operator , (const char* value);
		/// Adds the value to the list of values to be supplied to the SQL string formatting function.

	const std::string& toString() const;
		/// Creates a string from the accumulated SQL statement.

	std::size_t execute(bool reset = true);
		/// Executes the statement synchronously or asynchronously. 
		/// Stops when either a limit is hit or the whole statement was executed.
		/// Returns the number of rows extracted from the database (for statements
		/// returning data) or number of rows affected (for all other statements).
		/// If reset is true (default), associated storage is reset and reused.
		/// Otherwise, the results from this execution step are appended.
		/// Reset argument has no meaning for unlimited statements that return all rows.
		/// If isAsync() returns  true, the statement is executed asynchronously 
		/// and the return value from this function is zero.
		/// The result of execution (i.e. number of returned or affected rows) can be 
		/// obtained by calling wait() on the statement at a later point in time.

	const Result& executeAsync(bool reset = true);
		/// Executes the statement asynchronously. 
		/// Stops when either a limit is hit or the whole statement was executed.
		/// Returns immediately. Calling wait() (on either the result returned from this 
		/// call or the statement itself) returns the number of rows extracted or number 
		/// of rows affected by the statement execution.
		/// When executed on a synchronous statement, this method does not alter the
		/// statement's synchronous nature.

	void setAsync(bool async = true);
		/// Sets the asynchronous flag. If this flag is true, executeAsync() is called 
		/// from the now() manipulator. This setting does not affect the statement's
		/// capability to be executed synchronously by directly calling execute().

	bool isAsync() const;
		/// Returns true if statement was marked for asynchronous execution.

	std::size_t wait(long milliseconds = WAIT_FOREVER);
		/// Waits for the execution completion for asynchronous statements or
		/// returns immediately for synchronous ones. The return value for 
		/// asynchronous statement is the execution result (i.e. number of 
		/// rows retrieved). For synchronous statements, the return value is zero.

	bool initialized();
		/// Returns true if the statement was initialized (i.e. not executed yet).

	bool paused();
		/// Returns true if the statement was paused (a range limit stopped it
		/// and there is more work to do).

	bool done();
		/// Returns true if the statement was completely executed or false if a range limit stopped it
		/// and there is more work to do. When no limit is set, it will always return true after calling execute().

	Statement& reset(Session& session);
		/// Resets the Statement so that it can be filled with a new SQL command.

	bool canModifyStorage();
		/// Returns true if statement is in a state that allows the internal storage to be modified.

	Storage storage() const;
		/// Returns the internal storage type for the statement.

	void setStorage(const std::string& storage);
		/// Sets the internal storage type for the statement.

	const std::string& getStorage() const;
		/// Returns the internal storage type for the statement.

	std::size_t columnsExtracted(int dataSet = StatementImpl::USE_CURRENT_DATA_SET) const;
		/// Returns the number of columns returned for current data set.
		/// Default value indicates current data set (if any).

	std::size_t rowsExtracted(int dataSet = StatementImpl::USE_CURRENT_DATA_SET) const;
		/// Returns the number of rows returned for current data set during last statement
		/// execution. Default value indicates current data set (if any).

	std::size_t subTotalRowCount(int dataSet = StatementImpl::USE_CURRENT_DATA_SET) const;
		/// Returns the number of rows extracted so far for the data set.
		/// Default value indicates current data set (if any).

	std::size_t extractionCount() const;
		/// Returns the number of extraction storage buffers associated
		/// with the current data set.

	std::size_t dataSetCount() const;
		/// Returns the number of data sets associated with the statement.

	std::size_t nextDataSet();
		/// Returns the index of the next data set.

	std::size_t previousDataSet();
		/// Returns the index of the previous data set.

	bool hasMoreDataSets() const;
		/// Returns false if the current data set index points to the last
		/// data set. Otherwise, it returns true.

	void setRowFormatter(RowFormatter::Ptr pRowFormatter);
		/// Sets the row formatter for this statement.
		/// Statement takes the ownership of the formatter.

protected:
	typedef StatementImpl::Ptr ImplPtr;

	const AbstractExtractionVec& extractions() const;
		/// Returns the extractions vector.

	const MetaColumn& metaColumn(std::size_t pos) const;
		/// Returns the type for the column at specified position.

	const MetaColumn& metaColumn(const std::string& name) const;
		/// Returns the type for the column with specified name.

	 bool isNull(std::size_t col, std::size_t row) const;
		/// Returns true if the current row value at column pos is null.

	 bool isBulkExtraction() const;
		/// Returns true if this statement extracts data in bulk.

	ImplPtr impl() const;
		/// Returns pointer to statement implementation.

	const RowFormatter::Ptr& getRowFormatter();
		/// Returns the row formatter for this statement.

	Session session();
		/// Returns the underlying session.

private:

	const Result& doAsyncExec(bool reset = true);
		/// Asynchronously executes the statement.

	template <typename T>
	Statement& commaPODImpl(const T& val)
	{
		_arguments.push_back(val);
		return *this;
	}

	StatementImpl::Ptr _pImpl;

	// asynchronous execution related members
	bool                _async;
	mutable ResultPtr   _pResult;
	Mutex               _mutex;
	AsyncExecMethodPtr  _pAsyncExec;
	std::vector<Any>    _arguments;
	RowFormatter::Ptr   _pRowFormatter;
	mutable std::string _stmtString;
};

//
// inlines

inline std::size_t Statement::subTotalRowCount(int dataSet) const
{
	return _pImpl->subTotalRowCount(dataSet);
}


namespace Keywords {


//
// Manipulators
//

inline void Data_API now(Statement& statement)
	/// Enforces immediate execution of the statement.
	/// If _isAsync flag has been set, execution is invoked asynchronously.
{
	statement.execute();
}


inline void Data_API sync(Statement& statement)
	/// Sets the _isAsync flag to false, signalling synchronous execution.
	/// Synchronous execution is default, so specifying this manipulator
	/// only makes sense if async() was called for the statement before.
{
	statement.setAsync(false);
}


inline void Data_API async(Statement& statement)
	/// Sets the _async flag to true, signalling asynchronous execution.
{
	statement.setAsync(true);
}


inline void Data_API deque(Statement& statement)
	/// Sets the internal storage to std::deque.
	/// std::deque is default storage, so specifying this manipulator
	/// only makes sense if list() or deque() were called for the statement before.
{
	if (!statement.canModifyStorage())
		throw InvalidAccessException("Storage not modifiable.");

	statement.setStorage("deque");
}


inline void Data_API vector(Statement& statement)
	/// Sets the internal storage to std::vector.
{
	if (!statement.canModifyStorage())
		throw InvalidAccessException("Storage not modifiable.");

	statement.setStorage("vector");
}


inline void Data_API list(Statement& statement)
	/// Sets the internal storage to std::list.
{
	if (!statement.canModifyStorage())
		throw InvalidAccessException("Storage not modifiable.");

	statement.setStorage("list");
}


inline void Data_API reset(Statement& statement)
	/// Sets all internal settings to their respective default values.
{
	if (!statement.canModifyStorage())
		throw InvalidAccessException("Storage not modifiable.");

	statement.setStorage("deque");
	statement.setAsync(false);
}


} // namespace Keywords


//
// inlines
//

inline Statement& Statement::operator , (RowFormatter::Ptr pRowFformatter)
{
	_pRowFormatter = pRowFformatter;
	return *this;
}


inline Statement& Statement::operator , (char value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::UInt8 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::Int8 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::UInt16 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::Int16 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::UInt32 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::Int32 value)
{
	return commaPODImpl(value);
}


#ifndef POCO_LONG_IS_64_BIT
inline Statement& Statement::operator , (long value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (unsigned long value)
{
	return commaPODImpl(value);
}
#endif


inline Statement& Statement::operator , (Poco::UInt64 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (Poco::Int64 value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (double value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (float value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (bool value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (const std::string& value)
{
	return commaPODImpl(value);
}


inline Statement& Statement::operator , (const char* value)
{
	return commaPODImpl(std::string(value));
}


inline void Statement::removeBind(const std::string& name)
{
	_pImpl->removeBind(name);
}


inline Statement& Statement::operator , (AbstractBinding::Ptr pBind)
{
	return addBind(pBind);
}


inline Statement& Statement::operator , (AbstractBindingVec& bindVec)
{
	return addBinding(bindVec, false);
}


inline Statement& Statement::operator , (AbstractExtraction::Ptr pExtract)
{
	return addExtract(pExtract);
}


inline Statement& Statement::operator , (AbstractExtractionVec& extVec)
{
	return addExtraction(extVec, false);
}


inline Statement& Statement::operator , (AbstractExtractionVecVec& extVecVec)
{
	return addExtractions(extVecVec);
}


inline Statement::ImplPtr Statement::impl() const
{
	return _pImpl;
}


inline const std::string& Statement::toString() const
{
	return _stmtString = _pImpl->toString();
}

inline const AbstractExtractionVec& Statement::extractions() const
{
	return _pImpl->extractions();
}


inline const MetaColumn& Statement::metaColumn(std::size_t pos) const
{
	return _pImpl->metaColumn(pos);
}


inline const MetaColumn& Statement::metaColumn(const std::string& name) const
{
	return _pImpl->metaColumn(name);
}


inline void Statement::setStorage(const std::string& storage)
{
	_pImpl->setStorage(storage);
}


inline std::size_t Statement::extractionCount() const
{
	return _pImpl->extractionCount();
}


inline std::size_t Statement::columnsExtracted(int dataSet) const
{
	return _pImpl->columnsExtracted(dataSet);
}


inline std::size_t Statement::rowsExtracted(int dataSet) const
{
	return _pImpl->rowsExtracted(dataSet);
}


inline std::size_t Statement::dataSetCount() const
{
	return _pImpl->dataSetCount();
}


inline std::size_t Statement::nextDataSet()
{
	return _pImpl->activateNextDataSet();
}


inline std::size_t Statement::previousDataSet()
{
	return _pImpl->activatePreviousDataSet();
}


inline bool Statement::hasMoreDataSets() const
{
	return _pImpl->hasMoreDataSets();
}


inline Statement::Storage Statement::storage() const
{
	return static_cast<Storage>(_pImpl->getStorage());
}


inline bool Statement::canModifyStorage()
{
	return (0 == extractionCount()) && (initialized() || done());
}


inline bool Statement::initialized()
{
	return _pImpl->getState() == StatementImpl::ST_INITIALIZED;
}


inline bool Statement::paused()
{
	return _pImpl->getState() == StatementImpl::ST_PAUSED;
}


inline bool Statement::done()
{
	return _pImpl->getState() == StatementImpl::ST_DONE;
}


inline bool Statement::isNull(std::size_t col, std::size_t row) const
{
	return _pImpl->isNull(col, row);
}


inline bool Statement::isBulkExtraction() const
{
	return _pImpl->isBulkExtraction();
}


inline bool Statement::isAsync() const
{
	return _async;
}


inline void Statement::setRowFormatter(RowFormatter::Ptr pRowFormatter)
{
	_pRowFormatter = pRowFormatter;
}


inline const RowFormatter::Ptr& Statement::getRowFormatter()
{
	if (!_pRowFormatter) _pRowFormatter = new SimpleRowFormatter;
	return _pRowFormatter;
}


inline void swap(Statement& s1, Statement& s2)
{
	s1.swap(s2);
}


} } // namespace Poco::Data


namespace std
{
	template<>
	inline void swap<Poco::Data::Statement>(Poco::Data::Statement& s1, 
		Poco::Data::Statement& s2)
		/// Full template specalization of std:::swap for Statement
	{
		s1.swap(s2);
	}
}


#endif // Data_Statement_INCLUDED
