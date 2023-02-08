//
// ActiveResult.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the ActiveResult class.
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ActiveResult_INCLUDED
#define Foundation_ActiveResult_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Mutex.h"
#include "Poco/Event.h"
#include "Poco/RefCountedObject.h"
#include "Poco/Exception.h"
#include <algorithm>


namespace Poco {


template <class ResultType>
class ActiveResultHolder: public RefCountedObject
	/// This class holds the result of an asynchronous method
	/// invocation. It is used to pass the result from the
	/// execution thread back to the invocation thread. 
	/// The class uses reference counting for memory management.
	/// Do not use this class directly, use ActiveResult instead.
{
public:
	ActiveResultHolder():
		_pData(0),
		_pExc(0),
		_event(false)
		/// Creates an ActiveResultHolder.
	{
	}
		
	ResultType& data()
		/// Returns a reference to the actual result.
	{
		poco_check_ptr(_pData);
		return *_pData;
	}
	
	void data(ResultType* pData)
	{
		delete _pData;
		_pData = pData;
	}
	
	void wait()
		/// Pauses the caller until the result becomes available.
	{
		_event.wait();
	}
	
	bool tryWait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Returns true if the result became
		/// available, false otherwise.
	{
		return _event.tryWait(milliseconds);
	}
	
	void wait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Throws a TimeoutException if the
		/// result did not became available.
	{
		_event.wait(milliseconds);
	}
	
	void notify()
		/// Notifies the invoking thread that the result became available.
	{
		_event.set();
	}
	
	bool failed() const
		/// Returns true if the active method failed (and threw an exception).
		/// Information about the exception can be obtained by calling error().
	{
		return _pExc != 0;
	}
	
	std::string error() const
		/// If the active method threw an exception, a textual representation
		/// of the exception is returned. An empty string is returned if the
		/// active method completed successfully.
	{
		if (_pExc)
			return _pExc->message();
		else
			return std::string();
	}
	
	Exception* exception() const
		/// If the active method threw an exception, a clone of the exception
		/// object is returned, otherwise null.
	{
		return _pExc;
	}
	
	void error(const Exception& exc)
		/// Sets the exception.
	{
		delete _pExc;
		_pExc = exc.clone();
	}
	
	void error(const std::string& msg)
		/// Sets the exception.
	{
		delete _pExc;
		_pExc = new UnhandledException(msg);
	}

protected:
	~ActiveResultHolder()
	{
		delete _pData;
		delete _pExc;
	}

private:
	ResultType* _pData;
	Exception*  _pExc;
	Event       _event;
};



template <>
class ActiveResultHolder<void>: public RefCountedObject
{
public:
	ActiveResultHolder():
		_pExc(0),
		_event(false)
		/// Creates an ActiveResultHolder.
	{
	}
	
	void wait()
		/// Pauses the caller until the result becomes available.
	{
		_event.wait();
	}
	
	bool tryWait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Returns true if the result became
		/// available, false otherwise.
	{
		return _event.tryWait(milliseconds);
	}
	
	void wait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Throws a TimeoutException if the
		/// result did not became available.
	{
		_event.wait(milliseconds);
	}
	
	void notify()
		/// Notifies the invoking thread that the result became available.
	{
		_event.set();
	}
	
	bool failed() const
		/// Returns true if the active method failed (and threw an exception).
		/// Information about the exception can be obtained by calling error().
	{
		return _pExc != 0;
	}
	
	std::string error() const
		/// If the active method threw an exception, a textual representation
		/// of the exception is returned. An empty string is returned if the
		/// active method completed successfully.
	{
		if (_pExc)
			return _pExc->message();
		else
			return std::string();
	}
	
	Exception* exception() const
		/// If the active method threw an exception, a clone of the exception
		/// object is returned, otherwise null.
	{
		return _pExc;
	}
	
	void error(const Exception& exc)
		/// Sets the exception.
	{
		delete _pExc;
		_pExc = exc.clone();
	}
	
	void error(const std::string& msg)
		/// Sets the exception.
	{
		delete _pExc;
		_pExc = new UnhandledException(msg);
	}

protected:
	~ActiveResultHolder()
	{
		delete _pExc;
	}

private:
	Exception*  _pExc;
	Event       _event;
};


template <class RT>
class ActiveResult
	/// This class holds the result of an asynchronous method
	/// invocation (see class ActiveMethod). It is used to pass the 
	/// result from the execution thread back to the invocation thread. 
{
public:
	typedef RT ResultType;
	typedef ActiveResultHolder<ResultType> ActiveResultHolderType;

	ActiveResult(ActiveResultHolderType* pHolder):
		_pHolder(pHolder)
		/// Creates the active result. For internal use only.
	{
		poco_check_ptr (pHolder);
	}
	
	ActiveResult(const ActiveResult& result)
		/// Copy constructor.
	{
		_pHolder = result._pHolder;
		_pHolder->duplicate();
	}
	
	~ActiveResult()
		/// Destroys the result.
	{
		_pHolder->release();
	}
	
	ActiveResult& operator = (const ActiveResult& result)
		/// Assignment operator.
	{
		ActiveResult tmp(result);
		swap(tmp);
		return *this;
	}
	
	void swap(ActiveResult& result)
	{
		using std::swap;
		swap(_pHolder, result._pHolder);
	}
	
	ResultType& data() const
		/// Returns a reference to the result data.
	{
		return _pHolder->data();
	}
	
	void data(ResultType* pValue)
	{
		_pHolder->data(pValue);
	}
	
	void wait()
		/// Pauses the caller until the result becomes available.
	{
		_pHolder->wait();
	}
	
	bool tryWait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Returns true if the result became
		/// available, false otherwise.
	{
		return _pHolder->tryWait(milliseconds);
	}
	
	void wait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Throws a TimeoutException if the
		/// result did not became available.
	{
		_pHolder->wait(milliseconds);
	}
	
	bool available() const
		/// Returns true if a result is available.
	{
		return _pHolder->tryWait(0);
	}
	
	bool failed() const
		/// Returns true if the active method failed (and threw an exception).
		/// Information about the exception can be obtained by calling error().
	{
		return _pHolder->failed();
	}
	
	std::string error() const
		/// If the active method threw an exception, a textual representation
		/// of the exception is returned. An empty string is returned if the
		/// active method completed successfully.
	{
		return _pHolder->error();
	}

	Exception* exception() const
		/// If the active method threw an exception, a clone of the exception
		/// object is returned, otherwise null.
	{
		return _pHolder->exception();
	}

	void notify()
		/// Notifies the invoking thread that the result became available.
		/// For internal use only.
	{
		_pHolder->notify();
	}
	
	ResultType& data()
		/// Returns a non-const reference to the result data. For internal
		/// use only.
	{
		return _pHolder->data();
	}
	
	void error(const std::string& msg)
		/// Sets the failed flag and the exception message.
	{
		_pHolder->error(msg);
	}

	void error(const Exception& exc)
		/// Sets the failed flag and the exception message.
	{
		_pHolder->error(exc);
	}
	
private:
	ActiveResult();

	ActiveResultHolderType* _pHolder;
};



template <>
class ActiveResult<void>
	/// This class holds the result of an asynchronous method
	/// invocation (see class ActiveMethod). It is used to pass the 
	/// result from the execution thread back to the invocation thread. 
{
public:
	typedef ActiveResultHolder<void> ActiveResultHolderType;

	ActiveResult(ActiveResultHolderType* pHolder):
		_pHolder(pHolder)
		/// Creates the active result. For internal use only.
	{
		poco_check_ptr (pHolder);
	}
	
	ActiveResult(const ActiveResult& result)
		/// Copy constructor.
	{
		_pHolder = result._pHolder;
		_pHolder->duplicate();
	}
	
	~ActiveResult()
		/// Destroys the result.
	{
		_pHolder->release();
	}
	
	ActiveResult& operator = (const ActiveResult& result)
		/// Assignment operator.
	{
		ActiveResult tmp(result);
		swap(tmp);
		return *this;
	}
	
	void swap(ActiveResult& result)
	{
		using std::swap;
		swap(_pHolder, result._pHolder);
	}
	
	void wait()
		/// Pauses the caller until the result becomes available.
	{
		_pHolder->wait();
	}
	
	bool tryWait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Returns true if the result became
		/// available, false otherwise.
	{
		return _pHolder->tryWait(milliseconds);
	}
	
	void wait(long milliseconds)
		/// Waits up to the specified interval for the result to
		/// become available. Throws a TimeoutException if the
		/// result did not became available.
	{
		_pHolder->wait(milliseconds);
	}
	
	bool available() const
		/// Returns true if a result is available.
	{
		return _pHolder->tryWait(0);
	}
	
	bool failed() const
		/// Returns true if the active method failed (and threw an exception).
		/// Information about the exception can be obtained by calling error().
	{
		return _pHolder->failed();
	}
	
	std::string error() const
		/// If the active method threw an exception, a textual representation
		/// of the exception is returned. An empty string is returned if the
		/// active method completed successfully.
	{
		return _pHolder->error();
	}

	Exception* exception() const
		/// If the active method threw an exception, a clone of the exception
		/// object is returned, otherwise null.
	{
		return _pHolder->exception();
	}

	void notify()
		/// Notifies the invoking thread that the result became available.
		/// For internal use only.
	{
		_pHolder->notify();
	}
	
	void error(const std::string& msg)
		/// Sets the failed flag and the exception message.
	{
		_pHolder->error(msg);
	}

	void error(const Exception& exc)
		/// Sets the failed flag and the exception message.
	{
		_pHolder->error(exc);
	}
	
private:
	ActiveResult();

	ActiveResultHolderType* _pHolder;
};


} // namespace Poco


#endif // Foundation_ActiveResult_INCLUDED
