//
// ActiveRunnable.h
//
// Library: Foundation
// Package: Threading
// Module:  ActiveObjects
//
// Definition of the ActiveRunnable class.
//
// Copyright (c) 2004-2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ActiveRunnable_INCLUDED
#define Foundation_ActiveRunnable_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/ActiveResult.h"
#include "Poco/Runnable.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include "Poco/Exception.h"


namespace Poco {


class ActiveRunnableBase: public Runnable, public RefCountedObject
	/// The base class for all ActiveRunnable instantiations.
{
public:
	typedef AutoPtr<ActiveRunnableBase> Ptr;
};


template <class ResultType, class ArgType, class OwnerType>
class ActiveRunnable: public ActiveRunnableBase
	/// This class is used by ActiveMethod.
	/// See the ActiveMethod class for more information.
{
public:
	typedef ResultType (OwnerType::*Callback)(const ArgType&);
	typedef ActiveResult<ResultType> ActiveResultType;

	ActiveRunnable(OwnerType* pOwner, Callback method, const ArgType& arg, const ActiveResultType& result):
		_pOwner(pOwner),
		_method(method),
		_arg(arg),
		_result(result)
	{
		poco_check_ptr (pOwner);
	}

	void run()
	{
		ActiveRunnableBase::Ptr guard(this, false); // ensure automatic release when done
		try
		{
			_result.data(new ResultType((_pOwner->*_method)(_arg)));
		}
		catch (Exception& e)
		{
			_result.error(e);
		}
		catch (std::exception& e)
		{
			_result.error(e.what());
		}
		catch (...)
		{
			_result.error("unknown exception");
		}
		_result.notify();
	}

private:
	OwnerType* _pOwner;
	Callback   _method;
	ArgType    _arg;
	ActiveResultType _result;
};


template <class ArgType, class OwnerType>
class ActiveRunnable<void, ArgType, OwnerType>: public ActiveRunnableBase
	/// This class is used by ActiveMethod.
	/// See the ActiveMethod class for more information.
{
public:
	typedef void (OwnerType::*Callback)(const ArgType&);
	typedef ActiveResult<void> ActiveResultType;

	ActiveRunnable(OwnerType* pOwner, Callback method, const ArgType& arg, const ActiveResultType& result):
		_pOwner(pOwner),
		_method(method),
		_arg(arg),
		_result(result)
	{
		poco_check_ptr (pOwner);
	}

	void run()
	{
		ActiveRunnableBase::Ptr guard(this, false); // ensure automatic release when done
		try
		{
			(_pOwner->*_method)(_arg);
		}
		catch (Exception& e)
		{
			_result.error(e);
		}
		catch (std::exception& e)
		{
			_result.error(e.what());
		}
		catch (...)
		{
			_result.error("unknown exception");
		}
		_result.notify();
	}

private:
	OwnerType* _pOwner;
	Callback   _method;
	ArgType    _arg;
	ActiveResultType _result;
};


template <class ResultType, class OwnerType>
class ActiveRunnable<ResultType, void, OwnerType>: public ActiveRunnableBase
	/// This class is used by ActiveMethod.
	/// See the ActiveMethod class for more information.
{
public:
	typedef ResultType (OwnerType::*Callback)();
	typedef ActiveResult<ResultType> ActiveResultType;

	ActiveRunnable(OwnerType* pOwner, Callback method, const ActiveResultType& result):
		_pOwner(pOwner),
		_method(method),
		_result(result)
	{
		poco_check_ptr (pOwner);
	}

	void run()
	{
		ActiveRunnableBase::Ptr guard(this, false); // ensure automatic release when done
		try
		{
			_result.data(new ResultType((_pOwner->*_method)()));
		}
		catch (Exception& e)
		{
			_result.error(e);
		}
		catch (std::exception& e)
		{
			_result.error(e.what());
		}
		catch (...)
		{
			_result.error("unknown exception");
		}
		_result.notify();
	}

private:
	OwnerType* _pOwner;
	Callback   _method;
	ActiveResultType _result;
};


template <class OwnerType>
class ActiveRunnable<void, void, OwnerType>: public ActiveRunnableBase
	/// This class is used by ActiveMethod.
	/// See the ActiveMethod class for more information.
{
public:
	typedef void (OwnerType::*Callback)();
	typedef ActiveResult<void> ActiveResultType;

	ActiveRunnable(OwnerType* pOwner, Callback method, const ActiveResultType& result):
		_pOwner(pOwner),
		_method(method),
		_result(result)
	{
		poco_check_ptr (pOwner);
	}

	void run()
	{
		ActiveRunnableBase::Ptr guard(this, false); // ensure automatic release when done
		try
		{
			(_pOwner->*_method)();
		}
		catch (Exception& e)
		{
			_result.error(e);
		}
		catch (std::exception& e)
		{
			_result.error(e.what());
		}
		catch (...)
		{
			_result.error("unknown exception");
		}
		_result.notify();
	}

private:
	OwnerType* _pOwner;
	Callback   _method;
	ActiveResultType _result;
};


} // namespace Poco


#endif // Foundation_ActiveRunnable_INCLUDED
