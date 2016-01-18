//
// StatementImpl.cpp
//
// $Id: //poco/Main/Data/testsuite/src/StatementImpl.cpp#2 $
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "StatementImpl.h"


namespace Poco {
namespace Data {
namespace Test {


StatementImpl::StatementImpl()
{
}


StatementImpl::~StatementImpl()
{
}


void StatementImpl::compileImpl()
{
	// prepare binding
	_ptrBinder    = new Binder;
	_ptrExtractor = new Extractor;
	_ptrPrepare   = new Preparation();
}


bool StatementImpl::canBind() const
{
	return false;
}


void StatementImpl::bindImpl()
{
	// bind
	typedef Poco::Data::AbstractBindingVec Bindings;
	Bindings& binds = bindings();
	if (binds.empty())
		return;

	Bindings::iterator it    = binds.begin();
	Bindings::iterator itEnd = binds.end();
	std::size_t pos = 0;
	for (; it != itEnd && (*it)->canBind(); ++it)
	{
		(*it)->bind(pos);
		pos += (*it)->numOfColumnsHandled();
	}
}


bool StatementImpl::hasNext()
{
	return false;
}


void StatementImpl::next()
{
	Poco::Data::AbstractExtractionVec::iterator it    = extractions().begin();
	Poco::Data::AbstractExtractionVec::iterator itEnd = extractions().end();
	std::size_t pos = 0; 
	for (; it != itEnd; ++it)
	{
		(*it)->extract(pos);
		pos += (*it)->numOfColumnsHandled();
	}
}


} } } // namespace Poco::Data::Test
