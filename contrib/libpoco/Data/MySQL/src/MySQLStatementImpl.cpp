//
// MySQLException.cpp
//
// $Id: //poco/1.4/Data/MySQL/src/MySQLStatementImpl.cpp#1 $
//
// Library: Data
// Package: MySQL
// Module:  MySQLStatementImpl
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/MySQL/MySQLStatementImpl.h"

namespace Poco {
namespace Data {
namespace MySQL {


MySQLStatementImpl::MySQLStatementImpl(SessionImpl& h) :
	Poco::Data::StatementImpl(h), 
	_stmt(h.handle()), 
	_pBinder(new Binder),
	_pExtractor(new Extractor(_stmt, _metadata)), 
	_hasNext(NEXT_DONTKNOW)
{
}


MySQLStatementImpl::~MySQLStatementImpl()
{
}


std::size_t MySQLStatementImpl::columnsReturned() const
{
	return _metadata.columnsReturned();
}


int MySQLStatementImpl::affectedRowCount() const
{
	return _stmt.getAffectedRowCount();
}

	
const MetaColumn& MySQLStatementImpl::metaColumn(std::size_t pos) const
{
	return _metadata.metaColumn(pos);
}

	
bool MySQLStatementImpl::hasNext()
{
	if (_hasNext == NEXT_DONTKNOW)
	{
		if (_metadata.columnsReturned() == 0)
		{
			return false;
		}

		if (_stmt.fetch())
		{
			_hasNext = NEXT_TRUE;
			return true;
		}

		_hasNext = NEXT_FALSE;
		return false;
	}
	else if (_hasNext == NEXT_TRUE)
	{
		return true;
	}

	return false;
}

	
std::size_t MySQLStatementImpl::next()
{
	if (!hasNext())
		throw StatementException("No data received");	

	Poco::Data::AbstractExtractionVec::iterator it = extractions().begin();
	Poco::Data::AbstractExtractionVec::iterator itEnd = extractions().end();
	std::size_t pos = 0;

	for (; it != itEnd; ++it)
	{
		(*it)->extract(pos);
		pos += (*it)->numOfColumnsHandled();
	}

	_hasNext = NEXT_DONTKNOW;
	return 1;
}


bool MySQLStatementImpl::canBind() const
{
	bool ret = false;

	if ((_stmt.state() >= StatementExecutor::STMT_COMPILED) && !bindings().empty())
		ret = (*bindings().begin())->canBind();

	return ret;
}


bool MySQLStatementImpl::canCompile() const
{
	return (_stmt.state() < StatementExecutor::STMT_COMPILED);
}


void MySQLStatementImpl::compileImpl()
{
	_metadata.reset();
	_stmt.prepare(toString());
	_metadata.init(_stmt);

	if (_metadata.columnsReturned() > 0)
		_stmt.bindResult(_metadata.row());
}


void MySQLStatementImpl::bindImpl()
{
	Poco::Data::AbstractBindingVec& binds = bindings();
	std::size_t pos = 0;
	Poco::Data::AbstractBindingVec::iterator it = binds.begin();
	Poco::Data::AbstractBindingVec::iterator itEnd = binds.end();
	for (; it != itEnd && (*it)->canBind(); ++it)
	{
		(*it)->bind(pos);
		pos += (*it)->numOfColumnsHandled();
	}

	_stmt.bindParams(_pBinder->getBindArray(), _pBinder->size());
	_stmt.execute();
	_hasNext = NEXT_DONTKNOW;
}


Poco::Data::AbstractExtractor::Ptr MySQLStatementImpl::extractor()
{
	return _pExtractor;
}


Poco::Data::AbstractBinder::Ptr MySQLStatementImpl::binder()
{
	return _pBinder;
}


} } } // namespace Poco::Data::MySQL
