//
// MySQLstatementImpl.h
//
// $Id: //poco/1.4/Data/MySQL/include/Poco/Data/MySQL/MySQLStatementImpl.h#1 $
//
// Library: Data
// Package: MySQL
// Module:  MySQLstatementImpl
//
// Definition of the MySQLStatementImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MySQL_MySQLStatementImpl_INCLUDED
#define Data_MySQL_MySQLStatementImpl_INCLUDED

#include "Poco/Data/MySQL/MySQL.h"
#include "Poco/Data/MySQL/SessionImpl.h"
#include "Poco/Data/MySQL/Binder.h"
#include "Poco/Data/MySQL/Extractor.h"
#include "Poco/Data/MySQL/StatementExecutor.h"
#include "Poco/Data/MySQL/ResultMetadata.h"
#include "Poco/Data/StatementImpl.h"
#include "Poco/SharedPtr.h"
#include "Poco/Format.h"


namespace Poco {
namespace Data {
namespace MySQL {


class MySQL_API MySQLStatementImpl: public Poco::Data::StatementImpl
	/// Implements statement functionality needed for MySQL
{
public:
	MySQLStatementImpl(SessionImpl& s);
		/// Creates the MySQLStatementImpl.
		
	~MySQLStatementImpl();
		/// Destroys the MySQLStatementImpl.
		
protected:

	virtual std::size_t columnsReturned() const;
		/// Returns number of columns returned by query.

	virtual int affectedRowCount() const;
		/// Returns the number of affected rows.
		/// Used to find out the number of rows affected by insert, delete or update.
	
	virtual const MetaColumn& metaColumn(std::size_t pos) const;
		/// Returns column meta data.
		
	virtual bool hasNext();
		/// Returns true if a call to next() will return data.
		
	virtual std::size_t next();
		/// Retrieves the next row from the resultset.
		/// Will throw, if the resultset is empty.
	
	virtual bool canBind() const;
		/// Returns true if a valid statement is set and we can bind.

	virtual bool canCompile() const;
		/// Returns true if another compile is possible.
		
	virtual void compileImpl();
		/// Compiles the statement, doesn't bind yet
		
	virtual void bindImpl();
		/// Binds parameters
		
	virtual Poco::Data::AbstractExtractor::Ptr extractor();
		/// Returns the concrete extractor used by the statement.
		
	virtual Poco::Data::AbstractBinder::Ptr binder();
		/// Returns the concrete binder used by the statement.

private:
	enum
	{
		NEXT_DONTKNOW,
		NEXT_TRUE,
		NEXT_FALSE
	};

	StatementExecutor _stmt;
	ResultMetadata    _metadata;
	Binder::Ptr       _pBinder;
	Extractor::Ptr    _pExtractor;
	int               _hasNext;
	};


} } } // namespace Poco::Data::MySQL


#endif // Data_MySQL_MySQLStatementImpl_INCLUDED
