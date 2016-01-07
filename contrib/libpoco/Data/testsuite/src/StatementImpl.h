//
// StatementImpl.h
//
// $Id: //poco/Main/Data/testsuite/src/StatementImpl.h#2 $
//
// Definition of the StatementImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Test_StatementImpl_INCLUDED
#define Data_Test_StatementImpl_INCLUDED


#include "Poco/Data/StatementImpl.h"
#include "Poco/SharedPtr.h"
#include "Binder.h"
#include "Extractor.h"
#include "Preparator.h"


struct sqlite3;
struct sqlite3_stmt;


namespace Poco {
namespace Data {
namespace Test {


class StatementImpl: public Poco::Data::StatementImpl
	/// A no-op implementation of StatementImpl for testing.
{
public:
	StatementImpl();
		/// Creates the StatementImpl.

	~StatementImpl();
		/// Destroys the StatementImpl.

protected:
	bool hasNext();
		/// Returns true if a call to next() will return data.

	void next();
		/// Retrieves the next row from the resultset.
		/// Will throw, if the resultset is empty.

	bool canBind() const;
		/// Returns true if a valid statement is set and we can bind.

	void compileImpl();
		/// Compiles the statement, doesn't bind yet

	void bindImpl();
		/// Binds parameters

	AbstractExtractor& extractor();
		/// Returns the concrete extractor used by the statement.

	AbstractBinder& binder();
		/// Returns the concrete binder used by the statement.

private:
	Poco::SharedPtr<Binder>    _ptrBinder;
	Poco::SharedPtr<Extractor> _ptrExtractor;
	Poco::SharedPtr<Preparation> _ptrPrepare;
};


//
// inlines
//
inline AbstractExtractor& StatementImpl::extractor()
{
	return *_ptrExtractor;
}


inline AbstractBinder& StatementImpl::binder()
{
	return *_ptrBinder;
}


} } } // namespace Poco::Data::Test


#endif // Data_Test_StatementImpl_INCLUDED
