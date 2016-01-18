//
// DataException.cpp
//
// $Id: //poco/Main/Data/src/DataException.cpp#8 $
//
// Library: Data
// Package: DataCore
// Module:  DataException
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/DataException.h"
#include <typeinfo>


namespace Poco {
namespace Data {


POCO_IMPLEMENT_EXCEPTION(DataException, Poco::IOException, "Database Exception")
POCO_IMPLEMENT_EXCEPTION(RowDataMissingException, DataException, "Data for row missing")
POCO_IMPLEMENT_EXCEPTION(UnknownDataBaseException, DataException, "Type of data base unknown")
POCO_IMPLEMENT_EXCEPTION(UnknownTypeException, DataException, "Type of data unknown")
POCO_IMPLEMENT_EXCEPTION(ExecutionException, DataException, "Execution error")
POCO_IMPLEMENT_EXCEPTION(BindingException, DataException, "Binding error")
POCO_IMPLEMENT_EXCEPTION(ExtractException, DataException, "Extraction error")
POCO_IMPLEMENT_EXCEPTION(LimitException, DataException, "Limit error")
POCO_IMPLEMENT_EXCEPTION(NotSupportedException, DataException, "Feature or property not supported")
POCO_IMPLEMENT_EXCEPTION(SessionUnavailableException, DataException, "Session is unavailable")
POCO_IMPLEMENT_EXCEPTION(SessionPoolExhaustedException, DataException, "No more sessions available from the session pool")
POCO_IMPLEMENT_EXCEPTION(SessionPoolExistsException, DataException, "Session already exists in the pool")
POCO_IMPLEMENT_EXCEPTION(NoDataException, DataException, "No data found")
POCO_IMPLEMENT_EXCEPTION(LengthExceededException, DataException, "Data too long")
POCO_IMPLEMENT_EXCEPTION(ConnectionFailedException, DataException, "Connection attempt failed")
POCO_IMPLEMENT_EXCEPTION(NotConnectedException, DataException, "Not connected to data source")


} } // namespace Poco::Data
