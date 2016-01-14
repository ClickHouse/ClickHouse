//
// DataException.h
//
// $Id: //poco/Main/Data/include/Poco/Data/DataException.h#8 $
//
// Library: Data
// Package: DataCore
// Module:  DataException
//
// Definition of the DataException class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_DataException_INCLUDED
#define Data_DataException_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Exception.h"


namespace Poco {
namespace Data {


POCO_DECLARE_EXCEPTION(Data_API, DataException, Poco::IOException)
POCO_DECLARE_EXCEPTION(Data_API, RowDataMissingException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, UnknownDataBaseException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, UnknownTypeException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, ExecutionException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, BindingException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, ExtractException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, LimitException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, NotSupportedException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, SessionUnavailableException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, SessionPoolExhaustedException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, SessionPoolExistsException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, NoDataException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, LengthExceededException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, ConnectionFailedException, DataException)
POCO_DECLARE_EXCEPTION(Data_API, NotConnectedException, DataException)


} } // namespace Poco::Data


#endif // Data_DataException_INCLUDED
