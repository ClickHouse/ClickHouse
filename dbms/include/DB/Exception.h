#ifndef DBMS_EXCEPTION_H
#define DBMS_EXCEPTION_H

#include <Poco/Exception.h>


namespace DB
{
	
/** Тип исключения, чтобы отличать его от других.
  */
POCO_DECLARE_EXCEPTION(Foundation_API, Exception, Poco::Exception);

}

#endif
