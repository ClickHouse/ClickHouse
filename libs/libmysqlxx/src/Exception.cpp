#include <typeinfo>

#include <mysqlxx/Exception.h>


namespace mysqlxx
{

POCO_IMPLEMENT_EXCEPTION(Exception, Poco::Exception, "Exception");

}
