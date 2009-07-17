#include <typeinfo>
#include <DB/Exception.h>


namespace DB
{
	
POCO_IMPLEMENT_EXCEPTION(Exception, Poco::Exception, "DB::Exception");

}
