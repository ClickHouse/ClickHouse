#include <typeinfo>
#include <DB/Core/Exception.h>


namespace DB
{
	
POCO_IMPLEMENT_EXCEPTION(Exception, Poco::Exception, "DB::Exception");

}
