#include <mysqlxx/Value.h>
#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Query.h>
#include <mysqlxx/Exception.h>


void mysqlxx::Value::throwException(const char * text) const
{
	std::stringstream info;
	info << text;

	if (!isNull())
	{
		info << ": ";
		info.write(m_data, m_length);
	}

	if (res && res->getQuery())
		info << ", query: " << res->getQuery()->str().substr(0, MYSQLXX_QUERY_PREVIEW_LENGTH);

	throw CannotParseValue(info.str());
}
