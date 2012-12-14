#include "OLAPQueryConverter.h"


namespace DB
{
namespace OLAP
{

QueryConverter::QueryConverter(Poco::Util::AbstractConfiguration & config)
{
	
}

void QueryConverter::OLAPServerQueryToClickhouse(const QueryParseResult & query, Context & inout_context, std::string & out_query)
{
	Settings new_settings = inout_context.getSettings();
	
	if (query.concurrency != 0)
		new_settings.max_threads = query.concurrency;
	
	inout_context.setSettings(new_settings);
	
	out_query = "select 42, number from system.numbers limit 5";
}

}
}
