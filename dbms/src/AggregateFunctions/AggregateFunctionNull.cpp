#include <DB/AggregateFunctions/AggregateFunctionNull.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionNull(AggregateFunctionPtr & nested)
{
	return std::make_shared<AggregateFunctionNull>(nested);
}

}
