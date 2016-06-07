#include <DB/AggregateFunctions/AggregateFunctionArray.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionArray(AggregateFunctionPtr & nested)
{
	return new AggregateFunctionArray(nested);
}

}
