#include <DB/AggregateFunctions/AggregateFunctionIf.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionIf(AggregateFunctionPtr & nested)
{
	return new AggregateFunctionIf(nested);
}

}
