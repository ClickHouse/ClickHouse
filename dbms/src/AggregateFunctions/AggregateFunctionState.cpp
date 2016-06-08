#include <DB/AggregateFunctions/AggregateFunctionState.h>

namespace DB
{

AggregateFunctionPtr createAggregateFunctionState(AggregateFunctionPtr & nested)
{
	return std::make_shared<AggregateFunctionState>(nested);
}

}
