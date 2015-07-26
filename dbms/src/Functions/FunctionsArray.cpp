#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsArray.h>

namespace DB
{

void registerFunctionsArray(FunctionFactory & factory)
{
	factory.registerFunction<FunctionArray>();
	factory.registerFunction<FunctionArrayElement>();
	factory.registerFunction<FunctionHas>();
	factory.registerFunction<FunctionIndexOf>();
	factory.registerFunction<FunctionCountEqual>();
	factory.registerFunction<FunctionArrayEnumerate>();
	factory.registerFunction<FunctionArrayEnumerateUniq>();
	factory.registerFunction<FunctionArrayUniq>();
	factory.registerFunction<FunctionEmptyArrayUInt8>();
	factory.registerFunction<FunctionEmptyArrayUInt16>();
	factory.registerFunction<FunctionEmptyArrayUInt32>();
	factory.registerFunction<FunctionEmptyArrayUInt64>();
	factory.registerFunction<FunctionEmptyArrayInt8>();
	factory.registerFunction<FunctionEmptyArrayInt16>();
	factory.registerFunction<FunctionEmptyArrayInt32>();
	factory.registerFunction<FunctionEmptyArrayInt64>();
	factory.registerFunction<FunctionEmptyArrayFloat32>();
	factory.registerFunction<FunctionEmptyArrayFloat64>();
	factory.registerFunction<FunctionEmptyArrayDate>();
	factory.registerFunction<FunctionEmptyArrayDateTime>();
	factory.registerFunction<FunctionEmptyArrayString>();
	factory.registerFunction<FunctionEmptyArrayToSingle>();
	factory.registerFunction<FunctionRange>();
}

}
