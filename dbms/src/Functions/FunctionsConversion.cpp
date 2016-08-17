#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsConversion.h>

namespace DB
{

void throwExceptionForIncompletelyParsedValue(
	ReadBuffer & read_buffer, Block & block, const ColumnNumbers & arguments, size_t result)
{
	std::string message;
	{
		const IDataType & to_type = *block.getByPosition(result).type;

		WriteBufferFromString message_buf(message);
		message_buf << "Cannot parse string " << quote << String(read_buffer.buffer().begin(), read_buffer.buffer().size())
			<< " as " << to_type.getName()
			<< ": syntax error";

		if (read_buffer.offset())
			message_buf << " at position " << read_buffer.offset()
				<< " (parsed just " << quote << String(read_buffer.buffer().begin(), read_buffer.offset()) << ")";
		else
			message_buf << " at begin of string";

		if (to_type.behavesAsNumber())
			message_buf << ". Note: there are to" << to_type.getName() << "OrZero function, which returns zero instead of throwing exception.";
	}

	throw Exception(message, ErrorCodes::CANNOT_PARSE_TEXT);
}


void registerFunctionsConversion(FunctionFactory & factory)
{
	factory.registerFunction<FunctionToUInt8>();
	factory.registerFunction<FunctionToUInt16>();
	factory.registerFunction<FunctionToUInt32>();
	factory.registerFunction<FunctionToUInt64>();
	factory.registerFunction<FunctionToInt8>();
	factory.registerFunction<FunctionToInt16>();
	factory.registerFunction<FunctionToInt32>();
	factory.registerFunction<FunctionToInt64>();
	factory.registerFunction<FunctionToFloat32>();
	factory.registerFunction<FunctionToFloat64>();
	factory.registerFunction<FunctionToDate>();
	factory.registerFunction<FunctionToDateTime>();
	factory.registerFunction<FunctionToString>();
	factory.registerFunction<FunctionToFixedString>();
	factory.registerFunction<FunctionToUnixTimestamp>();
	factory.registerFunction<FunctionCast>();
	factory.registerFunction<FunctionToUInt8OrZero>();
	factory.registerFunction<FunctionToUInt16OrZero>();
	factory.registerFunction<FunctionToUInt32OrZero>();
	factory.registerFunction<FunctionToUInt64OrZero>();
	factory.registerFunction<FunctionToInt8OrZero>();
	factory.registerFunction<FunctionToInt16OrZero>();
	factory.registerFunction<FunctionToInt32OrZero>();
	factory.registerFunction<FunctionToInt64OrZero>();
	factory.registerFunction<FunctionToFloat32OrZero>();
	factory.registerFunction<FunctionToFloat64OrZero>();
}

}
