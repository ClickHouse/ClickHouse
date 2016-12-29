#include <DB/Functions/FunctionsArray.h>
#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsString.h>

namespace DB
{

String FunctionReverse::getName() const
{
	return name;
}


DataTypePtr FunctionReverse::getReturnTypeImpl(const DataTypes & arguments) const
{
	if (!typeid_cast<const DataTypeString *>(&*arguments[0]) && !typeid_cast<const DataTypeFixedString *>(&*arguments[0])
		&& !typeid_cast<const DataTypeArray *>(&*arguments[0]))
		throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

	return arguments[0]->clone();
}


void FunctionReverse::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
	const ColumnPtr column = block.getByPosition(arguments[0]).column;
	if (const ColumnString * col = typeid_cast<const ColumnString *>(column.get()))
	{
		std::shared_ptr<ColumnString> col_res = std::make_shared<ColumnString>();
		block.getByPosition(result).column = col_res;
		ReverseImpl::vector(col->getChars(), col->getOffsets(),
						col_res->getChars(), col_res->getOffsets());
	}
	else if (const ColumnFixedString * col = typeid_cast<const ColumnFixedString *>(column.get()))
	{
		auto col_res = std::make_shared<ColumnFixedString>(col->getN());
		block.getByPosition(result).column = col_res;
		ReverseImpl::vector_fixed(col->getChars(), col->getN(),
							col_res->getChars());
	}
	else if (const ColumnConstString * col = typeid_cast<const ColumnConstString *>(column.get()))
	{
		String res;
		ReverseImpl::constant(col->getData(), res);
		auto col_res = std::make_shared<ColumnConstString>(col->size(), res);
		block.getByPosition(result).column = col_res;
	}
	else if (typeid_cast<const ColumnArray *>(column.get()) || typeid_cast<const ColumnConstArray *>(column.get()))
	{
		FunctionArrayReverse().execute(block, arguments, result);
	}
	else
		throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN);
}



String FunctionAppendTrailingCharIfAbsent::getName() const
{
	return name;
}


DataTypePtr FunctionAppendTrailingCharIfAbsent::getReturnTypeImpl(const DataTypes & arguments) const
{
	if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
		throw Exception{
			"Illegal type " + arguments[0]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
		};

	if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
		throw Exception{
			"Illegal type " + arguments[1]->getName() + " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
		};

	return std::make_shared<DataTypeString>();
}


void FunctionAppendTrailingCharIfAbsent::executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result)
{
	const auto & column = block.getByPosition(arguments[0]).column;
	const auto & column_char = block.getByPosition(arguments[1]).column;

	if (!typeid_cast<const ColumnConstString *>(column_char.get()))
		throw Exception{
			"Second argument of function " + getName() + " must be a constant string",
			ErrorCodes::ILLEGAL_COLUMN
		};

	const auto & trailing_char_str = static_cast<const ColumnConstString &>(*column_char).getData();

	if (trailing_char_str.size() != 1)
		throw Exception{
			"Second argument of function " + getName() + " must be a one-character string",
			ErrorCodes::BAD_ARGUMENTS
		};

	if (const auto col = typeid_cast<const ColumnString *>(&*column))
	{
		auto col_res = std::make_shared<ColumnString>();
		block.getByPosition(result).column = col_res;

		const auto & src_data = col->getChars();
		const auto & src_offsets = col->getOffsets();

		auto & dst_data = col_res->getChars();
		auto & dst_offsets = col_res->getOffsets();

		const auto size = src_offsets.size();
		dst_data.resize(src_data.size() + size);
		dst_offsets.resize(size);

		ColumnString::Offset_t src_offset{};
		ColumnString::Offset_t dst_offset{};

		for (const auto i : ext::range(0, size))
		{
			const auto src_length = src_offsets[i] - src_offset;
			memcpySmallAllowReadWriteOverflow15(&dst_data[dst_offset], &src_data[src_offset], src_length);
			src_offset = src_offsets[i];
			dst_offset += src_length;

			if (src_length > 1 && dst_data[dst_offset - 2] != trailing_char_str.front())
			{
				dst_data[dst_offset - 1] = trailing_char_str.front();
				dst_data[dst_offset] = 0;
				++dst_offset;
			}

			dst_offsets[i] = dst_offset;
		}

		dst_data.resize_assume_reserved(dst_offset);
	}
	else if (const auto col = typeid_cast<const ColumnConstString *>(&*column))
	{
		const auto & in_data = col->getData();

		block.getByPosition(result).column = std::make_shared<ColumnConstString>(
			col->size(),
			in_data.size() == 0 ? in_data :
				in_data.back() == trailing_char_str.front() ? in_data : in_data + trailing_char_str);
	}
	else
		throw Exception{
			"Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of argument of function " + getName(),
			ErrorCodes::ILLEGAL_COLUMN
		};
}


void registerFunctionsString(FunctionFactory & factory)
{
	factory.registerFunction<FunctionEmpty>();
	factory.registerFunction<FunctionNotEmpty>();
	factory.registerFunction<FunctionLength>();
	factory.registerFunction<FunctionLengthUTF8>();
	factory.registerFunction<FunctionLower>();
	factory.registerFunction<FunctionUpper>();
	factory.registerFunction<FunctionLowerUTF8>();
	factory.registerFunction<FunctionUpperUTF8>();
	factory.registerFunction<FunctionReverse>();
	factory.registerFunction<FunctionReverseUTF8>();
	factory.registerFunction<FunctionConcat>();
	factory.registerFunction<FunctionConcatAssumeInjective>();
	factory.registerFunction<FunctionSubstring>();
	factory.registerFunction<FunctionSubstringUTF8>();
	factory.registerFunction<FunctionAppendTrailingCharIfAbsent>();
}

}
