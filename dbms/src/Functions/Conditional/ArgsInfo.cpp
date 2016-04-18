#include <DB/Functions/Conditional/ArgsInfo.h>
#include <DB/Functions/Conditional/common.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/Functions/NumberTraits.h>
#include <DB/Functions/DataTypeFromFieldTypeOrError.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{

extern const int ILLEGAL_TYPE_OF_ARGUMENT;

}

namespace Conditional
{

namespace
{

std::string dumpArgTypes(const DataTypes & args)
{
	std::string out;
	WriteBufferFromString buf{out};

	size_t else_arg = elseArg(args);

	bool is_first = true;
	for (size_t i = firstThen(); i < else_arg; i = nextThen(i))
	{
		if (is_first)
			is_first = false;
		else
			writeString("; ", buf);

		writeString(args[i]->getName(), buf);
	}

	writeString(args[else_arg]->getName(), buf);

	buf.next();

	return out;
}

template <typename TResult, typename TType>
struct ResultTypeChecker;

template <typename TType>
struct ResultTypeCheckerImpl final
{
	template <typename U>
	using ConcreteChecker = ResultTypeChecker<typename DataTypeFromFieldType<TType>::Type, U>;

	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		if (DataTypeDispatcher<ConcreteChecker>::apply(args, i, type_res))
			return true;
		else
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

template <>
struct ResultTypeCheckerImpl<typename NumberTraits::Error>
{
	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Analyze the type of each branch (then, else) of a multiIf function.
/// Determine the returned type if all branches are numeric.
template <typename TResult, typename TType>
struct ResultTypeChecker final
{
	using TCombinedResult = typename NumberTraits::ResultOfIf<typename TResult::FieldType,
		typename TType::FieldType>::Type;
	using CheckerImpl = ResultTypeCheckerImpl<TCombinedResult>;

	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		if (typeid_cast<const TType *>(&*args[i]) == nullptr)
			return false;

		DataTypePtr res = DataTypeFromFieldTypeOrError<TCombinedResult>::getDataType();
		if (!res)
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

		if (i == elseArg(args))
		{
			type_res = res;
			return true;
		}

		i = std::min(nextThen(i), elseArg(args));

		if (CheckerImpl::execute(args, i, type_res))
			return true;
		else
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Process the first Then.
template <typename TType>
struct FirstResultTypeChecker final
{
	template <typename U>
	using ConcreteChecker = ResultTypeChecker<TType, U>;

	static bool execute(const DataTypes & args, DataTypePtr & type_res)
	{
		size_t i = firstThen();

		if (typeid_cast<const TType *>(&*args[i]) == nullptr)
			return false;

		i = std::min(nextThen(i), elseArg(args));

		if (DataTypeDispatcher<ConcreteChecker>::apply(args, i, type_res))
			return true;
		else
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

}

DataTypePtr getReturnTypeForArithmeticArgs(const DataTypes & args)
{
	DataTypePtr type_res;
	if (!DataTypeDispatcher<FirstResultTypeChecker>::apply(args, type_res))
		throw Exception{"Illegal type of column 1 of function multiIf",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	return type_res;
}

bool hasArithmeticBranches(const DataTypes & args)
{
	size_t else_arg = elseArg(args);

	auto check = [&](size_t i)
	{
		return args[i]->behavesAsNumber();
	};

	for (size_t i = firstThen(); i < else_arg; i = nextThen(i))
	{
		if (!check(i))
			return false;
	}

	return check(else_arg);
}

bool hasArrayBranches(const DataTypes & args)
{
	size_t else_arg = elseArg(args);

	auto check = [&](size_t i)
	{
		return typeid_cast<const DataTypeArray *>(args[i].get()) != nullptr;
	};

	for (size_t i = firstThen(); i < elseArg(args); i = nextThen(i))
	{
		if (!check(i))
			return false;
	}

	return check(else_arg);
}

bool hasIdenticalTypes(const DataTypes & args)
{
	size_t else_arg = elseArg(args);
	auto first_type_name = args[firstThen()]->getName();

	auto check = [&](size_t i)
	{
		return args[i]->getName() == first_type_name;
	};

	for (size_t i = secondThen(); i < elseArg(args); i = nextThen(i))
	{
		if (!check(i))
			return false;
	}

	return check(else_arg);
}

bool hasFixedStrings(const DataTypes & args)
{
	size_t else_arg = elseArg(args);

	auto check = [&](size_t i)
	{
		return typeid_cast<const DataTypeFixedString *>(args[i].get()) != nullptr;
	};

	for (size_t i = firstThen(); i < elseArg(args); i = nextThen(i))
	{
		if (!check(i))
			return false;
	}

	return check(else_arg);
}

bool hasFixedStringsOfIdenticalLength(const DataTypes & args)
{
	size_t else_arg = elseArg(args);

	auto get_length = [&](size_t i)
	{
		auto fixed_str = typeid_cast<const DataTypeFixedString *>(args[i].get());
		if (fixed_str == nullptr)
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

		return fixed_str->getN();
	};

	auto first_length = get_length(firstThen());

	for (size_t i = secondThen(); i < elseArg(args); i = nextThen(i))
	{
		if (get_length(i) != first_length)
			return false;
	}

	return get_length(else_arg) == first_length;
}

bool hasStrings(const DataTypes & args)
{
	size_t else_arg = elseArg(args);

	auto check = [&](size_t i)
	{
		return (typeid_cast<const DataTypeFixedString *>(args[i].get()) != nullptr) ||
			(typeid_cast<const DataTypeString *>(args[i].get()) != nullptr);
	};

	for (size_t i = firstThen(); i < elseArg(args); i = nextThen(i))
	{
		if (!check(i))
			return false;
	}

	return check(else_arg);
}

}

}
