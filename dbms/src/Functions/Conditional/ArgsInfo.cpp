#include <DB/Functions/Conditional/ArgsInfo.h>
#include <DB/Functions/Conditional/CondException.h>
#include <DB/Functions/Conditional/common.h>
#include <DB/Functions/DataTypeTraits.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/WriteHelpers.h>

namespace DB
{

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

	writeString("; ", buf);
	writeString(args[else_arg]->getName(), buf);

	buf.next();

	return out;
}

/// Forward declarations.
template <typename TResult, typename TType>
class ResultDataTypeDeducer;

/// Internal class used by ResultDataTypeDeducer. Calls ResultDataTypeDeducer
/// for the next branch to be processed.
template <typename TType>
class ResultDataTypeDeducerImpl final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		if (! (ResultDataTypeDeducer<TType, DataTypeUInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeUInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeUInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeUInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeFloat32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<TType, DataTypeFloat64>::execute(args, i, type_res)))
			throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};
	}
};

/// Specialization for the error type.
template <>
class ResultDataTypeDeducerImpl<NumberTraits::Error> final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw CondException{CondErrorCodes::TYPE_DEDUCER_UPSCALING_ERROR, dump};
	}
};

/// Analyze the type of the branch currently being processed of a multiIf function.
/// Subsequently perform the same analysis for the remaining branches.
/// Determine the returned type if all the processed branches are numeric.
template <typename TResult, typename TType>
class ResultDataTypeDeducer final
{
private:
	using TCombined = typename DataTypeTraits::DataTypeProduct<TResult, TType>::Type;
	using DataTypeDeducerImpl = ResultDataTypeDeducerImpl<TCombined>;

public:
	static bool execute(const DataTypes & args, size_t i, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		if (typeid_cast<const TType *>(&*args[i]) == nullptr)
			return false;

		if (i == elseArg(args))
		{
			type_res = DataTypeTraits::ToEnrichedDataTypeObject<TCombined, false>::execute();
			if ((type_res.first == DataTypePtr()) && (type_res.second == DataTypePtr()))
				throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};
		}
		else
		{
			i = std::min(nextThen(i), elseArg(args));
			DataTypeDeducerImpl::execute(args, i, type_res);
		}

		return true;
	}
};

/// Analyze the type of each branch (then, else) of a multiIf function.
/// Determine the returned type if all branches are numeric.
class FirstResultDataTypeDeducer final
{
public:
	static void execute(const DataTypes & args, DataTypeTraits::EnrichedDataTypePtr & type_res)
	{
		using Void = typename DataTypeTraits::ToEnrichedDataType<NumberTraits::Enriched::Void>::Type;

		size_t i = firstThen();

		if (! (ResultDataTypeDeducer<Void, DataTypeUInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeUInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeUInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeUInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt8>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt16>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeInt64>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeFloat32>::execute(args, i, type_res)
			|| ResultDataTypeDeducer<Void, DataTypeFloat64>::execute(args, i, type_res)))
			throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};
	}
};

}

DataTypePtr getReturnTypeForArithmeticArgs(const DataTypes & args)
{
	DataTypeTraits::EnrichedDataTypePtr type_res;
	FirstResultDataTypeDeducer::execute(args, type_res);
	return type_res.first;
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
			throw CondException{CondErrorCodes::TYPE_DEDUCER_ILLEGAL_COLUMN_TYPE, toString(i)};

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
