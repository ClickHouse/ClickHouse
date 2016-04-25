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

	writeString("; ", buf);
	writeString(args[else_arg]->getName(), buf);

	buf.next();

	return out;
}

template <typename TType>
struct ExtendedDataType
{
	using FieldType = typename TType::FieldType;
};

template <>
struct ExtendedDataType<void>
{
	using FieldType = void;
};

/// Forward declarations.
template <typename TFloat, typename TUInt, typename TInt, typename TType>
struct ResultDataTypeDeducer;

template <typename TFloat, typename TUInt, typename TInt>
struct ResultDataTypeDeducerImpl;

template <typename TFloat, typename TUInt, typename TInt>
struct TypeComposer;

template <typename TType, typename TInt>
struct TypeComposerImpl;

/// Analyze the type of each branch (then, else) of a multiIf function.
/// Determine the returned type if all branches are numeric.
class FirstResultDataTypeDeducer final
{
private:
	template <typename U>
	using ConcreteDataTypeDeducer = ResultDataTypeDeducer<void, void, void, U>;

public:
	static void execute(const DataTypes & args, DataTypePtr & type_res)
	{
		size_t i = firstThen();

		if (!DataTypeDispatcher<ConcreteDataTypeDeducer>::apply(args, i, type_res))
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Analyze the type of the branch currently being processed of a multiIf function.
/// Subsequently perform the same analysis for the remaining branches.
/// Determine the returned type if all the processed branches are numeric.
template <typename TFloat, typename TUInt, typename TInt, typename TType>
class ResultDataTypeDeducer final
{
private:
	using TCombinedFloat = typename std::conditional<
		std::is_floating_point<typename ExtendedDataType<TType>::FieldType>::value,
		typename NumberTraits::ResultOfIf<
			typename ExtendedDataType<TFloat>::FieldType,
			typename ExtendedDataType<TType>::FieldType
		>::Type,
		typename ExtendedDataType<TFloat>::FieldType
	>::type;

	using TCombinedUInt = typename std::conditional<
		std::is_unsigned<typename ExtendedDataType<TType>::FieldType>::value,
		typename NumberTraits::ResultOfIf<
			typename ExtendedDataType<TUInt>::FieldType,
			typename ExtendedDataType<TType>::FieldType
		>::Type,
		typename ExtendedDataType<TUInt>::FieldType
	>::type;

	using TCombinedInt = typename std::conditional<
		std::is_signed<typename ExtendedDataType<TType>::FieldType>::value,
		typename NumberTraits::ResultOfIf<
			typename ExtendedDataType<TInt>::FieldType,
			typename ExtendedDataType<TType>::FieldType
		>::Type,
		typename ExtendedDataType<TInt>::FieldType
	>::type;

	using ConcreteComposer = TypeComposer<TCombinedFloat, TCombinedUInt, TCombinedInt>;
	using DataTypeDeducerImpl = ResultDataTypeDeducerImpl<TCombinedFloat, TCombinedUInt, TCombinedInt>;

public:
	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		if (typeid_cast<const TType *>(&*args[i]) == nullptr)
			return false;

		if (i == elseArg(args))
		{
			ConcreteComposer::execute(args, i, type_res);
			return true;
		}

		i = std::min(nextThen(i), elseArg(args));

		if (DataTypeDeducerImpl::execute(args, i, type_res))
			return true;
		else
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Internal class used by ResultDataTypeDeducer. From the deduced triplet
/// of numeric types (float, unsigned int, signed int), determine the
/// associated triplet of extended data types, then call ResultDataTypeDeducer
/// for the next branch to being processed.
template <typename TFloat, typename TUInt, typename TInt>
class ResultDataTypeDeducerImpl final
{
private:
	using ExtendedDataTypeFloat = typename std::conditional<
		std::is_void<TFloat>::value,
		void,
		typename DataTypeFromFieldType<TFloat>::Type
	>::type;

	using ExtendedDataTypeUInt = typename std::conditional<
		std::is_void<TUInt>::value,
		void,
		typename DataTypeFromFieldType<TUInt>::Type
	>::type;

	using ExtendedDataTypeInt = typename std::conditional<
		std::is_void<TInt>::value,
		void,
		typename DataTypeFromFieldType<TInt>::Type
	>::type;

	template <typename U>
	using ConcreteDataTypeDeducer = ResultDataTypeDeducer<
		ExtendedDataTypeFloat,
		ExtendedDataTypeUInt,
		ExtendedDataTypeInt,
		U
	>;

public:
	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		if (DataTypeDispatcher<ConcreteDataTypeDeducer>::apply(args, i, type_res))
			return true;
		else
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type deduction error.
template <typename TFloat, typename TUInt>
class ResultDataTypeDeducerImpl<TFloat, TUInt, typename NumberTraits::Error> final
{
public:
	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type deduction error.
template <typename TFloat, typename TInt>
class ResultDataTypeDeducerImpl<TFloat, typename NumberTraits::Error, TInt> final
{
public:
	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type deduction error.
template <typename TUInt, typename TInt>
class ResultDataTypeDeducerImpl<typename NumberTraits::Error, TUInt, TInt> final
{
public:
	static bool execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Compose a float type, an unsigned int type, and a signed int type.
/// Return the deduced type.
template <typename TFloat, typename TUInt, typename TInt>
class TypeComposer final
{
private:
	using TCombined = typename NumberTraits::ResultOfIf<TFloat, TUInt>::Type;

public:
	static void execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		TypeComposerImpl<TCombined, TInt>::execute(args, i, type_res);
	}
};

template <typename TType, typename TInt>
class TypeComposerImpl final
{
private:
	using TCombined = typename NumberTraits::ResultOfIf<TType, TInt>::Type;

public:
	static void execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		DataTypePtr res = DataTypeFromFieldTypeOrError<TCombined>::getDataType();
		if (!res)
			throw Exception{"Illegal type of column " + toString(i) +
				" of function multiIf", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
		type_res = res;
	}
};

/// Specialization for type composition error.
template <typename TInt>
class TypeComposerImpl<typename NumberTraits::Error, TInt> final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type composition error.
template <typename TType>
class TypeComposerImpl<TType, typename NumberTraits::Error> final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

/// Specialization for type composition error.
template <>
class TypeComposerImpl<typename NumberTraits::Error, typename NumberTraits::Error> final
{
public:
	static void execute(const DataTypes & args, size_t i, DataTypePtr & type_res)
	{
		std::string dump = dumpArgTypes(args);
		throw Exception{"Arguments of function multiIf are not upscalable to a "
			"common type without loss of precision: " + dump,
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
	}
};

}

DataTypePtr getReturnTypeForArithmeticArgs(const DataTypes & args)
{
	DataTypePtr type_res;
	FirstResultDataTypeDeducer::execute(args, type_res);
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
