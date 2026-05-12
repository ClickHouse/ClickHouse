#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeFactory.h>

#include <Common/typeid_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace FunctionSignatures
{

class TypeFunctionLeastSupertype : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        DataTypes types;
        types.reserve(args.size());
        for (const Value & arg : args)
            types.emplace_back(arg.type());
        return Value(getLeastSupertype(types));
    }

    std::string name() const override { return "leastSupertype"; }
};

class TypeFunctionArray : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function Array");
        return Value(DataTypePtr(std::make_shared<DataTypeArray>(args.front().type())));
    }

    std::string name() const override { return "Array"; }
};

class TypeFunctionFixedString : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function FixedString");
        return Value(DataTypePtr(std::make_shared<DataTypeFixedString>(args.front().field().safeGet<UInt64>())));
    }

    std::string name() const override { return "FixedString"; }
};

/// Creates DateTime with specified time zone.
class TypeFunctionDateTime : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() > 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function DateTime");
        if (args.empty())
            return Value(DataTypePtr(std::make_shared<DataTypeDateTime>()));
        return Value(DataTypePtr(std::make_shared<DataTypeDateTime>(args.front().field().safeGet<String>())));
    }

    std::string name() const override { return "DateTime"; }
};

class TypeFunctionTypeFromString : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function typeFromString");

        const DataTypeFactory & factory = DataTypeFactory::instance();
        return Value(factory.get(args.front().field().safeGet<String>()));
    }

    std::string name() const override { return "typeFromString"; }
};

class TypeFunctionDifference : public ITypeFunction
{
private:
    /// Call polymorphic lambda with tag argument of concrete field type of src_type.
    template <typename F>
    void dispatchForSourceType(const IDataType & src_type, F && f) const
    {
        WhichDataType which(src_type);

        if (which.isUInt8())
            f(UInt8());
        else if (which.isUInt16())
            f(UInt16());
        else if (which.isUInt32())
            f(UInt32());
        else if (which.isUInt64())
            f(UInt64());
        else if (which.isInt8())
            f(Int8());
        else if (which.isInt16())
            f(Int16());
        else if (which.isInt32())
            f(Int32());
        else if (which.isInt64())
            f(Int64());
        else if (which.isFloat32())
            f(Float32());
        else if (which.isFloat64())
            f(Float64());
        else if (which.isDate())
            f(DataTypeDate::FieldType());
        else if (which.isDateTime())
            f(DataTypeDateTime::FieldType());
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument for function {} must have numeric type.", name());
    }

public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function difference");

        const DataTypePtr & type = args.front().type();

        DataTypePtr res;

        dispatchForSourceType(*type, [&res](auto value)
        {
            res = std::make_shared<DataTypeNumber<typename NumberTraits::ResultOfSubtraction<decltype(value), decltype(value)>::Type>>();
        });

        return Value(res);
    }

    std::string name() const override { return "difference"; }
};

/// If the type was already Nullable, return it as is.
class TypeFunctionNullable : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function Nullable");

        return Value(makeNullable(args.front().type()));
    }

    std::string name() const override { return "Nullable"; }
};

class TypeFunctionLowCardinality : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function LowCardinality");

        const DataTypePtr & type = args.front().type();
        if (type->lowCardinality())
            return Value(type);
        else
            return Value(DataTypePtr(std::make_shared<DataTypeLowCardinality>(type)));
    }

    std::string name() const override { return "LowCardinality"; }
};


class TypeFunctionTuplesHaveSameSize : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() <= 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function tuplesHaveSameSize");

        std::optional<size_t> tuple_size;

        for (const auto & arg : args)
        {
            const DataTypePtr & type = arg.type();
            if (!isTuple(type))
                return Value(Field(UInt64(0)));

            const DataTypeTuple & tuple = typeid_cast<const DataTypeTuple &>(*type);
            if (!tuple_size)
                tuple_size = tuple.getElements().size();
            else if (*tuple_size != tuple.getElements().size())
                return Value(Field(UInt64(0)));
        }

        return Value(Field(UInt64(1)));
    }

    std::string name() const override { return "tuplesHaveSameSize"; }
};


void registerTypeFunctions()
{
    auto & factory = TypeFunctionFactory::instance();

    factory.registerElement<TypeFunctionLeastSupertype>();
    factory.registerElement<TypeFunctionArray>();
    factory.registerElement<TypeFunctionFixedString>();
    factory.registerElement<TypeFunctionDateTime>();
    factory.registerElement<TypeFunctionDifference>();
    factory.registerElement<TypeFunctionTypeFromString>();
    factory.registerElement<TypeFunctionNullable>();
    factory.registerElement<TypeFunctionLowCardinality>();

    /// Predicates.
    factory.registerElement<TypeFunctionTuplesHaveSameSize>();
}

}
}
