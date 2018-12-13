#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeFactory.h>

#include <Common/typeid_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{
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
        return getLeastSupertype(types);
    }

    std::string name() const override { return "leastSupertype"; }
};

class TypeFunctionArray : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception("Wrong number of arguments for type function Array", ErrorCodes::LOGICAL_ERROR);
        return DataTypePtr(std::make_shared<DataTypeArray>(args.front().type()));
    }

    std::string name() const override { return "Array"; }
};

class TypeFunctionFixedString : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception("Wrong number of arguments for type function FixedString", ErrorCodes::LOGICAL_ERROR);
        return DataTypePtr(std::make_shared<DataTypeFixedString>(args.front().field().safeGet<UInt64>()));
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
            throw Exception("Wrong number of arguments for type function DateTime", ErrorCodes::LOGICAL_ERROR);
        if (args.empty())
            return DataTypePtr(std::make_shared<DataTypeDateTime>());
        return DataTypePtr(std::make_shared<DataTypeDateTime>(args.front().field().safeGet<String>()));
    }

    std::string name() const override { return "DateTime"; }
};

class TypeFunctionTypeFromString : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception("Wrong number of arguments for type function TypeFromString", ErrorCodes::LOGICAL_ERROR);

        const DataTypeFactory & factory = DataTypeFactory::instance();

        return factory.get(args.front().field().safeGet<String>());
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
            throw Exception("Argument for function " + name() + " must have numeric type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception("Wrong number of arguments for type function difference", ErrorCodes::LOGICAL_ERROR);

        const DataTypePtr & type = args.front().type();

        DataTypePtr res;

        dispatchForSourceType(*type, [&res](auto value)
        {
            res = std::make_shared<DataTypeNumber<typename NumberTraits::ResultOfSubtraction<decltype(value), decltype(value)>::Type>>();
        });

        return res;
    }

    std::string name() const override { return "difference"; }
};


class TypeFunctionTuplesHaveSameSize : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception("Wrong number of arguments for type function TypeFromString", ErrorCodes::LOGICAL_ERROR);

        const DataTypeFactory & factory = DataTypeFactory::instance();

        return factory.get(args.front().field().safeGet<String>());
    }

    std::string name() const override { return "typeFromString"; }
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
}

}
}
