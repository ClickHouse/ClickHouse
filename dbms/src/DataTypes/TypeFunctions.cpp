#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

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

void registerTypeFunctions()
{
    auto & factory = TypeFunctionFactory::instance();

    factory.registerElement<TypeFunctionLeastSupertype>();
    factory.registerElement<TypeFunctionArray>();
    factory.registerElement<TypeFunctionFixedString>();
}

}
}
