#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
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

class TypeFunctionTuple : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function Tuple requires at least one element");

        DataTypes elems;
        Strings names;
        elems.reserve(args.size());
        names.reserve(args.size());
        bool any_named = false;
        for (const Value & arg : args)
        {
            elems.emplace_back(arg.type());
            names.emplace_back(arg.name);
            if (!arg.name.empty())
                any_named = true;
        }

        if (any_named)
        {
            /// All elements must be named (mixing named and unnamed is not a valid Tuple).
            for (size_t i = 0; i < args.size(); ++i)
                if (names[i].empty())
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Tuple element {} has no name but other elements are named — "
                        "either all elements must be wrapped in NamedField or none", i + 1);
            return Value(DataTypePtr(std::make_shared<DataTypeTuple>(elems, names)));
        }
        return Value(DataTypePtr(std::make_shared<DataTypeTuple>(elems)));
    }

    std::string name() const override { return "Tuple"; }
};

/// Attach a name to a type. Only meaningful inside the Tuple type-function — flows through
/// the Value::name field. Syntax: `NamedField('field_name', Type)`.
class TypeFunctionNamedField : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "NamedField requires exactly 2 arguments (name, type)");
        Value res = args[1];
        res.name = args[0].field().safeGet<String>();
        return res;
    }

    std::string name() const override { return "NamedField"; }
};

class TypeFunctionMap : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function Map requires exactly 2 arguments (key, value)");
        return Value(DataTypePtr(std::make_shared<DataTypeMap>(args[0].type(), args[1].type())));
    }

    std::string name() const override { return "Map"; }
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

/// Creates DateTime64 with specified scale and optional time zone.
class TypeFunctionDateTime64 : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.empty() || args.size() > 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function DateTime64");
        const UInt64 scale = args[0].field().safeGet<UInt64>();
        if (args.size() == 1)
            return Value(DataTypePtr(std::make_shared<DataTypeDateTime64>(scale)));
        return Value(DataTypePtr(std::make_shared<DataTypeDateTime64>(scale, args[1].field().safeGet<String>())));
    }

    std::string name() const override { return "DateTime64"; }
};

/// Creates Time64 with specified scale.
class TypeFunctionTime64 : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function Time64");
        return Value(DataTypePtr(std::make_shared<DataTypeTime64>(args.front().field().safeGet<UInt64>())));
    }

    std::string name() const override { return "Time64"; }
};

/// Extracts the scale of a DateTime64 / Time64, or DataTypeDateTime64::default_scale if the
/// argument has no scale (Date, Date32, DateTime, ...).
class TypeFunctionScaleOf : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function scaleOf");
        const DataTypePtr & type = args.front().type();
        if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(type.get()))
            return Value(Field(static_cast<UInt64>(dt64->getScale())));
        if (const auto * t64 = typeid_cast<const DataTypeTime64 *>(type.get()))
            return Value(Field(static_cast<UInt64>(t64->getScale())));
        return Value(Field(static_cast<UInt64>(DataTypeDateTime64::default_scale)));
    }

    std::string name() const override { return "scaleOf"; }
};

/// max(a, b) over UInt64 constants.
class TypeFunctionMax : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function max");
        const UInt64 a = args[0].field().safeGet<UInt64>();
        const UInt64 b = args[1].field().safeGet<UInt64>();
        return Value(Field(std::max(a, b)));
    }

    std::string name() const override { return "max"; }
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
    factory.registerElement<TypeFunctionTuple>();
    factory.registerElement<TypeFunctionNamedField>();
    factory.registerElement<TypeFunctionMap>();
    factory.registerElement<TypeFunctionFixedString>();
    factory.registerElement<TypeFunctionDateTime>();
    factory.registerElement<TypeFunctionDateTime64>();
    factory.registerElement<TypeFunctionTime64>();
    factory.registerElement<TypeFunctionScaleOf>();
    factory.registerElement<TypeFunctionMax>();
    factory.registerElement<TypeFunctionDifference>();
    factory.registerElement<TypeFunctionTypeFromString>();
    factory.registerElement<TypeFunctionNullable>();
    factory.registerElement<TypeFunctionLowCardinality>();

    /// Predicates.
    factory.registerElement<TypeFunctionTuplesHaveSameSize>();
}

}
}
