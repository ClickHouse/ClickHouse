#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>

#include <Common/typeid_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace FunctionSignatures
{

class TypeMatcherUnsignedInteger : public ITypeMatcher
{
public:
    std::string toString() const override { return "UnsignedInteger"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isUInt(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherInteger : public ITypeMatcher
{
public:
    std::string toString() const override { return "Integer"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isInteger(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNumber : public ITypeMatcher
{
public:
    std::string toString() const override { return "Number"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isNumber(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNativeUInt : public ITypeMatcher
{
public:
    std::string toString() const override { return "NativeUInt"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isNativeUInt(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherStringOrFixedString : public ITypeMatcher
{
public:
    std::string toString() const override { return "StringOrFixedString"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isStringOrFixedString(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherEnum : public ITypeMatcher
{
public:
    std::string toString() const override { return "Enum"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isEnum(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNULL : public ITypeMatcher
{
public:
    std::string toString() const override { return "NULL"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return type->onlyNull(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherRepresentedByNumber : public ITypeMatcher
{
public:
    std::string toString() const override { return "RepresentedByNumber"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return type->isValueRepresentedByNumber(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherSet : public ITypeMatcher
{
public:
    std::string toString() const override { return "Set"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return typeid_cast<const DataTypeSet *>(type.get()) != nullptr; }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherUnambiguouslyRepresentedInContiguousMemoryRegion : public ITypeMatcher
{
public:
    std::string toString() const override { return "UnambiguouslyRepresentedInContiguousMemoryRegion"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override
    {
        return type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion();
    }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDateOrDateTime : public ITypeMatcher
{
public:
    std::string toString() const override { return "DateOrDateTime"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override
    {
        WhichDataType which(type);
        return which.isDateOrDate32() || which.isDateTime() || which.isDateTime64();
    }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherAny : public ITypeMatcher
{
public:
    std::string toString() const override { return "Any"; }
    bool match(const DataTypePtr &, Variables &, size_t, size_t, std::string &) const override { return true; }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherFloat : public ITypeMatcher
{
public:
    std::string toString() const override { return "Float"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isFloat(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNativeNumber : public ITypeMatcher
{
public:
    std::string toString() const override { return "NativeNumber"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isNativeNumber(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNativeNumberOrDecimal : public ITypeMatcher
{
public:
    std::string toString() const override { return "NativeNumberOrDecimal"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isNativeNumber(type) || isDecimal(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDecimal : public ITypeMatcher
{
public:
    std::string toString() const override { return "Decimal"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isDecimal(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherUUID : public ITypeMatcher
{
public:
    std::string toString() const override { return "UUID"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isUUID(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherIPv4 : public ITypeMatcher
{
public:
    std::string toString() const override { return "IPv4"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isIPv4(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherIPv6 : public ITypeMatcher
{
public:
    std::string toString() const override { return "IPv6"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isIPv6(); }
    size_t getIndex() const override { return 0; }
};

/// String, FixedString, Array, Map, UUID, IPv4 or IPv6 — anything for which a byte/element length is well-defined.
class TypeMatcherStringArrayMapIP : public ITypeMatcher
{
public:
    std::string toString() const override { return "StringArrayMapIP"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override
    {
        WhichDataType which(type);
        return isStringOrFixedString(type) || isArray(type) || isMap(type)
            || which.isUUID() || which.isIPv4() || which.isIPv6();
    }
    size_t getIndex() const override { return 0; }
};


class TypeMatcherArray : public ITypeMatcher
{
private:
    TypeMatcherPtr child_matcher;
public:
    explicit TypeMatcherArray(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() > 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Array type matcher cannot have more than one argument");

        if (child_matchers.size() == 1)
            child_matcher = child_matchers[0];
    }

    std::string toString() const override { return child_matcher ? "Array(" + child_matcher->toString() + ")" : "Array"; }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        if (!isArray(type))
        {
            out_reason = "type " + type->getName() + " is not an Array";
            return false;
        }

        if (child_matcher)
        {
            const DataTypeArray & arr = typeid_cast<const DataTypeArray &>(*type);
            if (child_matcher->match(arr.getNestedType(), variables, iteration, arg_num, out_reason))
                return true;
            out_reason = "nested type of Array doesn't match " + child_matcher->toString() + (out_reason.empty() ? "" : ": " + out_reason);
            return false;
        }
        return true;
    }

    size_t getIndex() const override
    {
        return child_matcher ? child_matcher->getIndex() : 0;
    }
};

class TypeMatcherTuple : public ITypeMatcher
{
private:
    TypeMatchers child_matchers;
public:
    explicit TypeMatcherTuple(const TypeMatchers & child_matchers_) : child_matchers(child_matchers_) {}

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        out << "Tuple(";
        writeList(child_matchers, [&](const auto & elem){ out << elem->toString(); }, [&]{ out << ", "; });
        out << ")";
        return out.str();
    }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        if (!isTuple(type))
        {
            out_reason = "type " + type->getName() + " is not a Tuple";
            return false;
        }

        if (child_matchers.empty())
            return true;

        const DataTypes & nested_types = static_cast<const DataTypeTuple &>(*type).getElements();

        size_t size = child_matchers.size();
        if (size != nested_types.size())
        {
            out_reason = "wrong number of tuple elements (" + DB::toString(nested_types.size()) + "), expected " + DB::toString(child_matchers.size());
            return false;
        }

        for (size_t i = 0; i < size; ++i)
        {
            if (!child_matchers[i]->match(nested_types[i], variables, iteration, arg_num, out_reason))
            {
                out_reason = "element " + DB::toString(i) + " of tuple doesn't match" + (out_reason.empty() ? "" : ": " + out_reason);
                return false;
            }
        }

        return true;
    }

    size_t getIndex() const override
    {
        size_t res = 0;
        for (const auto & child : child_matchers)
            res = getCommonIndex(res, child->getIndex());
        return res;
    }
};

class TypeMatcherMaybeNullable : public ITypeMatcher
{
private:
    TypeMatcherPtr child_matcher;
public:
    explicit TypeMatcherMaybeNullable(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "MaybeNullable type matcher requires single argument");

        child_matcher = child_matchers[0];
    }

    std::string toString() const override { return "MaybeNullable(" + child_matcher->toString() + ")"; }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        return child_matcher->match(removeNullable(type), variables, iteration, arg_num, out_reason);
    }

    size_t getIndex() const override
    {
        return child_matcher->getIndex();
    }
};


template <typename TypeMatcher>
void registerTypeMatcherWithNoArguments(TypeMatcherFactory & factory)
{
    auto elem = std::make_shared<TypeMatcher>();
    auto name = elem->toString();
    factory.registerElement(name,
        [captured = std::move(elem), name](const TypeMatchers & children)
        {
            if (!children.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "{} type matcher cannot have arguments", name);
            return captured;
        });
}


void registerTypeMatchers()
{
    auto & factory = TypeMatcherFactory::instance();

    registerTypeMatcherWithNoArguments<TypeMatcherUnsignedInteger>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherInteger>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeUInt>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherStringOrFixedString>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherEnum>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNULL>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherRepresentedByNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherSet>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDateOrDateTime>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherUnambiguouslyRepresentedInContiguousMemoryRegion>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherAny>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherFloat>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeNumberOrDecimal>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDecimal>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherUUID>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherIPv4>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherIPv6>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherStringArrayMapIP>(factory);

    factory.registerElement("Array", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherArray>(children); });
    factory.registerElement("Tuple", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherTuple>(children); });
    factory.registerElement("MaybeNullable", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherMaybeNullable>(children); });
}

}
}
