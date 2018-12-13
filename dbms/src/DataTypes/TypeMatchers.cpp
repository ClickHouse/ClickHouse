#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>

#include <Common/typeid_cast.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{
namespace FunctionSignatures
{

class TypeMatcherUnsignedInteger : public ITypeMatcher
{
public:
    std::string toString() const override { return "UnsignedInteger"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isUnsignedInteger(type); }
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


class TypeMatcherArray : public ITypeMatcher
{
private:
    TypeMatcherPtr child_matcher;
public:
    TypeMatcherArray(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() > 1)
            throw Exception("Array type matcher cannot have more than one argument", ErrorCodes::LOGICAL_ERROR);

        if (child_matchers.size() == 1)
            child_matcher = child_matchers[0];
    }

    std::string toString() const override { return "Array(" + child_matcher->toString() + ")"; }

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
        else
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
    TypeMatcherTuple(const TypeMatchers & child_matchers) : child_matchers(child_matchers) {}

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
    TypeMatcherMaybeNullable(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() != 1)
            throw Exception("MaybeNullable type matcher requires single argument", ErrorCodes::LOGICAL_ERROR);

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
        [elem = std::move(elem), name](const TypeMatchers & children)
        {
            if (!children.empty())
                throw Exception(name + " type matcher cannot have arguments", ErrorCodes::LOGICAL_ERROR);
            return elem;
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

    factory.registerElement("Array", [](const TypeMatchers & children) { return std::make_shared<TypeMatcherArray>(children); });
    factory.registerElement("Tuple", [](const TypeMatchers & children) { return std::make_shared<TypeMatcherTuple>(children); });
    factory.registerElement("MaybeNullable", [](const TypeMatchers & children) { return std::make_shared<TypeMatcherMaybeNullable>(children); });
}

}
}
