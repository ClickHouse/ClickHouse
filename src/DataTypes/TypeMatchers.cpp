#include <DataTypes/FunctionSignature.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeFunction.h>
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

class TypeMatcherUInt : public ITypeMatcher
{
public:
    std::string toString() const override { return "UInt"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isUInt(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherInt : public ITypeMatcher
{
public:
    std::string toString() const override { return "Int"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isInt(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNativeInteger : public ITypeMatcher
{
public:
    std::string toString() const override { return "NativeInteger"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isNativeInteger(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherNativeInt : public ITypeMatcher
{
public:
    std::string toString() const override { return "NativeInt"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isNativeInt(); }
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

class TypeMatcherEnum8 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Enum8"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isEnum8(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherEnum16 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Enum16"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isEnum16(); }
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

class TypeMatcherNativeFloat : public ITypeMatcher
{
public:
    std::string toString() const override { return "NativeFloat"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isNativeFloat(); }
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

class TypeMatcherDecimal : public ITypeMatcher
{
public:
    std::string toString() const override { return "Decimal"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isDecimal(type); }
    size_t getIndex() const override { return 0; }
};

/// Matches any type whose value is internally stored as a number — used by hashing functions
/// that accept Number, Date, DateTime, IPv4, IPv6, Decimal, UUID, Bool, Enum, etc.
class TypeMatcherValueRepresentedByNumber : public ITypeMatcher
{
public:
    std::string toString() const override { return "NumberRepresentable"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return type->isValueRepresentedByNumber(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDecimal32 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Decimal32"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDecimal32(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDecimal64 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Decimal64"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDecimal64(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDecimal128 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Decimal128"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDecimal128(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDecimal256 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Decimal256"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDecimal256(); }
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

class TypeMatcherMap : public ITypeMatcher
{
public:
    std::string toString() const override { return "Map"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isMap(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherMapOf : public ITypeMatcher
{
private:
    TypeMatcherPtr key_matcher;
    TypeMatcherPtr value_matcher;
public:
    explicit TypeMatcherMapOf(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Map type matcher requires exactly two arguments (key and value)");
        key_matcher = child_matchers[0];
        value_matcher = child_matchers[1];
    }

    std::string toString() const override { return "Map(" + key_matcher->toString() + ", " + value_matcher->toString() + ")"; }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        if (!isMap(type))
        {
            out_reason = "expected Map, got " + type->getName();
            return false;
        }
        const auto & map_type = static_cast<const DataTypeMap &>(*type);
        if (!key_matcher->match(map_type.getKeyType(), variables, iteration, arg_num, out_reason))
        {
            out_reason = "Map key type doesn't match " + key_matcher->toString() + (out_reason.empty() ? "" : ": " + out_reason);
            return false;
        }
        if (!value_matcher->match(map_type.getValueType(), variables, iteration, arg_num, out_reason))
        {
            out_reason = "Map value type doesn't match " + value_matcher->toString() + (out_reason.empty() ? "" : ": " + out_reason);
            return false;
        }
        return true;
    }

    size_t getIndex() const override { return getCommonIndex(key_matcher->getIndex(), value_matcher->getIndex()); }
};

class TypeMatcherString : public ITypeMatcher
{
public:
    std::string toString() const override { return "String"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isString(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherFixedString : public ITypeMatcher
{
public:
    std::string toString() const override { return "FixedString"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isFixedString(type); }
    size_t getIndex() const override { return 0; }
};

/// Matches any DateTime regardless of timezone metadata.
class TypeMatcherDateTime : public ITypeMatcher
{
public:
    std::string toString() const override { return "DateTime"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDateTime(); }
    size_t getIndex() const override { return 0; }
};

/// Matches any DateTime64 regardless of scale/timezone.
class TypeMatcherDateTime64 : public ITypeMatcher
{
public:
    std::string toString() const override { return "DateTime64"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDateTime64(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherTime : public ITypeMatcher
{
public:
    std::string toString() const override { return "Time"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isTime(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherTime64 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Time64"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isTime64(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDate32 : public ITypeMatcher
{
public:
    std::string toString() const override { return "Date32"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDate32(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDate : public ITypeMatcher
{
public:
    std::string toString() const override { return "Date"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return WhichDataType(type).isDate(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherLowCardinality : public ITypeMatcher
{
public:
    std::string toString() const override { return "LowCardinality"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return type->lowCardinality(); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherLowCardinalityOf : public ITypeMatcher
{
private:
    TypeMatcherPtr child_matcher;
public:
    explicit TypeMatcherLowCardinalityOf(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "LowCardinality type matcher requires single argument");
        child_matcher = child_matchers[0];
    }

    std::string toString() const override { return "LowCardinality(" + child_matcher->toString() + ")"; }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        if (!type->lowCardinality())
        {
            out_reason = "expected LowCardinality, got " + type->getName();
            return false;
        }
        return child_matcher->match(removeLowCardinality(type), variables, iteration, arg_num, out_reason);
    }

    size_t getIndex() const override { return child_matcher->getIndex(); }
};

class TypeMatcherJSON : public ITypeMatcher
{
public:
    std::string toString() const override { return "JSON"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return type->getTypeId() == TypeIndex::Object; }
    size_t getIndex() const override { return 0; }
};

/// Holds a parenthesized list of child matchers. Used as a placeholder carried to a parent
/// matcher that needs a structured group (e.g. lambda argument-type list inside Function).
/// Trying to use this directly as a matcher errors.
class TypeMatcherList : public ITypeMatcher
{
private:
    TypeMatchers children;
public:
    explicit TypeMatcherList(TypeMatchers children_) : children(std::move(children_)) {}
    const TypeMatchers & getChildren() const { return children; }

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        out << "(";
        writeList(children, [&](const auto & c){ out << c->toString(); }, [&]{ out << ", "; });
        out << ")";
        return out.str();
    }

    bool match(const DataTypePtr &, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        out_reason = "parenthesized matcher list cannot be used as a top-level type matcher";
        return false;
    }

    size_t getIndex() const override
    {
        size_t res = 0;
        for (const auto & c : children)
            res = getCommonIndex(res, c->getIndex());
        return res;
    }
};


/// Matches any lambda (DataTypeFunction) — used when the function high-order arg is a
/// lambda and the caller doesn't constrain its shape.
class TypeMatcherFunction : public ITypeMatcher
{
public:
    std::string toString() const override { return "Function"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        if (typeid_cast<const DataTypeFunction *>(type.get()))
            return true;
        out_reason = "expected lambda Function, got " + type->getName();
        return false;
    }
    size_t getIndex() const override { return 0; }
};


/// Sentinel matcher inserted by the parser when it encounters `...` inside a
/// parenthesized matcher list (e.g. `Function((T, ...), R)`). It carries no
/// own matching logic — parent matchers that support variadic lambda args
/// (currently `TypeMatcherFunctionOf`) detect and consume it during
/// construction.
class TypeMatcherEllipsisMarker : public ITypeMatcher
{
public:
    std::string toString() const override { return "..."; }
    bool match(const DataTypePtr &, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        out_reason = "ellipsis marker cannot be used as a top-level matcher";
        return false;
    }
    size_t getIndex() const override { return 0; }
};


/// Matches DataTypeFunction with a specific argument-list shape and return-type shape.
/// Built from Function((Arg1, Arg2, ...), Result) in signatures: the first child is a
/// TypeMatcherList of argument matchers, the second is the return-type matcher.
///
/// Supports a trailing `...` inside the arg list to match a variadic lambda — the
/// matcher immediately preceding the ellipsis is repeated for any extra args. For
/// example, `Function((Any, ...), R)` matches a lambda with zero or more args (each
/// matched against `Any`); `Function((UInt8, T, ...), R)` matches a lambda whose
/// first arg is `UInt8` and whose remaining args each match `T`.
class TypeMatcherFunctionOf : public ITypeMatcher
{
private:
    TypeMatchers arg_matchers;
    bool is_variadic = false;
    TypeMatcherPtr return_matcher;
public:
    explicit TypeMatcherFunctionOf(const TypeMatchers & children)
    {
        if (children.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Function matcher takes exactly two arguments: an arg-list and a return-type matcher");
        const auto * arg_list = dynamic_cast<const TypeMatcherList *>(children[0].get());
        if (!arg_list)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "First argument of Function matcher must be a parenthesized arg list, e.g. Function((T, U), R)");
        arg_matchers = arg_list->getChildren();

        if (!arg_matchers.empty() && dynamic_cast<const TypeMatcherEllipsisMarker *>(arg_matchers.back().get()))
        {
            is_variadic = true;
            arg_matchers.pop_back();
            if (arg_matchers.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Function matcher with `...` must have at least one matcher before the ellipsis");
        }

        return_matcher = children[1];
    }

    std::string toString() const override
    {
        WriteBufferFromOwnString out;
        out << "Function((";
        writeList(arg_matchers, [&](const auto & c){ out << c->toString(); }, [&]{ out << ", "; });
        if (is_variadic)
            out << ", ...";
        out << "), " << return_matcher->toString() << ")";
        return out.str();
    }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        const auto * fn = typeid_cast<const DataTypeFunction *>(type.get());
        if (!fn)
        {
            out_reason = "expected lambda Function, got " + type->getName();
            return false;
        }
        const auto & fn_args = fn->getArgumentTypes();

        if (!is_variadic)
        {
            if (fn_args.size() != arg_matchers.size())
            {
                out_reason = "lambda arity mismatch: expected " + DB::toString(arg_matchers.size())
                    + ", got " + DB::toString(fn_args.size());
                return false;
            }
            for (size_t i = 0; i < arg_matchers.size(); ++i)
            {
                if (!arg_matchers[i]->match(fn_args[i], variables, iteration, arg_num, out_reason))
                {
                    out_reason = "lambda argument " + DB::toString(i) + " doesn't match: " + out_reason;
                    return false;
                }
            }
        }
        else
        {
            /// `(m1, ..., m_{N-1}, m_N, ...)` — the first N-1 matchers match positionally;
            /// the N-th matcher repeats to cover any remaining lambda args (including zero).
            const size_t num_fixed = arg_matchers.size() - 1;
            if (fn_args.size() < num_fixed)
            {
                out_reason = "lambda has too few arguments: expected at least " + DB::toString(num_fixed)
                    + ", got " + DB::toString(fn_args.size());
                return false;
            }
            for (size_t i = 0; i < num_fixed; ++i)
            {
                if (!arg_matchers[i]->match(fn_args[i], variables, iteration, arg_num, out_reason))
                {
                    out_reason = "lambda argument " + DB::toString(i) + " doesn't match: " + out_reason;
                    return false;
                }
            }
            const auto & repeat_matcher = arg_matchers.back();
            for (size_t i = num_fixed; i < fn_args.size(); ++i)
            {
                if (!repeat_matcher->match(fn_args[i], variables, iteration, arg_num, out_reason))
                {
                    out_reason = "lambda argument " + DB::toString(i) + " doesn't match: " + out_reason;
                    return false;
                }
            }
        }

        const auto & fn_ret = fn->getReturnType();
        if (!fn_ret)
        {
            out_reason = "lambda has no inferred return type yet";
            return false;
        }
        if (!return_matcher->match(fn_ret, variables, iteration, arg_num, out_reason))
        {
            out_reason = "lambda return type doesn't match: " + out_reason;
            return false;
        }
        return true;
    }

    size_t getIndex() const override
    {
        size_t res = return_matcher->getIndex();
        for (const auto & m : arg_matchers)
            res = getCommonIndex(res, m->getIndex());
        return res;
    }
};


/// Wraps a string literal token; used as a non-matching constraint passed to parent matchers
/// that need to know an identifier (e.g. an aggregate-function name to require).
class TypeMatcherStringLiteral : public ITypeMatcher
{
private:
    std::string literal;
public:
    explicit TypeMatcherStringLiteral(std::string literal_) : literal(std::move(literal_)) {}
    const std::string & getLiteral() const { return literal; }
    std::string toString() const override { return "'" + literal + "'"; }
    bool match(const DataTypePtr &, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        out_reason = "string literal '" + literal + "' cannot be used as a top-level type matcher";
        return false;
    }
    size_t getIndex() const override { return 0; }
};

/// Matches any DataTypeAggregateFunction regardless of aggregator name or arguments —
/// used when a function consumes "any aggregation state" (e.g. `finalizeAggregation`).
class TypeMatcherAggregateFunction : public ITypeMatcher
{
public:
    std::string toString() const override { return "AggregateFunction"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        if (typeid_cast<const DataTypeAggregateFunction *>(type.get()))
            return true;
        out_reason = "expected AggregateFunction, got " + type->getName();
        return false;
    }
    size_t getIndex() const override { return 0; }
};


/// Matches AggregateFunction(<name>, <element matchers...>) where <name> is a string literal
/// in the signature (e.g. AggregateFunction('groupBitmap', T)).
class TypeMatcherAggregateFunctionOf : public ITypeMatcher
{
private:
    std::string agg_name;
    TypeMatchers element_matchers;
public:
    explicit TypeMatcherAggregateFunctionOf(const TypeMatchers & children)
    {
        if (children.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "AggregateFunction matcher requires at least one argument (the aggregator name as a string literal)");
        const auto * name_lit = dynamic_cast<const TypeMatcherStringLiteral *>(children.front().get());
        if (!name_lit)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "First argument of AggregateFunction matcher must be a string literal (the aggregator name)");
        agg_name = name_lit->getLiteral();
        element_matchers.assign(children.begin() + 1, children.end());
    }

    std::string toString() const override
    {
        std::string s = "AggregateFunction('" + agg_name + "'";
        for (const auto & m : element_matchers)
            s += ", " + m->toString();
        s += ")";
        return s;
    }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        const auto * agg_type = typeid_cast<const DataTypeAggregateFunction *>(type.get());
        if (!agg_type)
        {
            out_reason = "expected AggregateFunction, got " + type->getName();
            return false;
        }
        if (agg_type->getFunctionName() != agg_name)
        {
            out_reason = "expected AggregateFunction('" + agg_name + "'...), got " + agg_type->getName();
            return false;
        }
        const auto & arg_types = agg_type->getArgumentsDataTypes();
        if (element_matchers.size() != arg_types.size())
        {
            out_reason = "AggregateFunction arity mismatch: expected " + DB::toString(element_matchers.size())
                + " element matchers, got AggregateFunction with " + DB::toString(arg_types.size()) + " arguments";
            return false;
        }
        for (size_t i = 0; i < element_matchers.size(); ++i)
        {
            if (!element_matchers[i]->match(arg_types[i], variables, iteration, arg_num, out_reason))
            {
                out_reason = "AggregateFunction argument " + DB::toString(i) + " doesn't match: " + out_reason;
                return false;
            }
        }
        return true;
    }

    size_t getIndex() const override
    {
        size_t res = 0;
        for (const auto & m : element_matchers)
            res = getCommonIndex(res, m->getIndex());
        return res;
    }
};

/// Matches `Nullable(Nothing)` — i.e. the type of a literal `NULL` or an `onlyNull` column.
class TypeMatcherNothing : public ITypeMatcher
{
public:
    std::string toString() const override { return "Nothing"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return type->onlyNull(); }
    size_t getIndex() const override { return 0; }
};

/// Matches the bare `Nothing` data type (e.g. the element type of `Array(Nothing)`
/// produced by `[]`). Different from `Nothing` above which matches `Nullable(Nothing)`.
class TypeMatcherIsNothing : public ITypeMatcher
{
public:
    std::string toString() const override { return "IsNothing"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isNothing(type); }
    size_t getIndex() const override { return 0; }
};

/// Matches the `Bool` data type (a UInt8 with the custom-name `Bool`).
class TypeMatcherBool : public ITypeMatcher
{
public:
    std::string toString() const override { return "Bool"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isBool(type); }
    size_t getIndex() const override { return 0; }
};

/// Matches the `QBit(...)` data type.
class TypeMatcherQBit : public ITypeMatcher
{
public:
    std::string toString() const override { return "QBit"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isQBit(type); }
    size_t getIndex() const override { return 0; }
};

/// Matches a Tuple with at least one element. The bare `Tuple` matcher accepts
/// any tuple including the empty `Tuple()`; this matcher is for functions
/// (e.g. `tupleElement`, `flattenTuple`) that require a non-empty tuple.
class TypeMatcherNonEmptyTuple : public ITypeMatcher
{
public:
    std::string toString() const override { return "NonEmptyTuple"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple_type)
        {
            out_reason = "type " + type->getName() + " is not a Tuple";
            return false;
        }
        if (tuple_type->getElements().empty())
        {
            out_reason = "expected non-empty Tuple, got empty Tuple()";
            return false;
        }
        return true;
    }
    size_t getIndex() const override { return 0; }
};

/// Matches the `Interval` data type (any `IntervalKind`).
class TypeMatcherInterval : public ITypeMatcher
{
public:
    std::string toString() const override { return "Interval"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isInterval(type); }
    size_t getIndex() const override { return 0; }
};

class TypeMatcherDynamic : public ITypeMatcher
{
public:
    std::string toString() const override { return "Dynamic"; }
    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string &) const override { return isDynamic(type); }
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

/// `TupleOfSize(N)` — matches a `Tuple` with exactly `N` elements, regardless of their
/// types. Useful for signatures that need to constrain tuple arity without enumerating
/// element matchers (e.g. `mortonEncode((x, y), mask_x, mask_y)` where the tuple has the
/// same number of elements as the trailing `NativeUInt` arguments).
class TypeMatcherTupleOfSize : public ITypeMatcher
{
private:
    size_t expected_size;
public:
    explicit TypeMatcherTupleOfSize(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "TupleOfSize matcher takes exactly one argument — an integer literal");
        const auto * size_lit = dynamic_cast<const TypeMatcherStringLiteral *>(child_matchers.front().get());
        if (!size_lit)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "TupleOfSize matcher's argument must be an integer literal");
        const String & literal = size_lit->getLiteral();
        expected_size = std::stoull(literal);
    }

    std::string toString() const override
    {
        return "TupleOfSize(" + DB::toString(expected_size) + ")";
    }

    bool match(const DataTypePtr & type, Variables &, size_t, size_t, std::string & out_reason) const override
    {
        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
        if (!tuple_type)
        {
            out_reason = "expected Tuple of size " + DB::toString(expected_size) + ", got " + type->getName();
            return false;
        }
        const size_t actual_size = tuple_type->getElements().size();
        if (actual_size != expected_size)
        {
            out_reason = "expected Tuple of size " + DB::toString(expected_size)
                + ", got Tuple of size " + DB::toString(actual_size);
            return false;
        }
        return true;
    }

    size_t getIndex() const override { return 0; }
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


class TypeMatcherNullable : public ITypeMatcher
{
private:
    TypeMatcherPtr child_matcher;
public:
    explicit TypeMatcherNullable(const TypeMatchers & child_matchers)
    {
        if (child_matchers.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Nullable type matcher requires single argument");

        child_matcher = child_matchers[0];
    }

    std::string toString() const override { return "Nullable(" + child_matcher->toString() + ")"; }

    bool match(const DataTypePtr & type, Variables & variables, size_t iteration, size_t arg_num, std::string & out_reason) const override
    {
        if (!type->isNullable())
        {
            out_reason = "expected Nullable type, got " + type->getName();
            return false;
        }
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
        [captured = std::move(elem)](const TypeMatchers & children) -> TypeMatcherPtr
        {
            /// A no-argument matcher whose name is given children (e.g. `FixedString(16)`,
            /// `Decimal(10, 2)`, `DateTime64(3)`) is not a matcher invocation but an exact
            /// parameterized type. Return "no match" instead of throwing, so the parser falls
            /// through to exact `DataTypeFactory` parsing and the parameterized type stays
            /// expressible in a signature.
            if (!children.empty())
                return nullptr;
            return captured;
        });
}


void registerTypeMatchers()
{
    auto & factory = TypeMatcherFactory::instance();

    registerTypeMatcherWithNoArguments<TypeMatcherUnsignedInteger>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherInteger>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherUInt>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherInt>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeUInt>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeInt>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeInteger>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherStringOrFixedString>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherEnum>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherEnum8>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherEnum16>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNULL>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherIsNothing>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherBool>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherInterval>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherQBit>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNonEmptyTuple>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherRepresentedByNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherSet>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDateOrDateTime>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherUnambiguouslyRepresentedInContiguousMemoryRegion>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherAny>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherFloat>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeFloat>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNativeNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDecimal>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDecimal32>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDecimal64>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDecimal128>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDecimal256>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherValueRepresentedByNumber>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherUUID>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherIPv4>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherIPv6>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherString>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherFixedString>(factory);
    /// Map: 0 args matches any Map; (K, V) matches Map(K, V) and captures key/value types.
    factory.registerElement("Map", [](const TypeMatchers & children) -> TypeMatcherPtr
    {
        if (children.empty())
            return std::make_shared<TypeMatcherMap>();
        return std::make_shared<TypeMatcherMapOf>(children);
    });
    registerTypeMatcherWithNoArguments<TypeMatcherDate>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDate32>(factory);
    /// LowCardinality has both a 0-arg form (matches any LowCardinality type) and a
    /// 1-arg form LowCardinality(X) (matches LowCardinality whose inner type matches X).
    factory.registerElement("LowCardinality", [](const TypeMatchers & children) -> TypeMatcherPtr
    {
        if (children.empty())
            return std::make_shared<TypeMatcherLowCardinality>();
        return std::make_shared<TypeMatcherLowCardinalityOf>(children);
    });
    registerTypeMatcherWithNoArguments<TypeMatcherJSON>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherNothing>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDynamic>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDateTime>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherDateTime64>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherTime>(factory);
    registerTypeMatcherWithNoArguments<TypeMatcherTime64>(factory);

    factory.registerElement("Array", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherArray>(children); });
    factory.registerElement("Tuple", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherTuple>(children); });
    factory.registerElement("TupleOfSize", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherTupleOfSize>(children); });
    factory.registerElement("MaybeNullable", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherMaybeNullable>(children); });
    factory.registerElement("Nullable", [](const TypeMatchers & children) -> TypeMatcherPtr { return std::make_shared<TypeMatcherNullable>(children); });
    factory.registerElement("AggregateFunction", [](const TypeMatchers & children) -> TypeMatcherPtr
    {
        if (children.empty())
            return std::make_shared<TypeMatcherAggregateFunction>();
        return std::make_shared<TypeMatcherAggregateFunctionOf>(children);
    });
    /// Function: 0 args matches any lambda; (arg_list, return) matches a specific shape.
    /// Here `arg_list` must be a parenthesized matcher list passed as a single child.
    factory.registerElement("Function", [](const TypeMatchers & children) -> TypeMatcherPtr
    {
        if (children.empty())
            return std::make_shared<TypeMatcherFunction>();
        return std::make_shared<TypeMatcherFunctionOf>(children);
    });
}

TypeMatcherPtr makeStringLiteralMatcher(std::string literal)
{
    return std::make_shared<TypeMatcherStringLiteral>(std::move(literal));
}

TypeMatcherPtr makeListMatcher(TypeMatchers children)
{
    return std::make_shared<TypeMatcherList>(std::move(children));
}

TypeMatcherPtr makeEllipsisMarkerMatcher()
{
    return std::make_shared<TypeMatcherEllipsisMarker>();
}

}
}
