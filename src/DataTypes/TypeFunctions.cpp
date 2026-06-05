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
#include <DataTypes/DataTypeInterval.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/getMostSubtype.h>
#include <DataTypes/DataTypeFactory.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Field.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/typeid_cast.h>
#include <Poco/String.h>

#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace FunctionSignatures
{

/// Read a numeric constant field as `UInt64`, regardless of whether the matcher
/// captured it from a signed (`Int8`/`Int32`/…) or unsigned type. `Field::safeGet<UInt64>`
/// throws `Bad get` when the field is stored as `Int64`, but matchers such as `Integer`
/// and `Number` accept signed constants, so a positive signed scale (e.g. for `DateTime64`)
/// must be coerced the way the legacy `extractPrecision` did via `getInt`/`get64`.
static UInt64 fieldToUInt64(const Field & field)
{
    return applyVisitor(FieldVisitorConvertToNumber<UInt64>(), field);
}

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

/// `leastSupertypeOrVariant(T1, T2, ...)` — like `leastSupertype` but, when the inputs
/// have no common supertype (and the `use_variant_as_common_type` setting is on), falls
/// back to `Variant(T1, T2, ...)` instead of throwing. Used by `map` and any other
/// constructor that needs to honour that setting.
class TypeFunctionLeastSupertypeOrVariant : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        DataTypes types;
        types.reserve(args.size());
        for (const Value & arg : args)
            types.emplace_back(arg.type());
        return Value(getLeastSupertypeOrVariant(types));
    }

    std::string name() const override { return "leastSupertypeOrVariant"; }
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
            return Value(DataTypePtr(std::make_shared<DataTypeTuple>(DataTypes{})));

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
        const UInt64 scale = fieldToUInt64(args[0].field());
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
        return Value(DataTypePtr(std::make_shared<DataTypeTime64>(fieldToUInt64(args.front().field()))));
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

/// Extracts the explicit time zone name of a DateTime / DateTime64 argument, or the empty
/// string when the argument carries no explicit time zone (Date, Date32, or a DateTime
/// whose time zone was not specified). Mirrors `extractTimeZoneNameFromFunctionArguments`
/// so that `(T : DateOrDateTime) -> DateTime(timezoneOf(T))` propagates the source time
/// zone exactly as the legacy `getReturnTypeImpl` did.
class TypeFunctionTimezoneOf : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function timezoneOf");
        const DataTypePtr & type = args.front().type();
        if (const auto * dt = typeid_cast<const DataTypeDateTime *>(type.get()))
            return Value(Field(dt->hasExplicitTimeZone() ? dt->getTimeZone().getTimeZone() : std::string()));
        if (const auto * dt64 = typeid_cast<const DataTypeDateTime64 *>(type.get()))
            return Value(Field(dt64->hasExplicitTimeZone() ? dt64->getTimeZone().getTimeZone() : std::string()));
        return Value(Field(std::string()));
    }

    std::string name() const override { return "timezoneOf"; }
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

/// Resolve a const-string type name to a `DataType`. Used in signatures of
/// functions that take the result type as a string argument:
///   reinterpret(value, const t String) -> typeFromString(t)
///   JSONExtract(json, [...], const t String) -> typeFromString(t)
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

/// `subcolumnTypeOf(T, const name)` — looks up the type of a named subcolumn
/// (or tuple element, or variant element, …) of `T`. Throws if `T` does not
/// expose a subcolumn with that name. Used by:
///   getSubcolumn(any, const name String) -> subcolumnTypeOf(any, name)
///   tupleElement(Tuple(...), const name String) -> subcolumnTypeOf(t, name)
///   variantElement(Variant(...), const name String) -> subcolumnTypeOf(v, name)
class TypeFunctionSubcolumnTypeOf : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 2)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function subcolumnTypeOf takes 2 arguments");
        const DataTypePtr & container = args[0].type();
        const String & name = args[1].field().safeGet<String>();
        return Value(container->getSubcolumnType(name));  // throws `ILLEGAL_COLUMN` if absent
    }

    std::string name() const override { return "subcolumnTypeOf"; }
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

        /// For `LowCardinality(T)` the only well-formed nullable shape is `LowCardinality(Nullable(T))`,
        /// since `Nullable(LowCardinality(...))` is not a valid SQL type. Mirror the convention used by
        /// hand-written `getReturnTypeImpl` in functions affected by `join_use_nulls`.
        return Value(makeNullableOrLowCardinalityNullable(args.front().type()));
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

/// Extract the underlying dictionary type of a LowCardinality(T). Used by lowCardinalityKeys.
class TypeFunctionDictionaryTypeOf : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function dictionaryTypeOf");
        const auto * lc = typeid_cast<const DataTypeLowCardinality *>(args.front().type().get());
        if (!lc)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Type function dictionaryTypeOf expects a LowCardinality argument, got {}",
                args.front().type()->getName());
        return Value(lc->getDictionaryType());
    }

    std::string name() const override { return "dictionaryTypeOf"; }
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


/// removeNullable(T) → unwraps an outer Nullable, leaving non-Nullable types unchanged.
/// Used by functions like coalesce that strip Nullable from intermediate arguments before
/// computing a common supertype.
class TypeFunctionRemoveNullable : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function removeNullable");

        return Value(removeNullable(args.front().type()));
    }

    std::string name() const override { return "removeNullable"; }
};


/// `NULL` on the return-type side evaluates to `Nullable(Nothing)`, the type of
/// the SQL NULL constant. Lets signatures write `(NULL, Any) -> NULL` instead of
/// `(Nullable(Nothing), Any) -> Nullable(Nothing)`.
class TypeFunctionNull : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (!args.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function NULL takes no arguments");

        return Value(DataTypePtr(std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>())));
    }

    std::string name() const override { return "NULL"; }
};


/// Helpers used by the arithmetic-promotion type functions below.
/// Native number sizes are expressed in *bits* throughout (8, 16, 32, 64, 128, 256).
namespace
{

    /// Bit-width of a native number type. Returns 0 for non-native-number types.
    size_t nativeNumberBits(const DataTypePtr & type)
    {
        WhichDataType w(type);
        if (w.isUInt8() || w.isInt8())   return 8;
        if (w.isUInt16() || w.isInt16()) return 16;
        if (w.isUInt32() || w.isInt32() || w.isFloat32()) return 32;
        if (w.isUInt64() || w.isInt64() || w.isFloat64()) return 64;
        if (w.isUInt128() || w.isInt128()) return 128;
        if (w.isUInt256() || w.isInt256()) return 256;
        if (w.isBFloat16()) return 16;
        return 0;
    }

    /// Whether the type is a signed native number — for `is_signed` flag.
    /// (Floats are signed by nature.)
    bool nativeNumberIsSigned(const DataTypePtr & type)
    {
        WhichDataType w(type);
        return w.isInt() || w.isFloat() || w.isBFloat16();
    }

    /// Whether the type is a floating-point native number.
    bool nativeNumberIsFloating(const DataTypePtr & type)
    {
        WhichDataType w(type);
        return w.isFloat() || w.isBFloat16();
    }

    /// Whether the type is an integer (signed or unsigned, native or big).
    bool nativeNumberIsInteger(const DataTypePtr & type)
    {
        WhichDataType w(type);
        return w.isInt() || w.isUInt();
    }

    /// Build a native number type from (bits, is_signed, is_floating).
    /// Sizes are 8, 16, 32, 64, 128, 256 — `Construct<is_signed, is_floating, bits/8>`
    /// from `NumberTraits.h` expressed in bits.
    DataTypePtr constructNativeNumber(size_t bits, bool is_signed, bool is_floating)
    {
        if (is_floating)
        {
            switch (bits)
            {
                case 8: case 16: return std::make_shared<DataTypeBFloat16>();
                case 32:         return std::make_shared<DataTypeFloat32>();
                case 64:         return std::make_shared<DataTypeFloat64>();
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot construct a floating-point type of {} bit(s)", bits);
            }
        }
        if (is_signed)
        {
            switch (bits)
            {
                case 8:   return std::make_shared<DataTypeInt8>();
                case 16:  return std::make_shared<DataTypeInt16>();
                case 32:  return std::make_shared<DataTypeInt32>();
                case 64:  return std::make_shared<DataTypeInt64>();
                case 128: return std::make_shared<DataTypeInt128>();
                case 256: return std::make_shared<DataTypeInt256>();
                default:
                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot construct a signed integer of {} bit(s)", bits);
            }
        }
        switch (bits)
        {
            case 8:   return std::make_shared<DataTypeUInt8>();
            case 16:  return std::make_shared<DataTypeUInt16>();
            case 32:  return std::make_shared<DataTypeUInt32>();
            case 64:  return std::make_shared<DataTypeUInt64>();
            case 128: return std::make_shared<DataTypeUInt128>();
            case 256: return std::make_shared<DataTypeUInt256>();
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "Cannot construct an unsigned integer of {} bit(s)", bits);
        }
    }

}


/// `nativeNumber(bits, signed_flag, floating_flag)` — the runtime analogue of
/// `NumberTraits::Construct<signed_flag, floating_flag, bits/8>::Type`. `bits` is
/// 8, 16, 32, 64, 128, or 256 (floating only supports 8, 16, 32, 64 — 8 and 16
/// collapse to `BFloat16`). `signed_flag` and `floating_flag` are 0/1 integer literals.
/// Example: `nativeNumber(64, 1, 0)` → `Int64`.
class TypeFunctionNativeNumber : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 3)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Type function nativeNumber takes 3 arguments: (bits, signed, floating)");
        size_t bits = args[0].field().safeGet<UInt64>();
        bool is_signed = args[1].field().safeGet<UInt64>() != 0;
        bool is_floating = args[2].field().safeGet<UInt64>() != 0;
        return Value(constructNativeNumber(bits, is_signed, is_floating));
    }

    std::string name() const override { return "nativeNumber"; }
};


/// `makeSigned(T)` — signed counterpart of a native number type. UInt8→Int8,
/// UInt16→Int16, …, UInt256→Int256. Signed integers and floats pass through.
class TypeFunctionMakeSigned : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function makeSigned takes 1 argument");
        const DataTypePtr & type = args.front().type();
        size_t bits = nativeNumberBits(type);
        if (!bits)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "makeSigned requires a native number type, got {}", type->getName());
        return Value(constructNativeNumber(bits, /*signed*/ true, nativeNumberIsFloating(type)));
    }

    std::string name() const override { return "makeSigned"; }
};


/// `makeUnsigned(T)` — unsigned counterpart of a native integer type. Int8→UInt8,
/// …, Int256→UInt256. Unsigned integers pass through. Floating-point types are
/// rejected (no unsigned float makes sense).
class TypeFunctionMakeUnsigned : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function makeUnsigned takes 1 argument");
        const DataTypePtr & type = args.front().type();
        if (nativeNumberIsFloating(type))
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "makeUnsigned rejects floating-point type {}", type->getName());
        size_t bits = nativeNumberBits(type);
        if (!bits)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "makeUnsigned requires a native integer type, got {}", type->getName());
        return Value(constructNativeNumber(bits, /*signed*/ false, /*floating*/ false));
    }

    std::string name() const override { return "makeUnsigned"; }
};


/// `nextLargerNativeBits(n)` — bit-wise analogue of `NumberTraits::nextSize`:
/// doubles `n` while it is below 64 bits, otherwise clamps. Used to widen the
/// result type for `plus`, `minus`, `multiply` to avoid overflow at the common
/// max(bits(A), bits(B)).
class TypeFunctionNextLargerNativeBits : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function nextLargerNativeBits takes 1 argument");
        UInt64 n = args[0].field().safeGet<UInt64>();
        if (n < 64)
            n *= 2;
        return Value(Field(n));
    }

    std::string name() const override { return "nextLargerNativeBits"; }
};


/// `maxBits(T1, T2, ...)` — max bit-width across native-number arguments.
/// Non-native-number arguments contribute 0. Returns a `UInt64` Field.
class TypeFunctionMaxBits : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        UInt64 result = 0;
        for (const auto & arg : args)
            result = std::max<UInt64>(result, nativeNumberBits(arg.type()));
        return Value(Field(result));
    }

    std::string name() const override { return "maxBits"; }
};


/// `maxSignedBits(T1, ...)` — max bit-width among inputs that are signed
/// integers/floats. Returns 0 if none are signed.
class TypeFunctionMaxSignedBits : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        UInt64 result = 0;
        for (const auto & arg : args)
            if (nativeNumberIsSigned(arg.type()))
                result = std::max<UInt64>(result, nativeNumberBits(arg.type()));
        return Value(Field(result));
    }

    std::string name() const override { return "maxSignedBits"; }
};


/// `maxUnsignedBits(T1, ...)` — max bit-width among inputs that are unsigned
/// native integers. Returns 0 if none are unsigned.
class TypeFunctionMaxUnsignedBits : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        UInt64 result = 0;
        for (const auto & arg : args)
            if (!nativeNumberIsSigned(arg.type()) && nativeNumberIsInteger(arg.type()))
                result = std::max<UInt64>(result, nativeNumberBits(arg.type()));
        return Value(Field(result));
    }

    std::string name() const override { return "maxUnsignedBits"; }
};


/// `maxIntegerBits(T1, ...)` — max bit-width among integer (signed or unsigned) arguments.
class TypeFunctionMaxIntegerBits : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        UInt64 result = 0;
        for (const auto & arg : args)
            if (nativeNumberIsInteger(arg.type()))
                result = std::max<UInt64>(result, nativeNumberBits(arg.type()));
        return Value(Field(result));
    }

    std::string name() const override { return "maxIntegerBits"; }
};


/// `maxFloatingBits(T1, ...)` — max bit-width among floating-point arguments.
class TypeFunctionMaxFloatingBits : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        UInt64 result = 0;
        for (const auto & arg : args)
            if (nativeNumberIsFloating(arg.type()))
                result = std::max<UInt64>(result, nativeNumberBits(arg.type()));
        return Value(Field(result));
    }

    std::string name() const override { return "maxFloatingBits"; }
};


/// `anySigned(T1, ...)` — `1` if any argument is a signed integer or a floating-point
/// type (which is signed by nature), otherwise `0`.
class TypeFunctionAnySigned : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        for (const auto & arg : args)
            if (nativeNumberIsSigned(arg.type()))
                return Value(Field(UInt64{1}));
        return Value(Field(UInt64{0}));
    }

    std::string name() const override { return "anySigned"; }
};


/// `anyFloating(T1, ...)` — `1` if any argument is a floating-point type, otherwise `0`.
class TypeFunctionAnyFloating : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        for (const auto & arg : args)
            if (nativeNumberIsFloating(arg.type()))
                return Value(Field(UInt64{1}));
        return Value(Field(UInt64{0}));
    }

    std::string name() const override { return "anyFloating"; }
};

/// `anyFloat64(T1, T2, ...)` — predicate type function that returns `1` if
/// any argument is `Float64` (native, not BFloat16), otherwise `0`. Used by
/// `greatCircleDistance` and friends to widen the result to `Float64`
/// whenever one of the coordinate inputs is `Float64`.
class TypeFunctionAnyFloat64 : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        for (const auto & arg : args)
            if (WhichDataType(arg.type()).isFloat64())
                return Value(Field(UInt64{1}));
        return Value(Field(UInt64{0}));
    }

    std::string name() const override { return "anyFloat64"; }
};

/// `anyBool(T1, T2, ...)` — predicate type function that returns `1` if any
/// argument is the `Bool` custom-named UInt8 type, otherwise `0`. Mirrors
/// `isBool` (which matches the literal type name) — `Nullable(Bool)` does
/// *not* satisfy this predicate, matching the legacy `and`/`or`/`xor` rule
/// that only un-wrapped Bool inputs propagate to a Bool result.
class TypeFunctionAnyBool : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        for (const auto & arg : args)
            if (arg.type()->getName() == "Bool")
                return Value(Field(UInt64{1}));
        return Value(Field(UInt64{0}));
    }

    std::string name() const override { return "anyBool"; }
};

/// `anyNullable(T1, T2, ...)` — predicate type function that returns `1` if
/// any argument is a `Nullable(...)` type, otherwise `0`.
class TypeFunctionAnyNullable : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        for (const auto & arg : args)
            if (arg.type()->isNullable())
                return Value(Field(UInt64{1}));
        return Value(Field(UInt64{0}));
    }

    std::string name() const override { return "anyNullable"; }
};

/// `concatTuples(T1, T2, ...)` — concatenates the element lists of the given
/// Tuple types and returns a flat Tuple containing all elements in order.
/// Throws if any argument is not a Tuple. Names are preserved when *every*
/// input tuple is named (mixing named and unnamed loses the names, matching
/// the legacy `tupleConcat` behaviour which builds an unnamed result tuple).
class TypeFunctionConcatTuples : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        DataTypes flat_elems;
        Strings flat_names;
        bool all_named = !args.empty();
        for (const auto & arg : args)
        {
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(arg.type().get());
            if (!tuple_type)
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                    "concatTuples requires Tuple(...), got {}", arg.type()->getName());

            const auto & elems = tuple_type->getElements();
            flat_elems.insert(flat_elems.end(), elems.begin(), elems.end());

            if (tuple_type->hasExplicitNames())
            {
                const auto & names = tuple_type->getElementNames();
                flat_names.insert(flat_names.end(), names.begin(), names.end());
            }
            else
                all_named = false;
        }

        if (all_named && !flat_names.empty())
            return Value(DataTypePtr(std::make_shared<DataTypeTuple>(flat_elems, flat_names)));
        return Value(DataTypePtr(std::make_shared<DataTypeTuple>(flat_elems)));
    }

    std::string name() const override { return "concatTuples"; }
};

/// `makeNullableIfCanBe(T)` — wraps `T` in `Nullable` if `T->canBeInsideNullable()`,
/// otherwise returns `T` unchanged. Used by `arrayElementOrNull` to compute the
/// result type — the Or-Null variant wraps in Nullable only when the element type
/// permits it (Tuple-of-tuple chains, Map, etc. cannot be inside Nullable).
class TypeFunctionMakeNullableIfCanBe : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function makeNullableIfCanBe takes 1 argument");

        const DataTypePtr & type = args.front().type();
        if (type->canBeInsideNullable())
            return Value(makeNullable(type));
        return Value(type);
    }

    std::string name() const override { return "makeNullableIfCanBe"; }
};

/// `mostSubtype(T1, T2, ...)` — finds the most specific common type. Used by
/// `arrayIntersect` to compute the result element type as the type that fits
/// in every input array. When the inputs share no common subtype (e.g. one
/// input is `Nothing` — i.e. `Array(Nothing)` produced by `[]`), the result
/// is `Nothing` rather than an exception, so `arrayIntersect([1], [])` keeps
/// returning `Array(Nothing)` rather than throwing `NO_COMMON_TYPE`.
class TypeFunctionMostSubtype : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        DataTypes types;
        types.reserve(args.size());
        for (const auto & arg : args)
            types.emplace_back(arg.type());
        return Value(getMostSubtype(types, /* throw_if_result_is_nothing */ false));
    }

    std::string name() const override { return "mostSubtype"; }
};

/// `innermostArrayElement(T)` — for a (possibly nested) Array type, returns the
/// innermost (non-Array) element type. `Array(Array(Int32))` -> `Int32`,
/// `Array(Int32)` -> `Int32`. Throws if `T` is not an Array.
class TypeFunctionInnermostArrayElement : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function innermostArrayElement takes 1 argument");

        DataTypePtr current = args.front().type();
        const auto * array_type = typeid_cast<const DataTypeArray *>(current.get());
        if (!array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "innermostArrayElement requires Array(...), got {}", current->getName());

        while (const auto * inner_array = typeid_cast<const DataTypeArray *>(current.get()))
            current = inner_array->getNestedType();
        return Value(current);
    }

    std::string name() const override { return "innermostArrayElement"; }
};

/// `reverseTuple(T)` — flips the element order of a Tuple, preserving names if
/// the tuple is named. Used by `reverse` to compute the result type when the
/// input is a Tuple. Throws if the input is not a Tuple.
class TypeFunctionReverseTuple : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function reverseTuple takes 1 argument");

        const auto * tuple_type = typeid_cast<const DataTypeTuple *>(args.front().type().get());
        if (!tuple_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "reverseTuple requires Tuple(...), got {}", args.front().type()->getName());

        const auto & elems = tuple_type->getElements();
        DataTypes reversed_elems(elems.rbegin(), elems.rend());

        if (tuple_type->hasExplicitNames())
        {
            const auto & names = tuple_type->getElementNames();
            Strings reversed_names(names.rbegin(), names.rend());
            return Value(DataTypePtr(std::make_shared<DataTypeTuple>(reversed_elems, reversed_names)));
        }

        return Value(DataTypePtr(std::make_shared<DataTypeTuple>(reversed_elems)));
    }

    std::string name() const override { return "reverseTuple"; }
};


/// `isFloat32OrSmaller(T)` — `1` if `T` is a floating-point type at most 32 bits
/// wide (i.e. `BFloat16` or `Float32`), otherwise `0`. The `Float64`-promotion
/// rule used by `arrayDistance`, `arrayNorm`, and the cosine/Lp variants is
/// "return `Float32` when the common type is `BFloat16` or `Float32`, else
/// `Float64`" — this predicate is the obvious way to express that branch.
class TypeFunctionIsFloat32OrSmaller : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function isFloat32OrSmaller takes 1 argument");
        const DataTypePtr & type = args.front().type();
        WhichDataType w(type);
        const bool small_float = w.isBFloat16() || w.isFloat32();
        return Value(Field(UInt64{small_float ? 1u : 0u}));
    }

    std::string name() const override { return "isFloat32OrSmaller"; }
};


/// `anyInteger(T1, ...)` — `1` if any argument is a native integer type, otherwise `0`.
class TypeFunctionAnyInteger : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        for (const auto & arg : args)
            if (nativeNumberIsInteger(arg.type()))
                return Value(Field(UInt64{1}));
        return Value(Field(UInt64{0}));
    }

    std::string name() const override { return "anyInteger"; }
};


/// `selectIf(cond, then_value, else_value)` — picks `then_value` when `cond` is a
/// non-zero integer Field, otherwise `else_value`. Useful in arithmetic signatures
/// for cases like `ResultOfBit` where the result width is `64` bits when any operand
/// is floating-point and `maxBits(A, B)` otherwise:
///   `nativeNumber(selectIf(anyFloating(A, B), 64, maxBits(A, B)), anySigned(A, B), 0)`.
class TypeFunctionSelectIf : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 3)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Type function selectIf takes 3 arguments: (cond, then, else)");
        const UInt64 cond = args[0].field().safeGet<UInt64>();
        return cond ? args[1] : args[2];
    }

    std::string name() const override { return "selectIf"; }
};


/// IntervalType('week') → DataTypeInterval(Kind::Week). Used by functions that take a
/// constant string naming an interval unit and return an Interval-typed value, such as
/// `toInterval(value, 'day')`.
class TypeFunctionIntervalType : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wrong number of arguments for type function IntervalType");

        const String name = args.front().field().safeGet<String>();
        IntervalKind kind;
        if (!IntervalKind::tryParseString(Poco::toLower(name), kind.kind))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'{}' doesn't look like an interval unit", name);

        return Value(DataTypePtr(std::make_shared<DataTypeInterval>(kind.kind)));
    }

    std::string name() const override { return "IntervalType"; }
};


/// `aggregateFunctionReturnType(T)` — unwraps a `DataTypeAggregateFunction` and
/// returns the type that the aggregator finalizes to (i.e. the type of
/// `finalizeAggregation(state)`). Throws if `T` is not an aggregate-function state.
class TypeFunctionAggregateFunctionReturnType : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Type function aggregateFunctionReturnType takes 1 argument");
        const auto * af = typeid_cast<const DataTypeAggregateFunction *>(args.front().type().get());
        if (!af)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "aggregateFunctionReturnType requires AggregateFunction(...), got {}",
                args.front().type()->getName());
        return Value(af->getReturnType());
    }

    std::string name() const override { return "aggregateFunctionReturnType"; }
};


/// AggregateFunction('sum', UInt64) → DataTypeAggregateFunction wrapping the sum aggregator
/// over UInt64. Used by functions whose result is an aggregation state typed by both the
/// aggregator name (constant string) and the argument types of the aggregator.
class TypeFunctionAggregateFunctionType : public ITypeFunction
{
public:
    Value apply(const Values & args) const override
    {
        if (args.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Type function AggregateFunction requires at least one argument (aggregator name)");

        const String agg_name = args.front().field().safeGet<String>();

        DataTypes arg_types;
        arg_types.reserve(args.size() - 1);
        for (size_t i = 1; i < args.size(); ++i)
            arg_types.emplace_back(args[i].type());

        Array params;
        AggregateFunctionProperties properties;
        AggregateFunctionPtr func
            = AggregateFunctionFactory::instance().get(agg_name, NullsAction::EMPTY, arg_types, params, properties);

        return Value(DataTypePtr(std::make_shared<DataTypeAggregateFunction>(std::move(func), std::move(arg_types), std::move(params))));
    }

    std::string name() const override { return "AggregateFunction"; }
};


void registerTypeFunctions()
{
    auto & factory = TypeFunctionFactory::instance();

    factory.registerElement<TypeFunctionLeastSupertype>();
    factory.registerElement<TypeFunctionLeastSupertypeOrVariant>();
    factory.registerElement<TypeFunctionArray>();
    factory.registerElement<TypeFunctionTuple>();
    factory.registerElement<TypeFunctionNamedField>();
    factory.registerElement<TypeFunctionMap>();
    factory.registerElement<TypeFunctionFixedString>();
    factory.registerElement<TypeFunctionDateTime>();
    factory.registerElement<TypeFunctionDateTime64>();
    factory.registerElement<TypeFunctionTime64>();
    factory.registerElement<TypeFunctionScaleOf>();
    factory.registerElement<TypeFunctionTimezoneOf>();
    factory.registerElement<TypeFunctionMax>();
    factory.registerElement<TypeFunctionDifference>();
    factory.registerElement<TypeFunctionTypeFromString>();
    factory.registerElement<TypeFunctionSubcolumnTypeOf>();
    factory.registerElement<TypeFunctionNullable>();
    factory.registerElement<TypeFunctionLowCardinality>();
    factory.registerElement<TypeFunctionDictionaryTypeOf>();
    factory.registerElement<TypeFunctionIntervalType>();
    factory.registerElement<TypeFunctionAggregateFunctionType>();
    factory.registerElement<TypeFunctionAggregateFunctionReturnType>();
    factory.registerElement<TypeFunctionRemoveNullable>();
    factory.registerElement<TypeFunctionNull>();
    factory.registerElement<TypeFunctionNativeNumber>();
    factory.registerElement<TypeFunctionMakeSigned>();
    factory.registerElement<TypeFunctionMakeUnsigned>();
    factory.registerElement<TypeFunctionNextLargerNativeBits>();
    factory.registerElement<TypeFunctionMaxBits>();
    factory.registerElement<TypeFunctionMaxSignedBits>();
    factory.registerElement<TypeFunctionMaxUnsignedBits>();
    factory.registerElement<TypeFunctionMaxIntegerBits>();
    factory.registerElement<TypeFunctionMaxFloatingBits>();
    factory.registerElement<TypeFunctionAnySigned>();
    factory.registerElement<TypeFunctionAnyFloating>();
    factory.registerElement<TypeFunctionAnyFloat64>();
    factory.registerElement<TypeFunctionAnyBool>();
    factory.registerElement<TypeFunctionAnyNullable>();
    factory.registerElement<TypeFunctionReverseTuple>();
    factory.registerElement<TypeFunctionConcatTuples>();
    factory.registerElement<TypeFunctionInnermostArrayElement>();
    factory.registerElement<TypeFunctionMostSubtype>();
    factory.registerElement<TypeFunctionMakeNullableIfCanBe>();
    factory.registerElement<TypeFunctionAnyInteger>();
    factory.registerElement<TypeFunctionSelectIf>();
    factory.registerElement<TypeFunctionIsFloat32OrSmaller>();

    /// Predicates.
    factory.registerElement<TypeFunctionTuplesHaveSameSize>();
}

}
}
