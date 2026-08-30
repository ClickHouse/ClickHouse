#pragma once
#include <Functions/IFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnTuple.h>
#include <Functions/FunctionHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/Operators.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int ILLEGAL_COLUMN;
    extern const int TOO_FEW_ARGUMENTS_FOR_FUNCTION;
    extern const int TOO_MANY_ARGUMENTS_FOR_FUNCTION;
}

class FunctionSpaceFillingCurveEncode: public IFunction
{
public:
    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    bool useDefaultImplementationForConstants() const override { return true; }

    /// Range-mask `Tuple` accessor for the expanded mode of `mortonEncode` / `hilbertEncode`.
    /// `is_const` selects between reading row 0 for every row or reading row `row_idx`.
    struct RangeMask
    {
        const ColumnTuple * tuple = nullptr;
        bool is_const = false;

        UInt64 read(size_t col_idx, size_t row_idx) const
        {
            return tuple->getColumn(col_idx).getUInt(is_const ? 0 : row_idx);
        }

        size_t tupleSize() const { return tuple->tupleSize(); }

        explicit operator bool() const { return tuple != nullptr; }
    };

    static RangeMask extractRangeMask(const ColumnsWithTypeAndName & arguments)
    {
        if (arguments.empty())
            return {};
        const auto * const_col = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        if (const_col)
        {
            const auto * tuple = typeid_cast<const ColumnTuple *>(const_col->getDataColumnPtr().get());
            if (tuple)
                return RangeMask{tuple, true};
            return {};
        }
        const auto * tuple = typeid_cast<const ColumnTuple *>(arguments[0].column.get());
        if (tuple)
            return RangeMask{tuple, false};
        return {};
    }

    /// The largest number of encoded dimensions the implementation supports.
    virtual size_t getMaxDimensions() const { return 8; }

    /// `mortonEncode`/`hilbertEncode` accept either (a) one to `getMaxDimensions()`
    /// `NativeUInt`s (encoded as a single space-filling-curve key), or (b) a leading
    /// `Tuple` of size N whose elements are all `NativeUInt`, followed by exactly N
    /// `NativeUInt` "mask" arguments. The mask alternatives are spelled out per
    /// arity because the `Tuple(NativeUInt, …)` matcher enforces both the arity
    /// equality and the per-element type at type-check time, and the DSL cannot
    /// yet correlate the `Tuple` size with the number of mask arguments (that
    /// would need a shared count variable, e.g.
    /// `(Tuple(NativeUInt...{N}), NativeUInt...{N})`).
    String getSignatureString() const override
    {
        const size_t max_dimensions = getMaxDimensions();

        WriteBufferFromOwnString out;
        out << "(NativeUInt...{1.." << max_dimensions << "}) -> UInt64";
        for (size_t dimensions = 1; dimensions <= max_dimensions; ++dimensions)
        {
            out << " OR (Tuple(";
            for (size_t i = 0; i < dimensions; ++i)
                out << (i ? ", " : "") << "NativeUInt";
            out << ")";
            for (size_t i = 0; i < dimensions; ++i)
                out << ", NativeUInt";
            out << ") -> UInt64";
        }
        return out.str();
    }

    /// The number of dimensions a call encodes: the size of the leading range-mask `Tuple` in the
    /// expanded mode, the number of arguments otherwise. The declarative signature already rejects
    /// a call that encodes more dimensions than the implementation supports, but it reports a
    /// signature mismatch; check the dimension count separately so that the two codes the legacy
    /// resolver and `executeImpl` used - `TOO_FEW_ARGUMENTS_FOR_FUNCTION` and
    /// `TOO_MANY_ARGUMENTS_FOR_FUNCTION` - stay the user-visible contract.
    void checkNumberOfDimensions(const DataTypes & arguments) const
    {
        if (arguments.empty())
            throw Exception(ErrorCodes::TOO_FEW_ARGUMENTS_FOR_FUNCTION,
                            "At least one UInt argument is required for function {}",
                            getName());

        size_t num_dimensions = arguments.size();
        if (const auto * type_tuple = typeid_cast<const DataTypeTuple *>(arguments[0].get()))
            num_dimensions = type_tuple->getElements().size();

        if (num_dimensions > getMaxDimensions())
            throw Exception(ErrorCodes::TOO_MANY_ARGUMENTS_FOR_FUNCTION,
                            "Illegal number of UInt arguments of function {}: should be not more than {} dimensions",
                            getName(), getMaxDimensions());
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        checkNumberOfDimensions(arguments);
        return IFunction::getReturnTypeImpl(arguments);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        types.reserve(arguments.size());
        for (const auto & argument : arguments)
            types.push_back(argument.type);
        checkNumberOfDimensions(types);
        return IFunction::getReturnTypeImpl(arguments);
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeUInt64>();
    }
};

template <UInt8 max_dimensions, UInt8 min_ratio, UInt8 max_ratio>
class FunctionSpaceFillingCurveDecode: public IFunction
{
public:
    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }

    /// Documentation-only — first arg is a constant `UInt` (the number of
    /// dimensions to decode into) or a constant `Tuple` of bit-shift masks;
    /// the result is a tuple of decoded coordinates (`UInt64` by default,
    /// shrunk per the mask when one is provided).
    String getSignatureString() const override
    {
        /// The `const` modifier applies to the whole first argument; it cannot be repeated
        /// inside the `|` alternation (the parser only accepts one `const` per argument).
        return "(const UInt | Tuple, UInt) -> Tuple";
    }

    /// The declarative signature above is documentation-only — the exact result type isn't
    /// expressible in the DSL, so the `ColumnsWithTypeAndName` override below is authoritative.
    /// Route the types-only path to it too, so the base `IFunction::getReturnTypeImpl(DataTypes)`
    /// fallback never evaluates the bare container return type (which would yield a wrong type or
    /// an internal error).
    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        ColumnsWithTypeAndName columns;
        columns.reserve(arguments.size());
        for (const auto & type : arguments)
            columns.emplace_back(nullptr, type, String{});
        return getReturnTypeImpl(columns);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        UInt64 tuple_size = 0;
        const auto * col_const = typeid_cast<const ColumnConst *>(arguments[0].column.get());
        if (!col_const)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column type {} for function {}, should be a constant (UInt or Tuple)",
                            arguments[0].type->getName(), getName());
        if (!WhichDataType(arguments[1].type).isNativeUInt())
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column type {} for function {}, should be a native UInt",
                            arguments[1].type->getName(), getName());
        const auto * mask = typeid_cast<const ColumnTuple *>(col_const->getDataColumnPtr().get());
        if (mask)
        {
            tuple_size = mask->tupleSize();
        }
        else if (WhichDataType(arguments[0].type).isNativeUInt())
        {
            tuple_size = col_const->getUInt(0);
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                            "Illegal column type {} for function {}, should be UInt or Tuple",
                            arguments[0].type->getName(), getName());
        if (tuple_size > max_dimensions || tuple_size < 1)
            throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                            "Illegal first argument for function {}, should be a number in range 1-{} or a Tuple of such size",
                            getName(), String{max_dimensions});
        if (mask)
        {
            const auto * type_tuple = typeid_cast<const DataTypeTuple *>(arguments[0].type.get());
            for (size_t i = 0; i < tuple_size; i++)
            {
                if (!WhichDataType(type_tuple->getElement(i)).isNativeUInt())
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                                    "Illegal type {} of argument in tuple for function {}, should be a native UInt",
                                    type_tuple->getElement(i)->getName(), getName());
                auto ratio = mask->getColumn(i).getUInt(0);
                if (ratio > max_ratio || ratio < min_ratio)
                    throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND,
                                    "Illegal argument {} in tuple for function {}, should be a number in range {}-{}",
                                    ratio, getName(), String{min_ratio}, String{max_ratio});
            }
        }
        DataTypes types(tuple_size);
        for (size_t i = 0; i < tuple_size; i++)
            types[i] = std::make_shared<DataTypeUInt64>();
        return std::make_shared<DataTypeTuple>(types);
    }
};

}
