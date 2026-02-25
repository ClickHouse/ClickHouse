#include <Functions/FunctionsConversion.h>

#if USE_EMBEDDED_COMPILER
#    include "DataTypes/Native.h"
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_CONVERT_TYPE;
    extern const int CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SIZES_OF_ARRAYS_DONT_MATCH;
    extern const int TYPE_MISMATCH;
}

namespace detail
{

UInt32 extractToDecimalScale(const ColumnWithTypeAndName & named_column)
{
    const auto * arg_type = named_column.type.get();
    bool ok = checkAndGetDataType<DataTypeUInt64>(arg_type)
        || checkAndGetDataType<DataTypeUInt32>(arg_type)
        || checkAndGetDataType<DataTypeUInt16>(arg_type)
        || checkAndGetDataType<DataTypeUInt8>(arg_type);
    if (!ok)
        throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type of toDecimal() scale {}", named_column.type->getName());

    Field field;
    named_column.column->get(0, field);
    return static_cast<UInt32>(field.safeGet<UInt32>());
}

ColumnUInt8::MutablePtr copyNullMap(ColumnPtr col)
{
    ColumnUInt8::MutablePtr null_map = nullptr;
    if (const auto * col_nullable = checkAndGetColumn<ColumnNullable>(col.get()))
    {
        null_map = ColumnUInt8::create();
        null_map->insertRangeFrom(col_nullable->getNullMapColumn(), 0, col_nullable->size());
    }
    return null_map;
}

}

namespace detail
{

ExecutableFunctionPtr FunctionCast::prepare(const ColumnsWithTypeAndName & /*sample_columns*/) const
{
    try
    {
        return std::make_unique<ExecutableFunctionCast>(
            prepareUnpackDictionaries(getArgumentTypes()[0], getResultType()), cast_name, diagnostic);
    }
    catch (Exception & e)
    {
        if (diagnostic)
            e.addMessage("while converting source column " + backQuoteIfNeed(diagnostic->column_from) +
                         " to destination column " + backQuoteIfNeed(diagnostic->column_to));
        throw;
    }
}

template <typename ToDataType>
FunctionCast::WrapperType FunctionCast::createWrapper(const DataTypePtr & from_type, const ToDataType * const to_type, bool requested_result_is_nullable) const
{
    TypeIndex from_type_index = from_type->getTypeId();
    WhichDataType which(from_type_index);
    TypeIndex to_type_index = to_type->getTypeId();
    WhichDataType to(to_type_index);
    bool can_apply_accurate_cast = (cast_type == CastType::accurate || cast_type == CastType::accurateOrNull)
        && (which.isInt() || which.isUInt() || which.isFloat());
    can_apply_accurate_cast |= cast_type == CastType::accurate && which.isStringOrFixedString() && to.isNativeInteger();

    if (requested_result_is_nullable && checkAndGetDataType<DataTypeString>(from_type.get()))
    {
        /// In case when converting to Nullable type, we apply different parsing rule,
        /// that will not throw an exception but return NULL in case of malformed input.
        FunctionPtr function = FunctionConvertFromString<ToDataType, FunctionCastName, ConvertFromStringExceptionMode::Null>::createFromSettings(settings);
        return createFunctionAdaptor(function, from_type);
    }
    else if (!can_apply_accurate_cast)
    {
        if constexpr (std::is_same_v<ToDataType, DataTypeDateTime>)
        {
            FunctionPtr function = FunctionTo<DataTypeDateTime>::Type::createFromSettings(settings);
            return createFunctionAdaptor(function, from_type);
        }
        else
        {
            FunctionPtr function = FunctionTo<ToDataType>::Type::createFromSettings(settings);
            return createFunctionAdaptor(function, from_type);
        }
    }

    return [this, from_type_index, to_type]
        (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count)
    {
        ColumnPtr result_column;
        auto res = callOnIndexAndDataType<ToDataType>(from_type_index, [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if constexpr (IsDataTypeNumber<LeftDataType>)
            {
                if constexpr (IsDataTypeDateOrDateTimeOrTime<RightDataType>)
                {
#define GENERATE_OVERFLOW_MODE_CASE(OVERFLOW_MODE, ADDITIONS) \
case FormatSettings::DateTimeOverflowBehavior::OVERFLOW_MODE: \
    result_column \
        = ConvertImpl<LeftDataType, RightDataType, FunctionCastName, FormatSettings::DateTimeOverflowBehavior::OVERFLOW_MODE>:: \
            execute(arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, settings, ADDITIONS()); \
    break;
                    if (cast_type == CastType::accurate)
                    {
                        switch (settings.date_time_overflow_behavior)
                        {
                            GENERATE_OVERFLOW_MODE_CASE(Throw, DateTimeAccurateConvertStrategyAdditions)
                            GENERATE_OVERFLOW_MODE_CASE(Ignore, DateTimeAccurateConvertStrategyAdditions)
                            GENERATE_OVERFLOW_MODE_CASE(Saturate, DateTimeAccurateConvertStrategyAdditions)
                        }
                    }
                    else
                    {
                        switch (settings.date_time_overflow_behavior)
                        {
                            GENERATE_OVERFLOW_MODE_CASE(Throw, DateTimeAccurateOrNullConvertStrategyAdditions)
                            GENERATE_OVERFLOW_MODE_CASE(Ignore, DateTimeAccurateOrNullConvertStrategyAdditions)
                            GENERATE_OVERFLOW_MODE_CASE(Saturate, DateTimeAccurateOrNullConvertStrategyAdditions)
                        }
                    }
#undef GENERATE_OVERFLOW_MODE_CASE

                    return true;
                }
                else if constexpr (IsDataTypeNumber<RightDataType>)
                {
                    if (cast_type == CastType::accurate)
                    {
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                            arguments,
                            result_type,
                            input_rows_count,
                            BehaviourOnErrorFromString::ConvertDefaultBehaviorTag,
                            settings,
                            AccurateConvertStrategyAdditions());
                    }
                    else
                    {
                        result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                            arguments,
                            result_type,
                            input_rows_count,
                            BehaviourOnErrorFromString::ConvertDefaultBehaviorTag,
                            settings,
                            AccurateOrNullConvertStrategyAdditions());
                    }

                    return true;
                }
            }
            else if constexpr (IsDataTypeStringOrFixedString<LeftDataType>)
            {
                if constexpr (IsDataTypeNumber<RightDataType>)
                {
                    chassert(cast_type == CastType::accurate);
                    result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                        arguments,
                        result_type,
                        input_rows_count,
                        BehaviourOnErrorFromString::ConvertDefaultBehaviorTag,
                        settings,
                        AccurateConvertStrategyAdditions());
                }
                return true;
            }

            return false;
        });

        /// Additionally check if callOnIndexAndDataType wasn't called at all.
        if (!res)
        {
            if (cast_type == CastType::accurateOrNull)
            {
                auto nullable_column_wrapper = FunctionCast::createToNullableColumnWrapper();
                return nullable_column_wrapper(arguments, result_type, column_nullable, input_rows_count);
            }
            else
            {
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Conversion from {} to {} is not supported",
                    from_type_index, to_type->getName());
            }
        }

        return result_column;
    };
}

template <typename ToDataType>
FunctionCast::WrapperType FunctionCast::createBoolWrapper(const DataTypePtr & from_type, const ToDataType * const to_type, bool requested_result_is_nullable) const
{
    if (checkAndGetDataType<DataTypeString>(from_type.get()))
    {
        if (cast_type == CastType::accurateOrNull)
        {
            return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
            {
                return ConvertImplGenericFromString<false>::execute(arguments, result_type, column_nullable, input_rows_count, settings);
            };
        }

        return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
        {
            return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, settings);
        };
    }

    return createWrapper<ToDataType>(from_type, to_type, requested_result_is_nullable);
}

FunctionCast::WrapperType FunctionCast::createUInt8ToBoolWrapper(const DataTypePtr from_type, const DataTypePtr to_type) const
{
    return [from_type, to_type] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/) -> ColumnPtr
    {
        /// Special case when we convert UInt8 column to Bool column.
        /// both columns have type UInt8, but we shouldn't use identity wrapper,
        /// because Bool column can contain only 0 and 1.
        auto res_column = to_type->createColumn();
        const auto & data_from = checkAndGetColumn<ColumnUInt8>(*arguments[0].column).getData();
        auto & data_to = assert_cast<ColumnUInt8 *>(res_column.get())->getData();
        data_to.resize(data_from.size());
        for (size_t i = 0; i != data_from.size(); ++i)
            data_to[i] = static_cast<bool>(data_from[i]);
        return res_column;
    };
}

FunctionCast::WrapperType FunctionCast::createStringWrapper(const DataTypePtr & from_type) const
{
    FunctionPtr function = FunctionToString::createFromSettings(settings);
    return createFunctionAdaptor(function, from_type);
}

FunctionCast::WrapperType FunctionCast::createFixedStringWrapper(const DataTypePtr & from_type, const size_t N) const
{
    if (!isStringOrFixedString(from_type))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "CAST AS FixedString is only implemented for types String and FixedString");

    bool exception_mode_null = cast_type == CastType::accurateOrNull;
    return [exception_mode_null, N] (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t /*input_rows_count*/)
    {
        if (exception_mode_null)
            return FunctionToFixedString::executeForN<ConvertToFixedStringExceptionMode::Null>(arguments, N);
        else
            return FunctionToFixedString::executeForN<ConvertToFixedStringExceptionMode::Throw>(arguments, N);
    };
}

#define GENERATE_INTERVAL_CASE(INTERVAL_KIND) \
        case IntervalKind::Kind::INTERVAL_KIND: \
            return createFunctionAdaptor(FunctionConvert<DataTypeInterval, NameToInterval##INTERVAL_KIND, PositiveMonotonicity>::createFromSettings(settings), from_type);

FunctionCast::WrapperType FunctionCast::createIntervalWrapper(const DataTypePtr & from_type, IntervalKind kind) const
{
    switch (kind.kind)
    {
        GENERATE_INTERVAL_CASE(Nanosecond)
        GENERATE_INTERVAL_CASE(Microsecond)
        GENERATE_INTERVAL_CASE(Millisecond)
        GENERATE_INTERVAL_CASE(Second)
        GENERATE_INTERVAL_CASE(Minute)
        GENERATE_INTERVAL_CASE(Hour)
        GENERATE_INTERVAL_CASE(Day)
        GENERATE_INTERVAL_CASE(Week)
        GENERATE_INTERVAL_CASE(Month)
        GENERATE_INTERVAL_CASE(Quarter)
        GENERATE_INTERVAL_CASE(Year)
    }
    throw Exception{ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion to unexpected IntervalKind: {}", kind.toString()};
}

#undef GENERATE_INTERVAL_CASE

template <typename ToDataType>
requires IsDataTypeDecimal<ToDataType>
FunctionCast::WrapperType FunctionCast::createDecimalWrapper(const DataTypePtr & from_type, const ToDataType * to_type, bool requested_result_is_nullable) const
{
    TypeIndex type_index = from_type->getTypeId();
    UInt32 scale = to_type->getScale();

    WhichDataType which(type_index);
    bool ok = which.isNativeInt() || which.isNativeUInt() || which.isDecimal() || which.isFloat() || which.isDateOrDate32() || which.isDateTime() || which.isDateTime64()
        || which.isTime() || which.isTime64() || which.isStringOrFixedString();
    if (!ok)
    {
        if (cast_type == CastType::accurateOrNull)
            return createToNullableColumnWrapper();
        else
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
                from_type->getName(), to_type->getName());
    }

    return [this, type_index, scale, to_type, requested_result_is_nullable]
        (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *column_nullable, size_t input_rows_count)
    {
        ColumnPtr result_column;
        auto res = callOnIndexAndDataType<ToDataType>(type_index, [&](const auto & types) -> bool
        {
            using Types = std::decay_t<decltype(types)>;
            using LeftDataType = typename Types::LeftType;
            using RightDataType = typename Types::RightType;

            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType> && !(std::is_same_v<DataTypeDateTime64, RightDataType> || std::is_same_v<DataTypeTime64, RightDataType>))
            {
                if (cast_type == CastType::accurate)
                {
                    AccurateConvertStrategyAdditions additions;
                    additions.scale = scale;
                    result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                        arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, settings, additions);

                    return true;
                }
                else if (cast_type == CastType::accurateOrNull)
                {
                    AccurateOrNullConvertStrategyAdditions additions;
                    additions.scale = scale;
                    result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                        arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, settings, additions);

                    return true;
                }
            }
            else if constexpr (std::is_same_v<LeftDataType, DataTypeString>)
            {
                if (requested_result_is_nullable)
                {
                    /// Consistent with CAST(Nullable(String) AS Nullable(Numbers))
                    /// In case when converting to Nullable type, we apply different parsing rule,
                    /// that will not throw an exception but return NULL in case of malformed input.
                    result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(
                        arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertReturnNullOnErrorTag, settings, scale);

                    return true;
                }
            }

            result_column = ConvertImpl<LeftDataType, RightDataType, FunctionCastName>::execute(arguments, result_type, input_rows_count, BehaviourOnErrorFromString::ConvertDefaultBehaviorTag, settings, scale);

            return true;
        });

        /// Additionally check if callOnIndexAndDataType wasn't called at all.
        if (!res)
        {
            if (cast_type == CastType::accurateOrNull)
            {
                auto nullable_column_wrapper = FunctionCast::createToNullableColumnWrapper();
                return nullable_column_wrapper(arguments, result_type, column_nullable, input_rows_count);
            }
            else
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE,
                    "Conversion from {} to {} is not supported",
                    type_index, to_type->getName());
        }

        return result_column;
    };
}

FunctionCast::WrapperType FunctionCast::createAggregateFunctionWrapper(const DataTypePtr & from_type_untyped, const DataTypeAggregateFunction * to_type) const
{
    /// Conversion from String through parsing.
    if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
    {
        return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
        {
            return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, settings);
        };
    }
    else if (const auto * agg_type = checkAndGetDataType<DataTypeAggregateFunction>(from_type_untyped.get()))
    {
        if (agg_type->getFunction()->haveSameStateRepresentation(*to_type->getFunction()))
        {
            return [function = to_type->getFunction()](
                       ColumnsWithTypeAndName & arguments,
                       const DataTypePtr & /* result_type */,
                       const ColumnNullable * /* nullable_source */,
                       size_t /*input_rows_count*/) -> ColumnPtr
            {
                const auto & argument_column = arguments.front();
                const auto * col_agg = checkAndGetColumn<ColumnAggregateFunction>(argument_column.column.get());
                if (col_agg)
                {
                    auto new_col_agg = ColumnAggregateFunction::create(*col_agg);
                    new_col_agg->set(function);
                    return new_col_agg;
                }
                else
                {
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Illegal column {} for function CAST AS AggregateFunction",
                        argument_column.column->getName());
                }
            };
        }
    }

    if (cast_type == CastType::accurateOrNull)
        return createToNullableColumnWrapper();
    else
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
            from_type_untyped->getName(), to_type->getName());
}

FunctionCast::WrapperType FunctionCast::createArrayWrapper(const DataTypePtr & from_type_untyped, const DataTypeArray & to_type) const
{
    /// Conversion from String through parsing.
    if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
    {
        return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
        {
            return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, settings);
        };
    }

    DataTypePtr from_type_holder;
    const auto * from_type = checkAndGetDataType<DataTypeArray>(from_type_untyped.get());
    const auto * from_type_map = checkAndGetDataType<DataTypeMap>(from_type_untyped.get());

    /// Convert from Map
    if (from_type_map)
    {
        /// Recreate array of unnamed tuples because otherwise it may work
        /// unexpectedly while converting to array of named tuples.
        from_type_holder = from_type_map->getNestedTypeWithUnnamedTuple();
        from_type = assert_cast<const DataTypeArray *>(from_type_holder.get());
    }

    if (!from_type)
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "CAST AS Array can only be performed between same-dimensional Array, Map or String types");
    }

    DataTypePtr from_nested_type = from_type->getNestedType();

    /// In query SELECT CAST([] AS Array(Array(String))) from type is Array(Nothing)
    bool from_empty_array = isNothing(from_nested_type);

    if (from_type->getNumberOfDimensions() != to_type.getNumberOfDimensions() && !from_empty_array)
        throw Exception(ErrorCodes::TYPE_MISMATCH,
            "CAST AS Array can only be performed between same-dimensional array types");

    const DataTypePtr & to_nested_type = to_type.getNestedType();

    /// Prepare nested type conversion
    const auto nested_function = prepareUnpackDictionaries(from_nested_type, to_nested_type);

    return [nested_function, from_nested_type, to_nested_type](
            ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * /*nullable_source*/, size_t /*input_rows_count*/) -> ColumnPtr
    {
        const auto & argument_column = arguments.front();

        const ColumnArray * col_array = nullptr;

        if (const ColumnMap * col_map = checkAndGetColumn<ColumnMap>(argument_column.column.get()))
            col_array = &col_map->getNestedColumn();
        else
            col_array = checkAndGetColumn<ColumnArray>(argument_column.column.get());

        if (col_array)
        {
            /// create columns for converting nested column containing original and result columns
            ColumnsWithTypeAndName nested_columns{{ col_array->getDataPtr(), from_nested_type, "" }};

            /// convert nested column
            /// Don't propagate nullable_source into array elements — the inner data column
            /// has a different size (total elements vs. number of rows), which would cause
            /// "ColumnNullable is not compatible with original" in createStringToEnumWrapper.
            auto result_column = nested_function(nested_columns, to_nested_type, nullptr, nested_columns.front().column->size());

            /// set converted nested column to result
            return ColumnArray::create(result_column, col_array->getOffsetsPtr());
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Illegal column {} for function CAST AS Array",
                argument_column.column->getName());
        }
    };
}

FunctionCast::ElementWrappers FunctionCast::getElementWrappers(const DataTypes & from_element_types, const DataTypes & to_element_types) const
{
    ElementWrappers element_wrappers;
    element_wrappers.reserve(from_element_types.size());

    /// Create conversion wrapper for each element in tuple
    for (size_t i = 0; i < from_element_types.size(); ++i)
    {
        const DataTypePtr & from_element_type = from_element_types[i];
        const DataTypePtr & to_element_type = to_element_types[i];
        element_wrappers.push_back(prepareUnpackDictionaries(from_element_type, to_element_type));
    }

    return element_wrappers;
}

FunctionCast::WrapperType FunctionCast::createTupleWrapper(const DataTypePtr & from_type_untyped, const DataTypeTuple * to_type) const
{
    /// Conversion from String through parsing.
    if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
    {
        return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
        {
            return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, settings);
        };
    }

    const auto * from_type = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get());
    if (!from_type)
        throw Exception(ErrorCodes::TYPE_MISMATCH, "CAST AS Tuple can only be performed between tuple types or from String.\n"
                        "Left type: {}, right type: {}", from_type_untyped->getName(), to_type->getName());

    const auto & from_element_types = from_type->getElements();
    const auto & to_element_types = to_type->getElements();

    std::vector<WrapperType> element_wrappers;
    std::vector<std::optional<size_t>> to_reverse_index;

    /// For named tuples allow conversions for tuples with
    /// different sets of elements. If element exists in @to_type
    /// and doesn't exist in @to_type it will be filled by default values.
    if (from_type->hasExplicitNames() && to_type->hasExplicitNames())
    {
        const auto & from_names = from_type->getElementNames();
        std::unordered_map<String, size_t> from_positions;
        from_positions.reserve(from_names.size());
        for (size_t i = 0; i < from_names.size(); ++i)
            from_positions[from_names[i]] = i;

        const auto & to_names = to_type->getElementNames();
        element_wrappers.reserve(to_names.size());
        to_reverse_index.reserve(from_names.size());

        for (size_t i = 0; i < to_names.size(); ++i)
        {
            auto it = from_positions.find(to_names[i]);
            if (it != from_positions.end())
            {
                element_wrappers.emplace_back(prepareUnpackDictionaries(from_element_types[it->second], to_element_types[i]));
                to_reverse_index.emplace_back(it->second);
            }
            else
            {
                element_wrappers.emplace_back();
                to_reverse_index.emplace_back();
            }
        }
    }
    else
    {
        if (from_element_types.size() != to_element_types.size())
            throw Exception(ErrorCodes::TYPE_MISMATCH, "CAST AS Tuple can only be performed between tuple types "
                            "with the same number of elements or from String.\nLeft type: {}, right type: {}",
                            from_type->getName(), to_type->getName());

        element_wrappers = getElementWrappers(from_element_types, to_element_types);
        to_reverse_index.reserve(to_element_types.size());
        for (size_t i = 0; i < to_element_types.size(); ++i)
            to_reverse_index.emplace_back(i);
    }

    return [element_wrappers, from_element_types, to_element_types, to_reverse_index]
        (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
    {
        const auto * col = arguments.front().column.get();

        size_t tuple_size = to_element_types.size();

        if (tuple_size == 0)
        {
            /// Preserve the number of rows for empty tuple columns
            return ColumnTuple::create(col->size());
        }

        const ColumnTuple & column_tuple = typeid_cast<const ColumnTuple &>(*col);

        Columns converted_columns(tuple_size);

        /// invoke conversion for each element
        for (size_t i = 0; i < tuple_size; ++i)
        {
            if (to_reverse_index[i])
            {
                size_t from_idx = *to_reverse_index[i];
                ColumnsWithTypeAndName element = {{column_tuple.getColumns()[from_idx], from_element_types[from_idx], "" }};
                converted_columns[i] = element_wrappers[i](element, to_element_types[i], nullable_source, input_rows_count);
            }
            else
            {
                converted_columns[i] = to_element_types[i]->createColumn()->cloneResized(input_rows_count);
            }
        }

        return ColumnTuple::create(converted_columns);
    };
}

FunctionCast::WrapperType FunctionCast::createQBitWrapper(const DataTypePtr & from_type_untyped, const DataTypeQBit & to_type) const
{
    /// Conversion from String through parsing.
    if (checkAndGetDataType<DataTypeString>(from_type_untyped.get()))
    {
        return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t input_rows_count) -> ColumnPtr
        {
            return ConvertImplGenericFromString<true>::execute(arguments, result_type, column_nullable, input_rows_count, settings);
        };
    }

    const auto * from_qbit_type = checkAndGetDataType<DataTypeQBit>(from_type_untyped.get());
    const auto * from_array_type = checkAndGetDataType<DataTypeArray>(from_type_untyped.get());

    /// TODO: possible to add from Map to QBit too
    /// From another QBit
    if (from_qbit_type)
    {
        if (from_qbit_type->getDimension() != to_type.getDimension())
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "CAST AS between two QBits can only be performed if they have the same number of elements. From: {}, To: {}",
                from_qbit_type->getName(),
                to_type.getName());
        else if (!from_qbit_type->getElementType()->equals(*to_type.getElementType()))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                "CAST AS between two QBits containing different types isn't implemented. From: {}, To: {}",
                from_qbit_type->getName(),
                to_type.getName());
        /// For identical types we create createIdentityWrapper in prepareImpl, so this is unreachable
        UNREACHABLE();
    }

    /// From Array to QBit
    if (from_array_type)
    {
        switch (to_type.getElementSize())
        {
            case 16:
                return createArrayToQBitWrapper<BFloat16>(*from_array_type, to_type);
            case 32:
                return createArrayToQBitWrapper<Float32>(*from_array_type, to_type);
            case 64:
                return createArrayToQBitWrapper<Float64>(*from_array_type, to_type);
            default:
                UNREACHABLE();
        }
    }

    throw Exception(
        ErrorCodes::TYPE_MISMATCH,
        "CAST AS QBit can only be performed from String, Array or another QBit. Left type: {}, right type: {}",
        from_type_untyped->getName(),
        to_type.getName());
}

template <typename FloatType>
ColumnPtr FunctionCast::convertArrayToQBit(
    ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * nullable_source, size_t n, size_t size)
{
    using Word = std::conditional_t<sizeof(FloatType) == 2, UInt16, std::conditional_t<sizeof(FloatType) == 4, UInt32, UInt64>>;

    ColumnPtr src_col = arguments.front().column;
    const auto * col_array = checkAndGetColumn<ColumnArray>(src_col.get());

    if (!col_array)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Unexpected column type {} for Array source when converting to QBit", src_col->getName());

    const auto & offsets = col_array->getOffsets();
    const auto & data = typeid_cast<const ColumnVector<FloatType> &>(col_array->getData()).getData();
    const size_t arrays_count = offsets.size();
    const size_t bytes_per_fixedstring = DataTypeQBit::bitsToBytes(n);
    const size_t padded_dimension = bytes_per_fixedstring * 8;

    /// Use the null map to skip NULL rows — their nested arrays may have default (empty) values
    /// that don't match the expected dimension, but the result will be masked by NULL anyway.
    const NullMap * null_map = nullable_source ? &nullable_source->getNullMapData() : nullptr;

    /// Verify array size matches expected QBit size (skip NULL rows)
    size_t prev_offset = 0;
    for (size_t row = 0; row < arrays_count; ++row)
    {
        size_t array_size = offsets[row] - prev_offset;
        if (!(null_map && (*null_map)[row]) && array_size != n)
            throw Exception(
                ErrorCodes::SIZES_OF_ARRAYS_DONT_MATCH, "Array arguments must have size {} for QBit conversion, got {}", n, array_size);
        prev_offset = offsets[row];
    }

    /// Handle empty input column
    if (arrays_count == 0)
    {
        MutableColumns empty_tuple_columns(size);

        for (size_t i = 0; i < size; ++i)
            empty_tuple_columns[i] = ColumnFixedString::create(bytes_per_fixedstring);

        ColumnPtr tuple = ColumnTuple::create(std::move(empty_tuple_columns));
        return ColumnQBit::create(tuple, n);
    }

    MutableColumns tuple_columns(size);
    for (size_t i = 0; i < size; ++i)
    {
        auto column = ColumnFixedString::create(bytes_per_fixedstring);
        column->reserve(arrays_count);
        tuple_columns[i] = std::move(column);
    }

    prev_offset = 0;
    for (size_t row = 0; row < arrays_count; ++row)
    {
        auto off = offsets[row];

        /// For NULL rows, insert default (zero) values — the result will be masked by NULL.
        if (null_map && (*null_map)[row])
        {
            for (size_t j = 0; j < size; ++j)
                assert_cast<ColumnFixedString &>(*tuple_columns[j]).insertDefault();
            prev_offset = off;
            continue;
        }

        /// Insert default values for each FixedString column and keep pointers to them
        std::vector<char *> row_ptrs(size);
        for (size_t j = 0; j < size; ++j)
        {
            auto & fixed_string_column = assert_cast<ColumnFixedString &>(*tuple_columns[j]);
            fixed_string_column.insertDefault();
            auto & chars = fixed_string_column.getChars();
            row_ptrs[j] = reinterpret_cast<char *>(&chars[chars.size() - bytes_per_fixedstring]);
        }

        /// Transpose bits right inside the FixedStrings
        for (size_t i = 0; i < n; ++i)
        {
            Word w = 0;

            FloatType v = data[prev_offset + i];
            std::memcpy(&w, &v, sizeof(Word));

            SerializationQBit::transposeBits<Word>(w, i, padded_dimension, row_ptrs.data());
        }

        prev_offset = off;
    }

    ColumnPtr tuple = ColumnTuple::create(std::move(tuple_columns));
    return ColumnQBit::create(tuple, n);
}

template <typename T>
FunctionCast::WrapperType FunctionCast::createArrayToQBitWrapper(const DataTypeArray & from_array_type, const DataTypeQBit & to_qbit_type) const
{
    const DataTypePtr & from_nested_type = from_array_type.getNestedType();
    const DataTypePtr & to_nested_type = to_qbit_type.getElementType();
    const size_t dimension = to_qbit_type.getDimension();
    const size_t element_size = to_qbit_type.getElementSize();

    return [nested_function = prepareUnpackDictionaries(from_nested_type, to_nested_type),
            from_nested_type,
            to_nested_type,
            to_array_type = std::make_shared<DataTypeArray>(to_nested_type),
            dimension,
            element_size](
               ColumnsWithTypeAndName & arguments,
               const DataTypePtr & result_type,
               const ColumnNullable * nullable_source,
               size_t /* input_rows_count */) -> ColumnPtr
    {
        const auto & col_array = assert_cast<const ColumnArray &>(*arguments.front().column);

        /// Don't propagate nullable_source into array elements — the inner data column
        /// has a different size (total elements vs. number of rows), and the original
        /// nullable_source column may have a different type than the converted column.
        ColumnsWithTypeAndName nested_columns{{col_array.getDataPtr(), from_nested_type, ""}};
        auto converted_nested = nested_function(nested_columns, to_nested_type, nullptr, nested_columns.front().column->size());
        auto converted_array = ColumnArray::create(converted_nested, col_array.getOffsetsPtr());
        ColumnsWithTypeAndName converted_arguments{{std::move(converted_array), std::make_shared<DataTypeArray>(to_nested_type), ""}};

        /// Pass nullable_source so that convertArrayToQBit can use the null map
        /// to skip NULL rows (whose nested arrays may have default/empty values).
        return convertArrayToQBit<T>(converted_arguments, result_type, nullable_source, dimension, element_size);
    };
}

FunctionCast::WrapperType FunctionCast::createTupleToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
{
    return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
        (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * /*nullable_source*/, size_t /*input_rows_count*/) -> ColumnPtr
    {
        const auto * col = arguments.front().column.get();
        const auto & column_tuple = assert_cast<const ColumnTuple &>(*col);

        Columns offsets(2);
        Columns converted_columns(2);
        for (size_t i = 0; i < 2; ++i)
        {
            const auto & column_array = assert_cast<const ColumnArray &>(column_tuple.getColumn(i));
            ColumnsWithTypeAndName element = {{column_array.getDataPtr(), from_kv_types[i], ""}};
            converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullptr, (element[0].column)->size());
            offsets[i] = column_array.getOffsetsPtr();
        }

        const auto & keys_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets[0]).getData();
        const auto & values_offsets = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets[1]).getData();
        if (keys_offsets != values_offsets)
            throw Exception(ErrorCodes::TYPE_MISMATCH,
                "CAST AS Map can only be performed from tuple of arrays with equal sizes.");

        return ColumnMap::create(converted_columns[0], converted_columns[1], offsets[0]);
    };
}

FunctionCast::WrapperType FunctionCast::createMapToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
{
    return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
        (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * /*nullable_source*/, size_t /*input_rows_count*/) -> ColumnPtr
    {
        const auto * col = arguments.front().column.get();
        const auto & column_map = typeid_cast<const ColumnMap &>(*col);
        const auto & nested_data = column_map.getNestedData();

        Columns converted_columns(2);
        for (size_t i = 0; i < 2; ++i)
        {
            ColumnsWithTypeAndName element = {{nested_data.getColumnPtr(i), from_kv_types[i], ""}};
            /// Don't propagate nullable_source into map key/value elements — the inner data
            /// columns have a different size (total key-value pairs vs. number of rows).
            converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullptr, (element[0].column)->size());
        }

        return ColumnMap::create(converted_columns[0], converted_columns[1], column_map.getNestedColumn().getOffsetsPtr());
    };
}

FunctionCast::WrapperType FunctionCast::createArrayToMapWrapper(const DataTypes & from_kv_types, const DataTypes & to_kv_types) const
{
    return [element_wrappers = getElementWrappers(from_kv_types, to_kv_types), from_kv_types, to_kv_types]
        (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable * /*nullable_source*/, size_t /*input_rows_count*/) -> ColumnPtr
    {
        const auto * col = arguments.front().column.get();
        const auto & column_array = typeid_cast<const ColumnArray &>(*col);
        const auto & nested_data = typeid_cast<const ColumnTuple &>(column_array.getData());

        Columns converted_columns(2);
        for (size_t i = 0; i < 2; ++i)
        {
            ColumnsWithTypeAndName element = {{nested_data.getColumnPtr(i), from_kv_types[i], ""}};
            converted_columns[i] = element_wrappers[i](element, to_kv_types[i], nullptr, (element[0].column)->size());
        }

        return ColumnMap::create(converted_columns[0], converted_columns[1], column_array.getOffsetsPtr());
    };
}

FunctionCast::WrapperType FunctionCast::createMapWrapper(const DataTypePtr & from_type_untyped, const DataTypeMap * to_type) const
{
    if (const auto * from_tuple = checkAndGetDataType<DataTypeTuple>(from_type_untyped.get()))
    {
        if (from_tuple->getElements().size() != 2)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "CAST AS Map from tuple requires 2 elements. "
                "Left type: {}, right type: {}",
                from_tuple->getName(),
                to_type->getName());

        DataTypes from_kv_types;
        const auto & to_kv_types = to_type->getKeyValueTypes();

        for (const auto & elem : from_tuple->getElements())
        {
            const auto * type_array = checkAndGetDataType<DataTypeArray>(elem.get());
            if (!type_array)
                throw Exception(ErrorCodes::TYPE_MISMATCH,
                    "CAST AS Map can only be performed from tuples of array. Got: {}", from_tuple->getName());

            from_kv_types.push_back(type_array->getNestedType());
        }

        return createTupleToMapWrapper(from_kv_types, to_kv_types);
    }
    else if (const auto * from_array = typeid_cast<const DataTypeArray *>(from_type_untyped.get()))
    {
        if (typeid_cast<const DataTypeNothing *>(from_array->getNestedType().get()))
            return [nested = to_type->getNestedType()](ColumnsWithTypeAndName &, const DataTypePtr &, const ColumnNullable *, size_t size)
            {
                return ColumnMap::create(nested->createColumnConstWithDefaultValue(size)->convertToFullColumnIfConst());
            };

        const auto * nested_tuple = typeid_cast<const DataTypeTuple *>(from_array->getNestedType().get());
        if (!nested_tuple || nested_tuple->getElements().size() != 2)
            throw Exception(
                ErrorCodes::TYPE_MISMATCH,
                "CAST AS Map from array requires nested tuple of 2 elements. "
                "Left type: {}, right type: {}",
                from_array->getName(),
                to_type->getName());

        return createArrayToMapWrapper(nested_tuple->getElements(), to_type->getKeyValueTypes());
    }
    else if (const auto * from_type = checkAndGetDataType<DataTypeMap>(from_type_untyped.get()))
    {
        return createMapToMapWrapper(from_type->getKeyValueTypes(), to_type->getKeyValueTypes());
    }
    else
    {
        throw Exception(ErrorCodes::TYPE_MISMATCH, "Unsupported types to CAST AS Map. "
            "Left type: {}, right type: {}", from_type_untyped->getName(), to_type->getName());
    }
}

FunctionCast::WrapperType FunctionCast::createObjectWrapper(const DataTypePtr & from_type, const DataTypeObject * to_object, bool requested_result_is_nullable) const
{
    if (checkAndGetDataType<DataTypeString>(from_type.get()))
    {
        return [this, requested_result_is_nullable](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count)
        {
            if (requested_result_is_nullable && cast_type == CastType::accurateOrNull)
                return ConvertImplGenericFromString<false>::execute(arguments, makeNullable(result_type), nullable_source, input_rows_count, settings);

            return ConvertImplGenericFromString<true>::execute(arguments, result_type, nullable_source, input_rows_count, settings);
        };
    }

    /// Cast Tuple/Object/Map/JSON to JSON type through serializing into JSON string and parsing back into JSON column.
    /// Potentially we can do smarter conversion Tuple -> JSON with type preservation, but it's questionable how exactly Tuple should be
    /// converted to JSON (for example, should we recursively convert nested Array(Tuple) to Array(JSON) or not, should we infer types from String fields, etc).
    /// Proper implementation of a conversion between 2 different JSON types is really complex, because different JSON types
    /// can have different parameters:
    /// - set of typed paths
    /// - set of skip rules
    /// - max_dynamic_paths/max_dynamic_types parameters
    /// Also max_dynamic_paths/max_dynamic_types parameters of nested JSON types depend on current max_dynamic_paths/max_dynamic_types,
    /// so if these parameters are changed, we have to recursively find all nested JSON types and change their parameters as well.
    /// It's all complicates the implementation and last attempt to implement it led to several bugs.
    /// So for now let's perform this conversion through cast to String and parsing new JSON back from it.
    /// It's not effective, but 100% accurate.
    if (checkAndGetDataType<DataTypeTuple>(from_type.get())
        || checkAndGetDataType<DataTypeMap>(from_type.get()) || checkAndGetDataType<DataTypeObject>(from_type.get()))
    {
        return [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count)
        {
            auto json_string = ColumnString::create();
            ColumnStringHelpers::WriteHelper<ColumnString> write_helper(assert_cast<ColumnString &>(*json_string), input_rows_count);
            auto & write_buffer = write_helper.getWriteBuffer();
            FormatSettings format_settings = settings.format_settings;
            auto serialization = arguments[0].type->getDefaultSerialization();
            format_settings.json.quote_64bit_integers = false;
            for (size_t i = 0; i < input_rows_count; ++i)
            {
                serialization->serializeTextJSON(*arguments[0].column, i, write_buffer, format_settings);
                write_helper.finishRow();
            }
            write_helper.finalize();

            ColumnsWithTypeAndName args_with_json_string = {ColumnWithTypeAndName(json_string->getPtr(), std::make_shared<DataTypeString>(), "")};
            return ConvertImplGenericFromString<true>::execute(args_with_json_string, result_type, nullable_source, input_rows_count, settings);
        };
    }

    throw Exception(
        ErrorCodes::TYPE_MISMATCH,
        "Cast to {} can be performed only from String/Map/Object/Tuple/JSON. Got: {}",
        to_object->getSchemaFormatString(),
        from_type->getName());
}

FunctionCast::WrapperType FunctionCast::createVariantToVariantWrapper(const DataTypeVariant & from_variant, const DataTypeVariant & to_variant) const
{
    /// We support only extension of variant type, so, only new types can be added.
    /// For example: Variant(T1, T2) -> Variant(T1, T2, T3) is supported, but Variant(T1, T2) -> Variant(T1, T3) is not supported.
    /// We want to extend Variant type for free without rewriting the data, but we sort data types inside Variant during type creation
    /// (we do it because we want Variant(T1, T2) to be the same as Variant(T2, T1)), but after extension the order of variant types
    /// (and so their discriminators) can be different. For example: Variant(T1, T3) -> Variant(T1, T2, T3).
    /// To avoid full rewrite of discriminators column, ColumnVariant supports it's local order of variant columns (and so local
    /// discriminators) and stores mapping global order -> local order.
    /// So, to extend Variant with new types for free, we should keep old local order for old variants, append new variants and change
    /// mapping global order -> local order according to the new global order.

    /// Create map (new variant type) -> (it's global discriminator in new order).
    const auto & new_variants = to_variant.getVariants();
    std::unordered_map<String, ColumnVariant::Discriminator> new_variant_types_to_new_global_discriminator;
    new_variant_types_to_new_global_discriminator.reserve(new_variants.size());
    for (ColumnVariant::Discriminator i = 0; i != new_variants.size(); ++i)
        new_variant_types_to_new_global_discriminator[new_variants[i]->getName()] = i;

    /// Create set of old variant types.
    const auto & old_variants = from_variant.getVariants();
    std::unordered_map<String, ColumnVariant::Discriminator> old_variant_types_to_old_global_discriminator;
    old_variant_types_to_old_global_discriminator.reserve(old_variants.size());
    for (ColumnVariant::Discriminator i = 0; i != old_variants.size(); ++i)
        old_variant_types_to_old_global_discriminator[old_variants[i]->getName()] = i;

    /// Check that the set of old variants types is a subset of new variant types and collect new global discriminator for each old global discriminator.
    std::unordered_map<ColumnVariant::Discriminator, ColumnVariant::Discriminator> old_global_discriminator_to_new;
    old_global_discriminator_to_new.reserve(old_variants.size());
    for (const auto & [old_variant_type, old_discriminator] : old_variant_types_to_old_global_discriminator)
    {
        auto it = new_variant_types_to_new_global_discriminator.find(old_variant_type);
        if (it == new_variant_types_to_new_global_discriminator.end())
            throw Exception(
                ErrorCodes::CANNOT_CONVERT_TYPE,
                "Cannot convert type {} to {}. Conversion between Variant types is allowed only when new Variant type is an extension "
                "of an initial one", from_variant.getName(), to_variant.getName());
        old_global_discriminator_to_new[old_discriminator] = it->second;
    }

    /// Collect variant types and their global discriminators that should be added to the old Variant to get the new Variant.
    std::vector<std::pair<DataTypePtr, ColumnVariant::Discriminator>> variant_types_and_discriminators_to_add;
    variant_types_and_discriminators_to_add.reserve(new_variants.size() - old_variants.size());
    for (size_t i = 0; i != new_variants.size(); ++i)
    {
        if (!old_variant_types_to_old_global_discriminator.contains(new_variants[i]->getName()))
            variant_types_and_discriminators_to_add.emplace_back(new_variants[i], i);
    }

    return [old_global_discriminator_to_new, variant_types_and_discriminators_to_add]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t) -> ColumnPtr
    {
        const auto & column_variant = assert_cast<const ColumnVariant &>(*arguments.front().column.get());
        size_t num_old_variants = column_variant.getNumVariants();
        Columns new_variant_columns;
        new_variant_columns.reserve(num_old_variants + variant_types_and_discriminators_to_add.size());
        std::vector<ColumnVariant::Discriminator> new_local_to_global_discriminators;
        new_local_to_global_discriminators.reserve(num_old_variants + variant_types_and_discriminators_to_add.size());
        for (ColumnVariant::Discriminator i = 0; i != num_old_variants; ++i)
        {
            new_variant_columns.push_back(column_variant.getVariantPtrByLocalDiscriminator(i));
            new_local_to_global_discriminators.push_back(old_global_discriminator_to_new.at(column_variant.globalDiscriminatorByLocal(i)));
        }

        for (const auto & [new_variant_type, new_global_discriminator] : variant_types_and_discriminators_to_add)
        {
            new_variant_columns.push_back(new_variant_type->createColumn());
            new_local_to_global_discriminators.push_back(new_global_discriminator);
        }

        return ColumnVariant::create(column_variant.getLocalDiscriminatorsPtr(), column_variant.getOffsetsPtr(), new_variant_columns, new_local_to_global_discriminators);
    };
}

FunctionCast::WrapperType FunctionCast::createWrapperIfCanConvert(const DataTypePtr & from, const DataTypePtr & to) const
{
    try
    {
        /// We can avoid try/catch here if we will implement check that 2 types can be cast, but it
        /// requires quite a lot of work. By now let's simply use try/catch.
        /// First, check that we can create a wrapper.
        WrapperType wrapper = prepareUnpackDictionaries(from, to);
        /// Second, check if we can perform a conversion on column with default value.
        /// (we cannot just check empty column as we do some checks only during iteration over rows).
        auto test_col = from->createColumn();
        test_col->insertDefault();
        ColumnsWithTypeAndName column_from = {{test_col->getPtr(), from, "" }};
        wrapper(column_from, to, nullptr, 1);
        return wrapper;
    }
    catch (const Exception &)
    {
        return {};
    }
}

FunctionCast::WrapperType FunctionCast::createVariantToColumnWrapper(const DataTypeVariant & from_variant, const DataTypePtr & to_type) const
{
    const auto & variant_types = from_variant.getVariants();
    std::vector<WrapperType> variant_wrappers;
    variant_wrappers.reserve(variant_types.size());

    /// Create conversion wrapper for each variant.
    for (const auto & variant_type : variant_types)
    {
        WrapperType wrapper;
        if (cast_type == CastType::accurateOrNull)
        {
            /// Create wrapper only if we support conversion from variant to the resulting type.
            wrapper = createWrapperIfCanConvert(variant_type, to_type);
        }
        else
        {
            wrapper = prepareUnpackDictionaries(variant_type, to_type);
        }
        variant_wrappers.push_back(wrapper);
    }

    return [variant_wrappers, variant_types, to_type]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
    {
        const auto & column_variant = assert_cast<const ColumnVariant &>(*arguments.front().column.get());

        /// First, cast each variant to the result type.
        std::vector<ColumnPtr> cast_variant_columns;
        cast_variant_columns.reserve(variant_types.size());
        for (size_t i = 0; i != variant_types.size(); ++i)
        {
            auto variant_col = column_variant.getVariantPtrByGlobalDiscriminator(i);
            ColumnsWithTypeAndName variant = {{variant_col, variant_types[i], "" }};
            const auto & variant_wrapper = variant_wrappers[i];
            ColumnPtr cast_variant;
            /// Check if we have wrapper for this variant.
            if (variant_wrapper)
                cast_variant = variant_wrapper(variant, result_type, nullptr, variant_col->size());
            cast_variant_columns.push_back(std::move(cast_variant));
        }

        /// Second, construct resulting column from cast variant columns according to discriminators.
        const auto & local_discriminators = column_variant.getLocalDiscriminators();
        auto res = result_type->createColumn();
        res->reserve(input_rows_count);
        for (size_t i = 0; i != input_rows_count; ++i)
        {
            auto global_discr = column_variant.globalDiscriminatorByLocal(local_discriminators[i]);
            if (global_discr == ColumnVariant::NULL_DISCRIMINATOR || !cast_variant_columns[global_discr])
                res->insertDefault();
            else
                res->insertFrom(*cast_variant_columns[global_discr], column_variant.offsetAt(i));
        }

        return res;
    };
}

ColumnPtr FunctionCast::createVariantFromDescriptorsAndOneNonEmptyVariant(const DataTypes & variant_types, const ColumnPtr & discriminators, const ColumnPtr & variant, ColumnVariant::Discriminator variant_discr)
{
    Columns variants;
    variants.reserve(variant_types.size());
    for (size_t i = 0; i != variant_types.size(); ++i)
    {
        if (i == variant_discr)
            variants.emplace_back(variant);
        else
            variants.push_back(variant_types[i]->createColumn());
    }

    return ColumnVariant::create(discriminators, variants);
}

FunctionCast::WrapperType FunctionCast::createStringToVariantWrapper() const
{
    return [&](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
    {
        auto column = arguments[0].column->convertToFullColumnIfLowCardinality();
        auto args = arguments;
        args[0].column = column;

        const ColumnNullable * column_nullable = nullptr;
        if (isColumnNullable(*args[0].column))
        {
            column_nullable = assert_cast<const ColumnNullable *>(args[0].column.get());
            args[0].column = column_nullable->getNestedColumnPtr();
        }

        args[0].type = removeNullable(removeLowCardinality(args[0].type));

        if (cast_type == CastType::accurateOrNull)
            return ConvertImplGenericFromString<false>::execute(args, result_type, column_nullable, input_rows_count, settings);
        return ConvertImplGenericFromString<true>::execute(args, result_type, column_nullable, input_rows_count, settings);
    };
}

FunctionCast::WrapperType FunctionCast::createColumnToVariantWrapper(const DataTypePtr & from_type, const DataTypeVariant & to_variant) const
{
    /// We allow converting NULL to Variant(...) as Variant can store NULLs.
    if (from_type->onlyNull())
    {
        return [](ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            auto result_column = result_type->createColumn();
            result_column->insertManyDefaults(input_rows_count);
            return result_column;
        };
    }

    auto variant_discr_opt = to_variant.tryGetVariantDiscriminator(removeNullableOrLowCardinalityNullable(from_type)->getName());
    /// Cast String to Variant through parsing if it's not Variant(String).
    if (settings.cast_string_to_variant_use_inference && isStringOrFixedString(removeNullable(removeLowCardinality(from_type))) && (!variant_discr_opt || to_variant.getVariants().size() > 1))
        return createStringToVariantWrapper();

    if (!variant_discr_opt)
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert type {} to {}. Conversion to Variant allowed only for types from this Variant", from_type->getName(), to_variant.getName());

    return [variant_discr = *variant_discr_opt]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t) -> ColumnPtr
    {
        const auto & result_variant_type = assert_cast<const DataTypeVariant &>(*result_type);
        const auto & variant_types = result_variant_type.getVariants();
        if (const ColumnNullable * col_nullable = typeid_cast<const ColumnNullable *>(arguments.front().column.get()))
        {
            const auto & column = col_nullable->getNestedColumnPtr();
            const auto & null_map = col_nullable->getNullMapData();
            IColumn::Filter filter;
            filter.reserve(column->size());
            auto discriminators = ColumnVariant::ColumnDiscriminators::create();
            auto & discriminators_data = discriminators->getData();
            discriminators_data.reserve(column->size());
            size_t variant_size_hint = 0;
            for (size_t i = 0; i != column->size(); ++i)
            {
                if (null_map[i])
                {
                    discriminators_data.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                    filter.push_back(static_cast<UInt8>(0));
                }
                else
                {
                    discriminators_data.push_back(variant_discr);
                    filter.push_back(static_cast<UInt8>(1));
                    ++variant_size_hint;
                }
            }

            ColumnPtr variant_column;
            /// If there were no NULLs, just use the column.
            if (variant_size_hint == column->size())
                variant_column = column;
            /// Otherwise we should use filtered column.
            else
                variant_column = column->filter(filter, variant_size_hint);
            return createVariantFromDescriptorsAndOneNonEmptyVariant(variant_types, std::move(discriminators), variant_column, variant_discr);
        }
        else if (isColumnLowCardinalityNullable(*arguments.front().column))
        {
            const auto & column = arguments.front().column;

            /// Variant column cannot have LowCardinality(Nullable(...)) variant, as Variant column stores NULLs itself.
            /// We should create a null-map, insert NULL_DISCRIMINATOR on NULL values and filter initial column.
            const auto & col_lc = assert_cast<const ColumnLowCardinality &>(*column);
            const auto & indexes = col_lc.getIndexes();
            auto null_index = col_lc.getDictionary().getNullValueIndex();
            IColumn::Filter filter;
            filter.reserve(col_lc.size());
            auto discriminators = ColumnVariant::ColumnDiscriminators::create();
            auto & discriminators_data = discriminators->getData();
            discriminators_data.reserve(col_lc.size());
            size_t variant_size_hint = 0;
            for (size_t i = 0; i != col_lc.size(); ++i)
            {
                if (indexes.getUInt(i) == null_index)
                {
                    discriminators_data.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                    filter.push_back(static_cast<UInt8>(0));
                }
                else
                {
                    discriminators_data.push_back(variant_discr);
                    filter.push_back(static_cast<UInt8>(1));
                    ++variant_size_hint;
                }
            }

            ColumnPtr variant_column;
            /// If there were no NULLs, we can just clone the column.
            /// We use cloneWithDefaultOnNull to make the dictionary not-nullable in the result column.
            if (variant_size_hint == col_lc.size())
                variant_column = col_lc.cloneWithDefaultOnNull();
            /// Otherwise we should filter column.
            else
                variant_column = assert_cast<const ColumnLowCardinality &>(*column->filter(filter, variant_size_hint)).cloneWithDefaultOnNull();

            return createVariantFromDescriptorsAndOneNonEmptyVariant(variant_types, std::move(discriminators), variant_column, variant_discr);
        }
        else
        {
            const auto & column = arguments.front().column;
            auto discriminators = ColumnVariant::ColumnDiscriminators::create();
            discriminators->getData().resize_fill(column->size(), variant_discr);
            return createVariantFromDescriptorsAndOneNonEmptyVariant(variant_types, std::move(discriminators), column, variant_discr);
        }
    };
}

FunctionCast::WrapperType FunctionCast::createVariantWrapper(const DataTypePtr & from_type, const DataTypePtr & to_type) const
{
    if (const auto * from_variant = checkAndGetDataType<DataTypeVariant>(from_type.get()))
    {
        if (const auto * to_variant = checkAndGetDataType<DataTypeVariant>(to_type.get()))
            return createVariantToVariantWrapper(*from_variant, *to_variant);

        return createVariantToColumnWrapper(*from_variant, to_type);
    }

    return createColumnToVariantWrapper(from_type, assert_cast<const DataTypeVariant &>(*to_type));
}

FunctionCast::WrapperType FunctionCast::createDynamicToColumnWrapper(const DataTypePtr &) const
{
    auto nested_convert = [this](ColumnsWithTypeAndName & args, const DataTypePtr & result_type) -> ColumnPtr
    {
        WrapperType wrapper;
        if (cast_type == CastType::accurateOrNull)
        {
            /// Create wrapper only if we support conversion from variant to the resulting type.
            wrapper = createWrapperIfCanConvert(args[0].type, result_type);
            if (!wrapper)
                return nullptr;
        }
        else
        {
            wrapper = prepareUnpackDictionaries(args[0].type, result_type);
        }

        return wrapper(args, result_type, nullptr, args[0].column->size());
    };

    return [nested_convert]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
    {
        return ConvertImplFromDynamicToColumn::execute(arguments, result_type, input_rows_count, nested_convert);
    };
}

FunctionCast::WrapperType FunctionCast::createStringToDynamicThroughParsingWrapper() const
{
    return [&](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
    {
        auto column = arguments[0].column->convertToFullColumnIfLowCardinality();
        auto args = arguments;
        args[0].column = column;

        const ColumnNullable * column_nullable = nullptr;
        if (isColumnNullable(*args[0].column))
        {
            column_nullable = assert_cast<const ColumnNullable *>(args[0].column.get());
            args[0].column = column_nullable->getNestedColumnPtr();
        }

        args[0].type = removeNullable(removeLowCardinality(args[0].type));

        if (cast_type == CastType::accurateOrNull)
            return ConvertImplGenericFromString<false>::execute(args, result_type, column_nullable, input_rows_count, settings);
        return ConvertImplGenericFromString<true>::execute(args, result_type, column_nullable, input_rows_count, settings);
    };
}

FunctionCast::WrapperType FunctionCast::createVariantToDynamicWrapper(const DataTypeVariant & from_variant_type, const DataTypeDynamic & dynamic_type) const
{
    /// First create extended Variant with shared variant type and cast this Variant to it.
    auto variants_for_dynamic = from_variant_type.getVariants();
    size_t number_of_variants = variants_for_dynamic.size();
    variants_for_dynamic.push_back(ColumnDynamic::getSharedVariantDataType());
    const auto & variant_type_for_dynamic = std::make_shared<DataTypeVariant>(variants_for_dynamic);
    auto old_to_new_variant_wrapper = createVariantToVariantWrapper(from_variant_type, *variant_type_for_dynamic);
    auto max_dynamic_types = dynamic_type.getMaxDynamicTypes();
    return [old_to_new_variant_wrapper, variant_type_for_dynamic, number_of_variants, max_dynamic_types]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * col_nullable, size_t input_rows_count) -> ColumnPtr
    {
        auto variant_column_for_dynamic = old_to_new_variant_wrapper(arguments, result_type, col_nullable, input_rows_count);
        /// If resulting Dynamic column can contain all variants from this Variant column, just create Dynamic column from it.
        if (max_dynamic_types >= number_of_variants)
            return ColumnDynamic::create(variant_column_for_dynamic, variant_type_for_dynamic, max_dynamic_types, max_dynamic_types);

        /// Otherwise some variants should go to the shared variant. Create temporary Dynamic column from this Variant and insert
        /// all data to the resulting Dynamic column, this insertion will do all the logic with shared variant.
        auto tmp_dynamic_column = ColumnDynamic::create(variant_column_for_dynamic, variant_type_for_dynamic, number_of_variants, number_of_variants);
        auto result_dynamic_column = ColumnDynamic::create(max_dynamic_types);
        result_dynamic_column->insertRangeFrom(*tmp_dynamic_column, 0, tmp_dynamic_column->size());
        return result_dynamic_column;
    };
}

FunctionCast::WrapperType FunctionCast::createColumnToDynamicWrapper(const DataTypePtr & from_type, const DataTypeDynamic & dynamic_type) const
{
    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(from_type.get()))
        return createVariantToDynamicWrapper(*variant_type, dynamic_type);

    if (from_type->onlyNull())
        return [](ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            auto result = result_type->createColumn();
            result->insertManyDefaults(input_rows_count);
            return result;
        };

    if (settings.cast_string_to_dynamic_use_inference && isStringOrFixedString(removeNullable(removeLowCardinality(from_type))))
        return createStringToDynamicThroughParsingWrapper();

    /// First, cast column to Variant with 2 variants - the type of the column we cast and shared variant type.
    auto variant_type = std::make_shared<DataTypeVariant>(DataTypes{removeNullableOrLowCardinalityNullable(from_type)});
    auto column_to_variant_wrapper = createColumnToVariantWrapper(from_type, *variant_type);
    /// Second, cast this Variant to Dynamic.
    auto variant_to_dynamic_wrapper = createVariantToDynamicWrapper(*variant_type, dynamic_type);
    return [column_to_variant_wrapper, variant_to_dynamic_wrapper, variant_type]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * col_nullable, size_t input_rows_count) -> ColumnPtr
    {
        auto variant_res = column_to_variant_wrapper(arguments, variant_type, col_nullable, input_rows_count);
        ColumnsWithTypeAndName args = {{variant_res, variant_type, ""}};
        return variant_to_dynamic_wrapper(args, result_type, nullptr, input_rows_count);
    };
}

FunctionCast::WrapperType FunctionCast::createDynamicToDynamicWrapper(const DataTypeDynamic & from_dynamic, const DataTypeDynamic & to_dynamic) const
{
    size_t from_max_types = from_dynamic.getMaxDynamicTypes();
    size_t to_max_types = to_dynamic.getMaxDynamicTypes();
    if (from_max_types == to_max_types)
        return createIdentityWrapper(from_dynamic.getPtr());

    if (to_max_types > from_max_types)
    {
        return [to_max_types]
               (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t) -> ColumnPtr
        {
            const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*arguments[0].column);
            /// We should use the same limit as already used in column and change only global limit.
            /// It's needed because shared variant should contain values only when limit is exceeded,
            /// so if there are already some data, we cannot increase the limit.
            return ColumnDynamic::create(dynamic_column.getVariantColumnPtr(), dynamic_column.getVariantInfo(), dynamic_column.getMaxDynamicTypes(), to_max_types);
        };
    }

    return [to_max_types]
           (ColumnsWithTypeAndName & arguments, const DataTypePtr &, const ColumnNullable *, size_t) -> ColumnPtr
    {
        const auto & dynamic_column = assert_cast<const ColumnDynamic &>(*arguments[0].column);
        /// If real limit in the column is not greater than desired, just use the same variant column.
        if (dynamic_column.getMaxDynamicTypes() <= to_max_types)
            return ColumnDynamic::create(dynamic_column.getVariantColumnPtr(), dynamic_column.getVariantInfo(), dynamic_column.getMaxDynamicTypes(), to_max_types);

        /// Otherwise some variants should go to the shared variant. We try to keep the most frequent variants.
        const auto & variant_info = dynamic_column.getVariantInfo();
        const auto & variants = assert_cast<const DataTypeVariant &>(*variant_info.variant_type).getVariants();
        const auto & statistics = dynamic_column.getStatistics();
        const auto & variant_column = dynamic_column.getVariantColumn();
        auto shared_variant_discr = dynamic_column.getSharedVariantDiscriminator();
        std::vector<std::tuple<size_t, String, DataTypePtr>> variants_with_sizes;
        variants_with_sizes.reserve(variant_info.variant_names.size());
        for (const auto & [name, discr] : variant_info.variant_name_to_discriminator)
        {
            /// Don't include shared variant.
            if (discr == shared_variant_discr)
                continue;

            size_t size = variant_column.getVariantByGlobalDiscriminator(discr).size();
            /// If column has statistics from the data part, use size from it for consistency.
            /// It's important to keep the same dynamic structure of the result column during ALTER.
            if (statistics)
            {
                auto statistics_it = statistics->variants_statistics.find(name);
                if (statistics_it != statistics->variants_statistics.end())
                    size = statistics_it->second;
            }
            variants_with_sizes.emplace_back(size, name, variants[discr]);
        }

        std::sort(variants_with_sizes.begin(), variants_with_sizes.end(), std::greater());
        DataTypes result_variants;
        result_variants.reserve(to_max_types + 1); /// +1 for shared variant.
        /// Add new variants from sorted list until we reach to_max_types.
        for (const auto & [size, name, type] : variants_with_sizes)
        {
            if (result_variants.size() < to_max_types)
                result_variants.push_back(type);
            else
                break;
        }

        /// Add shared variant.
        result_variants.push_back(ColumnDynamic::getSharedVariantDataType());
        /// Create resulting Variant type and Dynamic column.
        auto result_variant_type = std::make_shared<DataTypeVariant>(result_variants);
        auto result_dynamic_column = ColumnDynamic::create(result_variant_type->createColumn(), result_variant_type, to_max_types, to_max_types);
        const auto & result_variant_info = result_dynamic_column->getVariantInfo();
        auto & result_variant_column = result_dynamic_column->getVariantColumn();
        auto result_shared_variant_discr = result_dynamic_column->getSharedVariantDiscriminator();
        /// Create mapping from old discriminators to the new ones.
        std::vector<ColumnVariant::Discriminator> old_to_new_discriminators;
        old_to_new_discriminators.resize(variant_info.variant_name_to_discriminator.size(), result_shared_variant_discr);
        for (const auto & [name, discr] : result_variant_info.variant_name_to_discriminator)
        {
            auto old_discr = variant_info.variant_name_to_discriminator.at(name);
            old_to_new_discriminators[old_discr] = discr;
            /// Reuse old variant column if it's not shared variant.
            if (discr != result_shared_variant_discr)
                result_variant_column.getVariantPtrByGlobalDiscriminator(discr) = variant_column.getVariantPtrByGlobalDiscriminator(old_discr);
        }

        const auto & local_discriminators = variant_column.getLocalDiscriminators();
        const auto & offsets = variant_column.getOffsets();
        const auto & shared_variant = dynamic_column.getSharedVariant();
        auto & result_local_discriminators = result_variant_column.getLocalDiscriminators();
        result_local_discriminators.reserve(local_discriminators.size());
        auto & result_offsets = result_variant_column.getOffsets();
        result_offsets.reserve(offsets.size());
        auto & result_shared_variant = result_dynamic_column->getSharedVariant();
        for (size_t i = 0; i != local_discriminators.size(); ++i)
        {
            auto global_discr = variant_column.globalDiscriminatorByLocal(local_discriminators[i]);
            if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
            {
                result_local_discriminators.push_back(ColumnVariant::NULL_DISCRIMINATOR);
                result_offsets.emplace_back();
            }
            else if (global_discr == shared_variant_discr)
            {
                result_local_discriminators.push_back(result_variant_column.localDiscriminatorByGlobal(result_shared_variant_discr));
                result_offsets.push_back(result_shared_variant.size());
                result_shared_variant.insertFrom(shared_variant, offsets[i]);
            }
            else
            {
                auto result_global_discr = old_to_new_discriminators[global_discr];
                if (result_global_discr == result_shared_variant_discr)
                {
                    result_local_discriminators.push_back(result_variant_column.localDiscriminatorByGlobal(result_shared_variant_discr));
                    result_offsets.push_back(result_shared_variant.size());
                    ColumnDynamic::serializeValueIntoSharedVariant(
                        result_shared_variant,
                        variant_column.getVariantByGlobalDiscriminator(global_discr),
                        variants[global_discr],
                        variants[global_discr]->getDefaultSerialization(),
                        offsets[i]);
                }
                else
                {
                    result_local_discriminators.push_back(result_variant_column.localDiscriminatorByGlobal(result_global_discr));
                    result_offsets.push_back(offsets[i]);
                }
            }
        }

        return result_dynamic_column;
    };
}

FunctionCast::WrapperType FunctionCast::createDynamicWrapper(const DataTypePtr & from_type, const DataTypePtr & to_type) const
{
    if (const auto * from_dynamic = checkAndGetDataType<DataTypeDynamic>(from_type.get()))
    {
        if (const auto * to_dynamic = checkAndGetDataType<DataTypeDynamic>(to_type.get()))
            return createDynamicToDynamicWrapper(*from_dynamic, *to_dynamic);

        return createDynamicToColumnWrapper(to_type);
    }

    return createColumnToDynamicWrapper(from_type, *checkAndGetDataType<DataTypeDynamic>(to_type.get()));
}

template <typename FieldType>
FunctionCast::WrapperType FunctionCast::createEnumWrapper(const DataTypePtr & from_type, const DataTypeEnum<FieldType> * to_type) const
{
    using EnumType = DataTypeEnum<FieldType>;
    using Function = typename FunctionTo<EnumType>::Type;

    if (const auto * from_enum8 = checkAndGetDataType<DataTypeEnum8>(from_type.get()))
        checkEnumToEnumConversion(from_enum8, to_type);
    else if (const auto * from_enum16 = checkAndGetDataType<DataTypeEnum16>(from_type.get()))
        checkEnumToEnumConversion(from_enum16, to_type);

    if (checkAndGetDataType<DataTypeString>(from_type.get()))
        return createStringToEnumWrapper<ColumnString, EnumType>();
    else if (checkAndGetDataType<DataTypeFixedString>(from_type.get()))
        return createStringToEnumWrapper<ColumnFixedString, EnumType>();
    else if (isNativeNumber(from_type))
    {
        if (!settings.check_conversion_from_numbers_to_enum)
        {
            auto function = Function::createFromSettings(settings);
            return createFunctionAdaptor(function, from_type);
        }

        if (checkAndGetDataType<DataTypeInt8>(from_type.get()))
            return createNumberToEnumWrapper<ColumnInt8, EnumType>();
        else if (checkAndGetDataType<DataTypeInt16>(from_type.get()))
            return createNumberToEnumWrapper<ColumnInt16, EnumType>();
        else if (checkAndGetDataType<DataTypeInt32>(from_type.get()))
            return createNumberToEnumWrapper<ColumnInt32, EnumType>();
        else if (checkAndGetDataType<DataTypeInt64>(from_type.get()))
            return createNumberToEnumWrapper<ColumnInt64, EnumType>();
        else if (checkAndGetDataType<DataTypeUInt8>(from_type.get()))
            return createNumberToEnumWrapper<ColumnUInt8, EnumType>();
        else if (checkAndGetDataType<DataTypeUInt16>(from_type.get()))
            return createNumberToEnumWrapper<ColumnUInt16, EnumType>();
        else if (checkAndGetDataType<DataTypeUInt32>(from_type.get()))
            return createNumberToEnumWrapper<ColumnUInt32, EnumType>();
        else if (checkAndGetDataType<DataTypeUInt64>(from_type.get()))
            return createNumberToEnumWrapper<ColumnUInt64, EnumType>();
        else if (checkAndGetDataType<DataTypeFloat32>(from_type.get()))
            return createNumberToEnumWrapper<ColumnFloat32, EnumType>();
        else if (checkAndGetDataType<DataTypeFloat64>(from_type.get()))
            return createNumberToEnumWrapper<ColumnFloat64, EnumType>();
    }
    else if (isEnum(from_type))
    {
        auto function = Function::createFromSettings(settings);
        return createFunctionAdaptor(function, from_type);
    }
    else if (cast_type == CastType::accurateOrNull)
        return createToNullableColumnWrapper();

    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
        from_type->getName(), to_type->getName());
}

template <typename EnumTypeFrom, typename EnumTypeTo>
void FunctionCast::checkEnumToEnumConversion(const EnumTypeFrom * from_type, const EnumTypeTo * to_type) const
{
    const auto & from_values = from_type->getValues();
    const auto & to_values = to_type->getValues();

    using ValueType = std::common_type_t<typename EnumTypeFrom::FieldType, typename EnumTypeTo::FieldType>;
    using NameValuePair = std::pair<std::string, ValueType>;
    using EnumValues = std::vector<NameValuePair>;

    EnumValues name_intersection;
    std::set_intersection(std::begin(from_values), std::end(from_values),
        std::begin(to_values), std::end(to_values), std::back_inserter(name_intersection),
        [] (auto && from, auto && to) { return from.first < to.first; });

    for (const auto & name_value : name_intersection)
    {
        const auto & old_value = name_value.second;
        const auto & new_value = to_type->getValue(name_value.first);
        if (old_value != new_value)
            throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Enum conversion changes value for element '{}' from {} to {}",
                name_value.first, toString(old_value), toString(new_value));
    }
}

template <typename ColumnStringType, typename EnumType>
FunctionCast::WrapperType FunctionCast::createStringToEnumWrapper() const
{
    const char * function_name = cast_name;
    return [function_name] (
        ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
    {
        const auto & first_col = arguments.front().column.get();
        const auto & result_type = typeid_cast<const EnumType &>(*res_type);

        const ColumnStringType * col = typeid_cast<const ColumnStringType *>(first_col);

        if (col && nullable_col && nullable_col->size() != col->size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnNullable is not compatible with original");

        if (col)
        {
            const auto size = col->size();

            auto res = result_type.createColumn();
            auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
            out_data.resize(size);

            auto default_enum_value = result_type.getValues().front().second;

            if (nullable_col)
            {
                for (size_t i = 0; i < size; ++i)
                {
                    if (!nullable_col->isNullAt(i))
                        out_data[i] = result_type.getValue(col->getDataAt(i));
                    else
                        out_data[i] = default_enum_value;
                }
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                    out_data[i] = result_type.getValue(col->getDataAt(i));
            }

            return res;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} as first argument of function {}",
                first_col->getName(), function_name);
    };
}

template <typename ColumnNumberType, typename EnumType>
FunctionCast::WrapperType FunctionCast::createNumberToEnumWrapper() const
{
    using FieldType = EnumType::FieldType;

    return [function_name = cast_name] (
        ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
    {
        const auto & first_col = arguments.front().column.get();
        const auto & result_type = typeid_cast<const EnumType &>(*res_type);

        const ColumnNumberType * col = typeid_cast<const ColumnNumberType *>(first_col);

        if (!col)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} as first argument of function {}",
                first_col->getName(), function_name);

        if (nullable_col && nullable_col->size() != col->size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnNullable is not compatible with original");

        const auto size = col->size();
        const auto & in_data = col->getData();

        auto res = result_type.createColumn();
        auto & out_data = static_cast<typename EnumType::ColumnType &>(*res).getData();
        out_data.resize(size);

        const auto default_enum_value = result_type.getValues().front().second;

        const NullMap * null_map = nullptr;
        if (nullable_col)
            null_map = &nullable_col->getNullMapData();

        for (size_t i = 0; i < size; ++i)
        {
            if (null_map && (*null_map)[i])
            {
                out_data[i] = default_enum_value;
            }
            else
            {
                FieldType converted_value;
                if (!accurate::convertNumeric<typename ColumnNumberType::ValueType, FieldType, false>(in_data[i], converted_value))
                    throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Value {} cannot be converted to enum type", toString(in_data[i]));

                // This checks the number value exists in Enum values.
                /// If not found, an error is thrown.
                result_type.findByValue(converted_value);
                out_data[i] = converted_value;
            }
        }

        return res;
    };
}

template <typename EnumType>
FunctionCast::WrapperType FunctionCast::createEnumToStringWrapper() const
{
    const char * function_name = cast_name;
    return [function_name] (
        ColumnsWithTypeAndName & arguments, const DataTypePtr & res_type, const ColumnNullable * nullable_col, size_t /*input_rows_count*/)
    {
        using ColumnEnumType = typename EnumType::ColumnType;

        const auto & first_col = arguments.front().column.get();
        const auto & first_type = arguments.front().type.get();

        const ColumnEnumType * enum_col = typeid_cast<const ColumnEnumType *>(first_col);
        const EnumType * enum_type = typeid_cast<const EnumType *>(first_type);

        if (enum_col && nullable_col && nullable_col->size() != enum_col->size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "ColumnNullable is not compatible with original");

        if (enum_col && enum_type)
        {
            const auto size = enum_col->size();
            const auto & enum_data = enum_col->getData();

            auto res = res_type->createColumn();

            if (nullable_col)
            {
                for (size_t i = 0; i < size; ++i)
                {
                    if (!nullable_col->isNullAt(i))
                    {
                        const auto & value = enum_type->getNameForValue(enum_data[i]);
                        res->insertData(value.data(), value.size());
                    }
                    else
                        res->insertDefault();
                }
            }
            else
            {
                for (size_t i = 0; i < size; ++i)
                {
                    const auto & value = enum_type->getNameForValue(enum_data[i]);
                    res->insertData(value.data(), value.size());
                }
            }

            return res;
        }
        else
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected column {} as first argument of function {}",
                first_col->getName(), function_name);
    };
}

FunctionCast::WrapperType FunctionCast::prepareUnpackDictionaries(const DataTypePtr & from_type, const DataTypePtr & to_type) const
{
    /// Conversion from/to Variant/Dynamic data type is processed in a special way.
    /// We don't need to remove LowCardinality/Nullable.
    if (isDynamic(to_type) || isDynamic(from_type))
        return createDynamicWrapper(from_type, to_type);

    if (isVariant(to_type) || isVariant(from_type))
        return createVariantWrapper(from_type, to_type);

    const auto * from_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(from_type.get());
    const auto * to_low_cardinality = typeid_cast<const DataTypeLowCardinality *>(to_type.get());
    const auto & from_nested = from_low_cardinality ? from_low_cardinality->getDictionaryType() : from_type;
    const auto & to_nested = to_low_cardinality ? to_low_cardinality->getDictionaryType() : to_type;

    if (from_type->onlyNull())
    {
        if (!to_nested->isNullable() && !isVariant(to_type))
        {
            if (cast_type == CastType::accurateOrNull)
            {
                return createToNullableColumnWrapper();
            }
            else
            {
                throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Cannot convert NULL to a non-nullable type");
            }
        }

        return [](ColumnsWithTypeAndName &, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count)
        {
            return result_type->createColumnConstWithDefaultValue(input_rows_count)->convertToFullColumnIfConst();
        };
    }

    bool skip_not_null_check = false;

    if (from_low_cardinality && from_nested->isNullable() && !to_nested->isNullable())
        /// Disable check for dictionary. Will check that column doesn't contain NULL in wrapper below.
        skip_not_null_check = true;

    auto wrapper = prepareRemoveNullable(from_nested, to_nested, skip_not_null_check);
    if (!from_low_cardinality && !to_low_cardinality)
        return wrapper;

    return [wrapper, from_low_cardinality, to_low_cardinality, skip_not_null_check]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * nullable_source, size_t input_rows_count) -> ColumnPtr
    {
        ColumnsWithTypeAndName args = {arguments[0]};
        auto & arg = args.front();
        auto res_type = result_type;

        ColumnPtr converted_column;

        ColumnPtr res_indexes;
        /// For some types default can't be cast (for example, String to Int). In that case convert column to full.
        bool src_converted_to_full_column = false;

        {
            auto tmp_rows_count = input_rows_count;

            if (to_low_cardinality)
                res_type = to_low_cardinality->getDictionaryType();

            if (from_low_cardinality)
            {
                const auto & col_low_cardinality = typeid_cast<const ColumnLowCardinality &>(*arguments[0].column);

                if (skip_not_null_check && col_low_cardinality.containsNull())
                    throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type");

                arg.column = col_low_cardinality.getDictionary().getNestedColumn();
                arg.type = from_low_cardinality->getDictionaryType();

                /// TODO: Make map with defaults conversion.
                src_converted_to_full_column = !removeNullable(arg.type)->equals(*removeNullable(res_type));
                if (src_converted_to_full_column)
                    arg.column = arg.column->index(col_low_cardinality.getIndexes(), 0);
                else
                    res_indexes = col_low_cardinality.getIndexesPtr();

                tmp_rows_count = arg.column->size();
            }

            /// Perform the requested conversion.
            converted_column = wrapper(args, res_type, nullable_source, tmp_rows_count);
        }

        if (to_low_cardinality)
        {
            auto res_column = to_low_cardinality->createColumn();
            auto & col_low_cardinality = typeid_cast<ColumnLowCardinality &>(*res_column);

            if (from_low_cardinality && !src_converted_to_full_column)
                col_low_cardinality.insertRangeFromDictionaryEncodedColumn(*converted_column, *res_indexes);
            else
                col_low_cardinality.insertRangeFromFullColumn(*converted_column, 0, converted_column->size());

            return res_column;
        }
        else if (!src_converted_to_full_column)
            return converted_column->index(*res_indexes, 0);
        else
            return converted_column;
    };
}

FunctionCast::WrapperType FunctionCast::prepareRemoveNullable(const DataTypePtr & from_type, const DataTypePtr & to_type, bool skip_not_null_check) const
{
    /// Determine whether pre-processing and/or post-processing must take place during conversion.

    bool source_is_nullable = from_type->isNullable();
    bool result_is_nullable = to_type->isNullable();

    auto wrapper = prepareImpl(removeNullable(from_type), removeNullable(to_type), result_is_nullable);

    if (result_is_nullable)
    {
        return [wrapper, source_is_nullable]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            /// Create a temporary columns on which to perform the operation.
            const auto & nullable_type = static_cast<const DataTypeNullable &>(*result_type);
            const auto & nested_type = nullable_type.getNestedType();

            ColumnsWithTypeAndName tmp_args;
            if (source_is_nullable)
                tmp_args = createBlockWithNestedColumns(arguments);
            else
                tmp_args = arguments;

            const ColumnNullable * nullable_source = nullptr;

            /// Add original ColumnNullable for createStringToEnumWrapper()
            if (source_is_nullable)
            {
                if (arguments.size() != 1)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid number of arguments");
                nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
            }

            /// Perform the requested conversion.
            auto tmp_res = wrapper(tmp_args, nested_type, nullable_source, input_rows_count);

            /// May happen in fuzzy tests. For debug purpose.
            if (!tmp_res)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Couldn't convert {} to {} in prepareRemoveNullable wrapper.",
                                arguments[0].type->getName(), nested_type->getName());

            return wrapInNullable(tmp_res, arguments, nested_type, input_rows_count);
        };
    }
    else if (source_is_nullable)
    {
        /// Conversion from Nullable to non-Nullable.

        return [wrapper, skip_not_null_check]
            (ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
        {
            auto tmp_args = createBlockWithNestedColumns(arguments);
            auto nested_type = removeNullable(result_type);

            /// Check that all values are not-NULL.
            /// Check can be skipped in case if LowCardinality dictionary is transformed.
            /// In that case, correctness will be checked beforehand.
            if (!skip_not_null_check)
            {
                const auto & col = arguments[0].column;
                const auto & nullable_col = assert_cast<const ColumnNullable &>(*col);
                const auto & null_map = nullable_col.getNullMapData();

                if (!memoryIsZero(null_map.data(), 0, null_map.size()))
                    throw Exception(ErrorCodes::CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN, "Cannot convert NULL value to non-Nullable type");
            }
            const ColumnNullable * nullable_source = typeid_cast<const ColumnNullable *>(arguments.front().column.get());
            return wrapper(tmp_args, nested_type, nullable_source, input_rows_count);
        };
    }
    else
        return wrapper;
}

FunctionCast::WrapperType FunctionCast::prepareImpl(const DataTypePtr & from_type, const DataTypePtr & to_type, bool requested_result_is_nullable) const
{
    if (isUInt8(from_type) && isBool(to_type))
        return createUInt8ToBoolWrapper(from_type, to_type);

    /// We can cast IPv6 into IPv6, IPv4 into IPv4, but we should not allow to cast FixedString(16) into IPv6 as part of identity cast
    bool safe_convert_custom_types = true;

    if (const auto * to_type_custom_name = to_type->getCustomName())
        safe_convert_custom_types = from_type->getCustomName() && from_type->getCustomName()->getName() == to_type_custom_name->getName();
    else if (const auto * from_type_custom_name = from_type->getCustomName())
        safe_convert_custom_types = to_type->getCustomName() && from_type_custom_name->getName() == to_type->getCustomName()->getName();

    if (from_type->equals(*to_type) && safe_convert_custom_types)
    {
        /// We can only use identity conversion for DataTypeAggregateFunction when they are strictly equivalent.
        if (typeid_cast<const DataTypeAggregateFunction *>(from_type.get()))
        {
            if (DataTypeAggregateFunction::strictEquals(from_type, to_type))
                return createIdentityWrapper(from_type);
        }
        else
            return createIdentityWrapper(from_type);
    }
    else if (WhichDataType(from_type).isNothing())
        return createNothingWrapper(to_type.get());

    WrapperType ret;

    auto make_default_wrapper = [&](const auto & types) -> bool
    {
        using Types = std::decay_t<decltype(types)>;
        using ToDataType = typename Types::LeftType;

        if constexpr (is_any_of<ToDataType,
            DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
            DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
            DataTypeBFloat16, DataTypeFloat32, DataTypeFloat64,
            DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeTime,
            DataTypeUUID, DataTypeIPv4, DataTypeIPv6>)
        {
            ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
            return true;
        }
        if constexpr (std::is_same_v<ToDataType, DataTypeUInt8>)
        {
            if (isBool(to_type))
                ret = createBoolWrapper<ToDataType>(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
            else
                ret = createWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
            return true;
        }
        if constexpr (
            std::is_same_v<ToDataType, DataTypeEnum8> ||
            std::is_same_v<ToDataType, DataTypeEnum16>)
        {
            ret = createEnumWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()));
            return true;
        }
        if constexpr (is_any_of<ToDataType,
            DataTypeDecimal<Decimal32>, DataTypeDecimal<Decimal64>,
            DataTypeDecimal<Decimal128>, DataTypeDecimal<Decimal256>,
            DataTypeDateTime64, DataTypeTime64>)
        {
            ret = createDecimalWrapper(from_type, checkAndGetDataType<ToDataType>(to_type.get()), requested_result_is_nullable);
            return true;
        }

        return false;
    };

    auto make_custom_serialization_wrapper = [&, this](const auto & types) -> bool
    {
        using Types = std::decay_t<decltype(types)>;
        using ToDataType = typename Types::RightType;
        using FromDataType = typename Types::LeftType;

        if constexpr (WhichDataType(FromDataType::type_id).isStringOrFixedString())
        {
            if constexpr (std::is_same_v<ToDataType, DataTypeIPv4>)
            {
                ret = [this, requested_result_is_nullable](
                          ColumnsWithTypeAndName & arguments,
                          const DataTypePtr & result_type,
                          const ColumnNullable * column_nullable,
                          size_t) -> ColumnPtr
                {
                    if (!WhichDataType(result_type).isIPv4())
                        throw Exception(ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected IPv4", result_type->getName());

                    const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                    if (requested_result_is_nullable)
                        return convertToIPv4<IPStringToNumExceptionMode::Null>(arguments[0].column, null_map);
                    else if (settings.cast_ipv4_ipv6_default_on_conversion_error || settings.input_format_ipv4_default_on_conversion_error)
                        return convertToIPv4<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                    else
                        return convertToIPv4<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                };

                return true;
            }

            if constexpr (std::is_same_v<ToDataType, DataTypeIPv6>)
            {
                ret = [this, requested_result_is_nullable](
                          ColumnsWithTypeAndName & arguments,
                          const DataTypePtr & result_type,
                          const ColumnNullable * column_nullable,
                          size_t) -> ColumnPtr
                {
                    if (!WhichDataType(result_type).isIPv6())
                        throw Exception(
                            ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected IPv6", result_type->getName());

                    const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                    if (requested_result_is_nullable)
                        return convertToIPv6<IPStringToNumExceptionMode::Null>(arguments[0].column, null_map);
                    else if (settings.cast_ipv4_ipv6_default_on_conversion_error || settings.input_format_ipv6_default_on_conversion_error)
                        return convertToIPv6<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                    else
                        return convertToIPv6<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
                };

                return true;
            }

            if (to_type->getCustomSerialization() && to_type->getCustomName())
            {
                ret = [requested_result_is_nullable, this](
                          ColumnsWithTypeAndName & arguments,
                          const DataTypePtr & result_type,
                          const ColumnNullable * column_nullable,
                          size_t input_rows_count) -> ColumnPtr
                {
                    auto wrapped_result_type = result_type;
                    if (requested_result_is_nullable)
                        wrapped_result_type = makeNullable(result_type);
                    if (this->cast_type == CastType::accurateOrNull)
                        return ConvertImplGenericFromString<false>::execute(
                            arguments, wrapped_result_type, column_nullable, input_rows_count, settings);
                    return ConvertImplGenericFromString<true>::execute(
                        arguments, wrapped_result_type, column_nullable, input_rows_count, settings);
                };
                return true;
            }
        }
        else if constexpr (WhichDataType(FromDataType::type_id).isIPv6() && WhichDataType(ToDataType::type_id).isIPv4())
        {
            ret = [this, requested_result_is_nullable](
                            ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable * column_nullable, size_t)
                    -> ColumnPtr
            {
                if (!WhichDataType(result_type).isIPv4())
                    throw Exception(
                        ErrorCodes::TYPE_MISMATCH, "Wrong result type {}. Expected IPv4", result_type->getName());

                const auto * null_map = column_nullable ? &column_nullable->getNullMapData() : nullptr;
                if (requested_result_is_nullable)
                    return convertIPv6ToIPv4<IPStringToNumExceptionMode::Null>(arguments[0].column, null_map);
                else if (settings.cast_ipv4_ipv6_default_on_conversion_error)
                    return convertIPv6ToIPv4<IPStringToNumExceptionMode::Default>(arguments[0].column, null_map);
                else
                    return convertIPv6ToIPv4<IPStringToNumExceptionMode::Throw>(arguments[0].column, null_map);
            };

            return true;
        }

        if constexpr (WhichDataType(ToDataType::type_id).isStringOrFixedString())
        {
            if constexpr (WhichDataType(FromDataType::type_id).isEnum())
            {
                ret = createEnumToStringWrapper<FromDataType>();
                return true;
            }
            else if (from_type->getCustomSerialization())
            {
                ret = [this](ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, const ColumnNullable *, size_t input_rows_count) -> ColumnPtr
                {
                    return ConvertImplGenericToString<typename ToDataType::ColumnType>::execute(arguments, result_type, input_rows_count, settings.format_settings);
                };
                return true;
            }
        }

        return false;
    };

    if (callOnTwoTypeIndexes(from_type->getTypeId(), to_type->getTypeId(), make_custom_serialization_wrapper))
        return ret;

    if (callOnIndexAndDataType<void>(to_type->getTypeId(), make_default_wrapper))
        return ret;

    switch (to_type->getTypeId())
    {
        case TypeIndex::String:
            return createStringWrapper(from_type);
        case TypeIndex::FixedString:
            return createFixedStringWrapper(from_type, checkAndGetDataType<DataTypeFixedString>(to_type.get())->getN());
        case TypeIndex::Array:
            return createArrayWrapper(from_type, static_cast<const DataTypeArray &>(*to_type));
        case TypeIndex::Tuple:
            return createTupleWrapper(from_type, checkAndGetDataType<DataTypeTuple>(to_type.get()));
        case TypeIndex::QBit:
            return createQBitWrapper(from_type, static_cast<const DataTypeQBit &>(*to_type));
        case TypeIndex::Map:
            return createMapWrapper(from_type, checkAndGetDataType<DataTypeMap>(to_type.get()));
        case TypeIndex::Object:
            return createObjectWrapper(from_type, checkAndGetDataType<DataTypeObject>(to_type.get()), requested_result_is_nullable);
        case TypeIndex::AggregateFunction:
            return createAggregateFunctionWrapper(from_type, checkAndGetDataType<DataTypeAggregateFunction>(to_type.get()));
        case TypeIndex::Interval:
            return createIntervalWrapper(from_type, checkAndGetDataType<DataTypeInterval>(to_type.get())->getKind());
        default:
            break;
    }

    if (cast_type == CastType::accurateOrNull)
        return createToNullableColumnWrapper();
    else
        throw Exception(ErrorCodes::CANNOT_CONVERT_TYPE, "Conversion from {} to {} is not supported",
            from_type->getName(), to_type->getName());
}

}

FunctionBasePtr createFunctionBaseCast(
    ContextPtr context,
    const char * name,
    const ColumnsWithTypeAndName & arguments,
    const DataTypePtr & return_type,
    std::optional<CastDiagnostic> diagnostic,
    CastType cast_type,
    FormatSettings::DateTimeOverflowBehavior date_time_overflow_behavior)
{
    DataTypes data_types(arguments.size());

    for (size_t i = 0; i < arguments.size(); ++i)
        data_types[i] = arguments[i].type;

    detail::FunctionCast::MonotonicityForRange monotonicity;

    if (isEnum(arguments.front().type)
        && castTypeToEither<DataTypeEnum8, DataTypeEnum16>(return_type.get(), [&](auto & type)
        {
            monotonicity = detail::FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }
    else if (castTypeToEither<
        DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64, DataTypeUInt128, DataTypeUInt256,
        DataTypeInt8, DataTypeInt16, DataTypeInt32, DataTypeInt64, DataTypeInt128, DataTypeInt256,
        DataTypeFloat32, DataTypeFloat64,
        DataTypeDate, DataTypeDate32, DataTypeDateTime, DataTypeDateTime64, DataTypeTime, DataTypeTime64,
        DataTypeString>(recursiveRemoveLowCardinality(return_type).get(), [&](auto & type)
        {
            monotonicity = detail::FunctionTo<std::decay_t<decltype(type)>>::Type::Monotonic::get;
            return true;
        }))
    {
    }

    return std::make_unique<detail::FunctionCast>(
        context, name, std::move(monotonicity), data_types, return_type, diagnostic, cast_type, date_time_overflow_behavior);
}


#if USE_EMBEDDED_COMPILER

namespace detail
{

bool castType(const IDataType * type, auto && f)
{
    using Types = TypeList<
        DataTypeUInt8,
        DataTypeUInt16,
        DataTypeUInt32,
        DataTypeUInt64,
        DataTypeUInt128,
        DataTypeUInt256,
        DataTypeInt8,
        DataTypeInt16,
        DataTypeInt32,
        DataTypeInt64,
        DataTypeInt128,
        DataTypeInt256,
        DataTypeFloat32,
        DataTypeFloat64,
        DataTypeDecimal32,
        DataTypeDecimal64,
        DataTypeDecimal128,
        DataTypeDecimal256,
        DataTypeDate,
        DataTypeDateTime,
        DataTypeFixedString,
        DataTypeString,
        DataTypeInterval>;
    return castTypeToEither(Types{}, type, std::forward<decltype(f)>(f));
}

template <typename F>
bool castBothTypes(const IDataType * left, const IDataType * right, F && f)
{
    return castType(left, [&](const auto & left_) { return castType(right, [&](const auto & right_) { return f(left_, right_); }); });
}

bool convertIsCompilableImpl(const DataTypes & types, const DataTypePtr & result_type)
{
    if (types.empty())
        return false;

    if (!canBeNativeType(types[0]) || !canBeNativeType(result_type))
        return false;

    return castBothTypes(
        types[0].get(),
        result_type.get(),
        [](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                    return true;
                else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                    return true;
                else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                    return true;
            }
            return false;
        });
}

llvm::Value * convertCompileImpl(llvm::IRBuilderBase & builder, const ValuesWithType & arguments, const DataTypePtr & result_type)
{
    llvm::Value * result = nullptr;
    castBothTypes(
        arguments[0].type.get(),
        result_type.get(),
        [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;
            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftFieldType = typename LeftDataType::FieldType;
                using RightFieldType = typename RightDataType::FieldType;

                if (isBool(right.getPtr()))
                {
                    /// nativeBoolCast returns i1, but Bool (UInt8) needs i8
                    auto * bool_value = nativeBoolCast(builder, arguments[0]);
                    result = builder.CreateZExt(bool_value, builder.getInt8Ty());
                    return true;
                }

                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    result = nativeCast(builder, arguments[0], right.getPtr());
                    return true;
                }
                else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                {
                    auto scale = right.getScale();
                    auto multiplier = DecimalUtils::scaleMultiplier<NativeType<RightFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<LeftFieldType>)
                    {
                        /// left type is float and right type is decimal
                        auto * from_type = arguments[0].value->getType();
                        auto * from_value = arguments[0].value;
                        result = builder.CreateFMul(from_value, llvm::ConstantFP::get(from_type, static_cast<LeftFieldType>(multiplier)));
                        result = nativeCast(builder, left.getPtr(), result, right.getPtr());
                    }
                    else
                    {
                        /// left type is integer and right type is decimal
                        auto * from_value = nativeCast(builder, arguments[0], right.getPtr());
                        auto * multiplier_value = getNativeValue(builder, right.getPtr(), RightFieldType(multiplier));
                        result = builder.CreateMul(from_value, multiplier_value);
                    }
                    return true;
                }
                else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    auto scale = left.getScale();
                    auto divider = DecimalUtils::scaleMultiplier<NativeType<LeftFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<RightFieldType>)
                    {
                        DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
                        auto * from_value = nativeCast(builder, arguments[0], double_type);
                        auto * d = llvm::ConstantFP::get(builder.getDoubleTy(), static_cast<double>(divider));
                        result = nativeCast(builder, double_type, builder.CreateFDiv(from_value, d), right.getPtr());
                    }
                    else
                    {
                        llvm::Value * d = nullptr;
                        auto * from_type = arguments[0].value->getType();
                        if constexpr (std::is_integral_v<NativeType<LeftFieldType>>)
                            d = llvm::ConstantInt::get(from_type, static_cast<uint64_t>(divider), true);
                        else
                        {
                            llvm::APInt v(from_type->getIntegerBitWidth(), divider.items);
                            d = llvm::ConstantInt::get(from_type, v);
                        }

                        auto * from_value = arguments[0].value;
                        auto * whole_part = builder.CreateSDiv(from_value, d);
                        result = nativeCast(builder, left.getPtr(), whole_part, right.getPtr());
                    }
                    return true;
                }
            }

            return false;
        });

    return result;
}


bool FunctionCast::isCompilable() const
{
    if (getName() != "CAST" || argument_types.size() != 2)
        return false;

    const auto & input_type = argument_types[0];
    const auto & result_type = getResultType();
    auto denull_input_type = removeNullable(input_type);
    auto denull_result_type = removeNullable(result_type);
    if (!canBeNativeType(denull_input_type) || !canBeNativeType(denull_result_type))
        return false;

    return castBothTypes(denull_input_type.get(), denull_result_type.get(), [](const auto & left, const auto & right)
    {
        using LeftDataType = std::decay_t<decltype(left)>;
        using RightDataType = std::decay_t<decltype(right)>;
        if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
        {
            if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                return true;
            else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                return true;
            else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                return true;
        }
        return false;
    });
}

llvm::Value * FunctionCast::compile(llvm::IRBuilderBase & builder, const ValuesWithType & arguments) const
{
    const auto & input_type = arguments[0].type;
    const auto & result_type = getResultType();

    llvm::Value * result_value = nullptr;
    llvm::Value * input_value = arguments[0].value;
    llvm::Value * input_isnull = nullptr;
    if (input_type->isNullable())
    {
        input_isnull = builder.CreateExtractValue(input_value, {1});
        input_value = builder.CreateExtractValue(input_value, {0});
    }

    auto denull_input_type = removeNullable(input_type);
    auto denull_result_type = removeNullable(result_type);

    castBothTypes(
        denull_input_type.get(),
        denull_result_type.get(),
        [&](const auto & left, const auto & right)
        {
            using LeftDataType = std::decay_t<decltype(left)>;
            using RightDataType = std::decay_t<decltype(right)>;

            if constexpr (IsDataTypeDecimalOrNumber<LeftDataType> && IsDataTypeDecimalOrNumber<RightDataType>)
            {
                using LeftFieldType = typename LeftDataType::FieldType;
                using RightFieldType = typename RightDataType::FieldType;

                if (isBool(right.getPtr()))
                {
                    /// nativeBoolCast returns i1, but Bool (UInt8) needs i8
                    auto * bool_value = nativeBoolCast(builder, left.getPtr(), input_value);
                    result_value = builder.CreateZExt(bool_value, builder.getInt8Ty());
                    return true;
                }

                if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    result_value = nativeCast(builder, left.getPtr(), input_value, right.getPtr());
                    return true;
                }
                else if constexpr (IsDataTypeNumber<LeftDataType> && IsDataTypeDecimal<RightDataType>)
                {
                    auto scale = right.getScale();
                    auto multiplier = DecimalUtils::scaleMultiplier<NativeType<RightFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<LeftFieldType>)
                    {
                        /// left type is float and right type is decimal
                        auto * from_type = toNativeType(builder, left);
                        result_value = builder.CreateFMul(input_value, llvm::ConstantFP::get(from_type, static_cast<LeftFieldType>(multiplier)));
                        result_value = nativeCast(builder, left.getPtr(), result_value, right.getPtr());
                    }
                    else
                    {
                        /// left type is integer and right type is decimal
                        auto * from_value = nativeCast(builder, left.getPtr(), input_value, right.getPtr());
                        auto * multiplier_value = getNativeValue(builder, right.getPtr(), RightFieldType(multiplier));
                        result_value = builder.CreateMul(from_value, multiplier_value);
                    }
                    return true;
                }
                else if constexpr (IsDataTypeDecimal<LeftDataType> && IsDataTypeNumber<RightDataType>)
                {
                    auto scale = left.getScale();
                    auto divider = DecimalUtils::scaleMultiplier<NativeType<LeftFieldType>>(scale);
                    if constexpr (std::is_floating_point_v<RightFieldType>)
                    {
                        DataTypePtr double_type = std::make_shared<DataTypeFloat64>();
                        auto * from_value = nativeCast(builder, left.getPtr(), input_value, double_type);
                        auto * d = llvm::ConstantFP::get(builder.getDoubleTy(), static_cast<double>(divider));
                        result_value = nativeCast(builder, double_type, builder.CreateFDiv(from_value, d), right.getPtr());
                    }
                    else
                    {
                        llvm::Value * d = nullptr;
                        auto * from_type = toNativeType(builder, left.getPtr());
                        if constexpr (std::is_integral_v<NativeType<LeftFieldType>>)
                            d = llvm::ConstantInt::get(from_type, static_cast<uint64_t>(divider), true);
                        else
                        {
                            llvm::APInt v(from_type->getIntegerBitWidth(), divider.items);
                            d = llvm::ConstantInt::get(from_type, v);
                        }

                        auto * from_value = input_value;
                        auto * whole_part = builder.CreateSDiv(from_value, d);
                        result_value = nativeCast(builder, left.getPtr(), whole_part, right.getPtr());
                    }
                    return true;
                }
            }
            return false;
        });

        if (!result_value)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot compile CAST function for types {} -> {}",
                input_type->getName(),
                result_type->getName());

        if (result_type->isNullable())
        {
            llvm::Value * result_isnull = input_isnull ? input_isnull : llvm::ConstantInt::get(builder.getInt1Ty(), 0);
            auto * nullable_structure_type = toNativeType(builder, result_type);
            llvm::Value * nullable_structure_value = llvm::Constant::getNullValue(nullable_structure_type);
            nullable_structure_value = builder.CreateInsertValue(nullable_structure_value, result_value, {0});
            return builder.CreateInsertValue(nullable_structure_value, result_isnull, {1});
        }
        else
        {
            return result_value;
        }
}

}
#endif

}
