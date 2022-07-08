#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>

#include <Common/typeid_cast.h>
#include <Common/assert_cast.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnNullable.h>

#include <Access/Common/AccessFlags.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/castColumn.h>

#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <base/range.h>

#include <type_traits>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNSUPPORTED_METHOD;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
    extern const int TYPE_MISMATCH;
}


/** Functions that use plug-ins (external) dictionaries_loader.
  *
  * Get the value of the attribute of the specified type.
  *     dictGetType(dictionary, attribute, id),
  *         Type - placeholder for the type name, any numeric and string types are currently supported.
  *        The type must match the actual attribute type with which it was declared in the dictionary structure.
  *
  * Get an array of identifiers, consisting of the source and parents chain.
  *  dictGetHierarchy(dictionary, id).
  *
  * Is the first identifier the child of the second.
  *  dictIsIn(dictionary, child_id, parent_id).
  */


class FunctionDictHelper : WithContext
{
public:
    explicit FunctionDictHelper(ContextPtr context_) : WithContext(context_) {}

    std::shared_ptr<const IDictionary> getDictionary(const String & dictionary_name)
    {
        auto current_context = getContext();
        auto dict = current_context->getExternalDictionariesLoader().getDictionary(dictionary_name, current_context);

        if (!access_checked)
        {
            current_context->checkAccess(AccessType::dictGet, dict->getDatabaseOrNoDatabaseTag(), dict->getDictionaryID().getTableName());
            access_checked = true;
        }

        return dict;
    }

    std::shared_ptr<const IDictionary> getDictionary(const ColumnPtr & column)
    {
        const auto * dict_name_col = checkAndGetColumnConst<ColumnString>(column.get());

        if (!dict_name_col)
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Expected const String column");

        return getDictionary(dict_name_col->getValue<String>());
    }

    static const DictionaryAttribute & getDictionaryHierarchicalAttribute(const std::shared_ptr<const IDictionary> & dictionary)
    {
        const auto & dictionary_structure = dictionary->getStructure();
        auto hierarchical_attribute_index_optional = dictionary_structure.hierarchical_attribute_index;

        if (!dictionary->hasHierarchy() || !hierarchical_attribute_index_optional.has_value())
            throw Exception(ErrorCodes::UNSUPPORTED_METHOD,
                "Dictionary {} does not support hierarchy",
                dictionary->getFullName());

        size_t hierarchical_attribute_index = *hierarchical_attribute_index_optional;
        const auto & hierarchical_attribute = dictionary_structure.attributes[hierarchical_attribute_index];

        return hierarchical_attribute;
    }

    bool isDictGetFunctionInjective(const Block & sample_columns)
    {
        /// Assume non-injective by default
        if (!sample_columns)
            return false;

        if (sample_columns.columns() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Wrong arguments count");

        const auto * dict_name_col = checkAndGetColumnConst<ColumnString>(sample_columns.getByPosition(0).column.get());
        if (!dict_name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "First argument of function dictGet must be a constant string");

        const auto * attr_name_col = checkAndGetColumnConst<ColumnString>(sample_columns.getByPosition(1).column.get());
        if (!attr_name_col)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Second argument of function dictGet must be a constant string");

        const auto & dictionary_name = dict_name_col->getValue<String>();
        const auto & attribute_name = attr_name_col->getValue<String>();

        return getDictionary(dictionary_name)->isInjective(attribute_name);
    }

    DictionaryStructure getDictionaryStructure(const String & dictionary_name) const
    {
        return getContext()->getExternalDictionariesLoader().getDictionaryStructure(dictionary_name, getContext());
    }

private:
    /// Access cannot be not granted, since in this case checkAccess() will throw and access_checked will not be updated.
    std::atomic<bool> access_checked = false;

    /// We must not cache dictionary or dictionary's structure here, because there are places
    /// where ExpressionActionsPtr is cached (StorageDistributed caching it for sharding_key_expr and
    /// optimize_skip_unused_shards), and if the dictionary will be cached within "query" then
    /// cached ExpressionActionsPtr will always have first version of the query and the dictionary
    /// will not be updated after reload (see https://github.com/ClickHouse/ClickHouse/pull/16205)
};


class FunctionDictHas final : public IFunction
{
public:
    static constexpr auto name = "dictHas";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictHas>(context);
    }

    explicit FunctionDictHas(ContextPtr context_) : helper(context_) {}

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const final { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() < 2)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Wrong argument count for function {}",
                getName());

        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function, expected a string",
                arguments[0]->getName(),
                getName());

        return std::make_shared<DataTypeUInt8>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /** Do not require existence of the dictionary if the function is called for empty columns.
          * This is needed to allow successful query analysis on a server,
          *  that is the initiator of a distributed query,
          *  in the case when the function will be invoked for real data only at the remote servers.
          * This feature is controversial and implemented specially
          *  for backward compatibility with the case in the Banner System application.
          */
        if (input_rows_count == 0)
            return result_type->createColumn();

        String dictionary_name;

        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
            dictionary_name = name_col->getValue<String>();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a const string.",
                arguments[0].type->getName(),
                getName());

        auto dictionary = helper.getDictionary(dictionary_name);
        auto dictionary_key_type = dictionary->getKeyType();
        auto dictionary_special_key_type = dictionary->getSpecialKeyType();

        const auto & key_column_with_type = arguments[1];
        auto key_column = key_column_with_type.column;
        auto key_column_type = key_column_with_type.type;

        ColumnPtr range_col;
        DataTypePtr range_col_type;

        if (dictionary_special_key_type == DictionarySpecialKeyType::Range)
        {
            if (arguments.size() != 3)
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Wrong argument count for function {} when dictionary has key type range",
                    getName());

            range_col = arguments[2].column;
            range_col_type = arguments[2].type;

            if (!(range_col_type->isValueRepresentedByInteger() && range_col_type->getSizeOfValueInMemory() <= sizeof(Int64)))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of fourth argument of function {} must be convertible to Int64.",
                    range_col_type->getName(),
                    getName());
        }

        Columns key_columns;
        DataTypes key_types;

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            key_columns = {key_column};
            key_types = {key_column_with_type.type};
        }
        else if (dictionary_key_type == DictionaryKeyType::Complex)
        {
            /// Functions in external dictionaries_loader only support full-value (not constant) columns with keys.
            key_column = key_column->convertToFullColumnIfConst();

            if (isTuple(key_column_type))
            {
                key_columns = assert_cast<const ColumnTuple &>(*key_column).getColumnsCopy();
                key_types = assert_cast<const DataTypeTuple &>(*key_column_type).getElements();
            }
            else
            {
                size_t keys_size = dictionary->getStructure().getKeysSize();

                if (keys_size > 1)
                {
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Third argument of function {} must be tuple when dictionary is complex and key contains more than 1 attribute."
                        "Actual type {}.",
                        getName(),
                        key_column_type->getName());
                }
                else
                {
                    key_columns = {key_column};
                    key_types = {key_column_type};
                }
            }
        }

        dictionary->convertKeyColumns(key_columns, key_types);

        if (dictionary_special_key_type == DictionarySpecialKeyType::Range)
        {
            key_columns.emplace_back(range_col);
            key_types.emplace_back(range_col_type);
        }

        return dictionary->hasKeys(key_columns, key_types);
    }

private:
    mutable FunctionDictHelper helper;
};

enum class DictionaryGetFunctionType
{
    get,
    getOrDefault
};

/// This variant of function derives the result type automatically.
template <DictionaryGetFunctionType dictionary_get_function_type>
class FunctionDictGetNoType final : public IFunction
{
public:
    static constexpr auto name = dictionary_get_function_type == DictionaryGetFunctionType::get ? "dictGet" : "dictGetOrDefault";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictGetNoType>(context);
    }

    explicit FunctionDictGetNoType(ContextPtr context_) : helper(context_) {}

    String getName() const override { return name; }

    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool useDefaultImplementationForConstants() const final { return true; }
    bool useDefaultImplementationForNulls() const final { return false; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return {0, 1}; }

    bool isDeterministic() const override { return false; }

    bool isInjective(const ColumnsWithTypeAndName & sample_columns) const override
    {
        return helper.isDictGetFunctionInjective(sample_columns);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if (arguments.size() < 3)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Wrong argument count for function {}",
                getName());

        String dictionary_name;
        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
            dictionary_name = name_col->getValue<String>();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a const string.",
                arguments[0].type->getName(),
                getName());

        Strings attribute_names = getAttributeNamesFromColumn(arguments[1].column, arguments[1].type);

        auto dictionary_structure = helper.getDictionaryStructure(dictionary_name);

        DataTypes attribute_types;
        attribute_types.reserve(attribute_names.size());
        for (auto & attribute_name : attribute_names)
        {
            /// We're extracting the return type from the dictionary's config, without loading the dictionary.
            const auto & attribute = dictionary_structure.getAttribute(attribute_name);
            attribute_types.emplace_back(attribute.type);
        }

        bool key_is_nullable = arguments[2].type->isNullable();
        if (attribute_types.size() > 1)
        {
            if (key_is_nullable)
                throw Exception(ErrorCodes::UNSUPPORTED_METHOD, "Function {} support nullable key only for single dictionary attribute", getName());

            return std::make_shared<DataTypeTuple>(attribute_types, attribute_names);
        }
        else
        {
            if (key_is_nullable)
                return makeNullable(attribute_types.front());
            else
                return attribute_types.front();
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        String dictionary_name;

        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
            dictionary_name = name_col->getValue<String>();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a const string.",
                arguments[0].type->getName(),
                getName());

        Strings attribute_names = getAttributeNamesFromColumn(arguments[1].column, arguments[1].type);

        auto dictionary = helper.getDictionary(dictionary_name);
        auto dictionary_key_type = dictionary->getKeyType();
        auto dictionary_special_key_type = dictionary->getSpecialKeyType();

        size_t current_arguments_index = 3;

        ColumnPtr range_col = nullptr;
        DataTypePtr range_col_type = nullptr;

        if (dictionary_special_key_type == DictionarySpecialKeyType::Range)
        {
            if (current_arguments_index >= arguments.size())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {} should be {}",
                    getName(),
                    arguments.size(),
                    arguments.size() + 1);

            range_col = arguments[current_arguments_index].column;
            range_col_type = arguments[current_arguments_index].type;

            if (!(range_col_type->isValueRepresentedByInteger() && range_col_type->getSizeOfValueInMemory() <= sizeof(Int64)))
                throw Exception(ErrorCodes::ILLEGAL_COLUMN,
                    "Illegal type {} of fourth argument of function must be convertible to Int64.",
                    range_col_type->getName(),
                    getName());

            ++current_arguments_index;
        }

        Columns default_cols;

        if (dictionary_get_function_type == DictionaryGetFunctionType::getOrDefault)
        {
            if (current_arguments_index >= arguments.size())
                throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {} should be {}",
                    getName(),
                    arguments.size(),
                    arguments.size() + 1);

            const auto & column_before_cast = arguments[current_arguments_index];
            ColumnWithTypeAndName column_to_cast = {column_before_cast.column->convertToFullColumnIfConst(), column_before_cast.type, column_before_cast.name};

            auto result = castColumnAccurate(column_to_cast, result_type);

            if (attribute_names.size() > 1)
            {
                const auto * tuple_column = checkAndGetColumn<ColumnTuple>(result.get());

                if (!tuple_column)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Wrong argument for function {} default values column must be tuple",
                        getName());

                if (tuple_column->tupleSize() != attribute_names.size())
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Wrong argument for function {} default values tuple column must contain same column size as requested attributes",
                        getName());

                default_cols = tuple_column->getColumnsCopy();
            }
            else
            {
                default_cols.emplace_back(result);
            }

            ++current_arguments_index;
        }
        else
        {
            for (size_t i = 0; i < attribute_names.size(); ++i)
                default_cols.emplace_back(nullptr);
        }

        if (current_arguments_index < arguments.size())
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: passed {} should be {}",
                    getName(),
                    arguments.size(),
                    current_arguments_index);

        auto key_col_with_type = arguments[2];

        bool key_is_only_null = key_col_with_type.type->onlyNull();
        if (key_is_only_null)
            return result_type->createColumnConstWithDefaultValue(input_rows_count);

        bool key_is_nullable = key_col_with_type.type->isNullable();
        if (key_is_nullable)
            key_col_with_type = columnGetNested(key_col_with_type);

        auto key_column = key_col_with_type.column;

        Columns key_columns;
        DataTypes key_types;

        if (dictionary_key_type == DictionaryKeyType::Simple)
        {
            key_columns = {key_column};
            key_types = {key_col_with_type.type};
        }
        else if (dictionary_key_type == DictionaryKeyType::Complex)
        {
            /// Functions in external dictionaries_loader only support full-value (not constant) columns with keys.
            key_column = key_column->convertToFullColumnIfConst();
            DataTypePtr key_column_type = key_col_with_type.type;

            if (isTuple(key_column_type))
            {
                key_columns = assert_cast<const ColumnTuple &>(*key_column).getColumnsCopy();
                key_types = assert_cast<const DataTypeTuple &>(*key_column_type).getElements();
            }
            else
            {
                size_t keys_size = dictionary->getStructure().getKeysSize();

                if (keys_size > 1)
                {
                    throw Exception(
                         ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                         "Third argument of function {} must be tuple when dictionary is complex and key contains more than 1 attribute."
                         "Actual type {}.",
                         getName(),
                         key_col_with_type.type->getName());
                }
                else
                {
                    key_columns = {std::move(key_column)};
                    key_types = {std::move(key_column_type)};
                }
            }
        }

        dictionary->convertKeyColumns(key_columns, key_types);

        if (dictionary_special_key_type == DictionarySpecialKeyType::Range)
        {
            key_columns.emplace_back(range_col);
            key_types.emplace_back(range_col_type);
        }

        DataTypePtr attribute_type = result_type;
        if (key_is_nullable)
        {
            DataTypes attribute_types;
            attribute_types.reserve(attribute_names.size());
            for (auto & attribute_name : attribute_names)
            {
                const auto & attribute = dictionary->getStructure().getAttribute(attribute_name);
                attribute_types.emplace_back(attribute.type);
            }

            attribute_type = attribute_types.front();
        }

        auto result_column = executeDictionaryRequest(dictionary, attribute_names, key_columns, key_types, attribute_type, default_cols);

        if (key_is_nullable)
            result_column = wrapInNullable(result_column, {arguments[2]}, result_type, input_rows_count);

        return result_column;
    }

private:

    ColumnPtr executeDictionaryRequest(
        std::shared_ptr<const IDictionary> & dictionary,
        const Strings & attribute_names,
        const Columns & key_columns,
        const DataTypes & key_types,
        const DataTypePtr & result_type,
        const Columns & default_cols) const
    {
        ColumnPtr result;

        if (attribute_names.size() > 1)
        {
            const auto & result_tuple_type = assert_cast<const DataTypeTuple &>(*result_type);

            Columns result_columns = dictionary->getColumns(
                attribute_names,
                result_tuple_type.getElements(),
                key_columns,
                key_types,
                default_cols);

            result = ColumnTuple::create(std::move(result_columns));
        }
        else
        {
            result = dictionary->getColumn(
                attribute_names[0],
                result_type,
                key_columns,
                key_types,
                default_cols.front());
        }

        return result;
    }

    Strings getAttributeNamesFromColumn(const ColumnPtr & column, const DataTypePtr & type) const
    {
        Strings attribute_names;

        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(column.get()))
        {
            attribute_names.emplace_back(name_col->getValue<String>());
        }
        else if (const auto * tuple_col_const = checkAndGetColumnConst<ColumnTuple>(column.get()))
        {
            const ColumnTuple & tuple_col = assert_cast<const ColumnTuple &>(tuple_col_const->getDataColumn());
            size_t tuple_size = tuple_col.tupleSize();

            if (tuple_size < 1)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Tuple second argument of function {} must contain multiple constant string columns");

            for (size_t i = 0; i < tuple_col.tupleSize(); ++i)
            {
                const auto * tuple_column = tuple_col.getColumnPtr(i).get();

                const auto * attribute_name_column = checkAndGetColumn<ColumnString>(tuple_column);

                if (!attribute_name_column)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Tuple second argument of function {} must contain multiple constant string columns",
                        getName());

                attribute_names.emplace_back(attribute_name_column->getDataAt(0));
            }
        }
        else
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of second argument of function {}, expected a const string or const tuple of const strings.",
                type->getName(),
                getName());
        }

        return attribute_names;
    }

    mutable FunctionDictHelper helper;
};

template <typename DataType, typename Name, DictionaryGetFunctionType dictionary_get_function_type>
class FunctionDictGetImpl final : public IFunction
{
    using Type = typename DataType::FieldType;

public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictGetImpl>(context);
    }

    explicit FunctionDictGetImpl(ContextPtr context_) : impl(context_) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool useDefaultImplementationForConstants() const final { return true; }

    bool isDeterministic() const override { return false; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return {0, 1}; }

    bool isInjective(const ColumnsWithTypeAndName & sample_columns) const override
    {
        return impl.isInjective(sample_columns);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override
    {
        DataTypePtr result;

        if constexpr (IsDataTypeDecimal<DataType>)
            result = std::make_shared<DataType>(DataType::maxPrecision(), 0);
        else
            result = std::make_shared<DataType>();

        return result;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        auto return_type = impl.getReturnTypeImpl(arguments);

        if (!return_type->equals(*result_type))
            throw Exception{"Dictionary attribute has different type " + return_type->getName() + " expected " + result_type->getName(),
                    ErrorCodes::TYPE_MISMATCH};

        return impl.executeImpl(arguments, return_type, input_rows_count);
    }

    const FunctionDictGetNoType<dictionary_get_function_type> impl;
};

template<typename DataType, typename Name>
using FunctionDictGet = FunctionDictGetImpl<DataType, Name, DictionaryGetFunctionType::get>;

struct NameDictGetUInt8 { static constexpr auto name = "dictGetUInt8"; };
struct NameDictGetUInt16 { static constexpr auto name = "dictGetUInt16"; };
struct NameDictGetUInt32 { static constexpr auto name = "dictGetUInt32"; };
struct NameDictGetUInt64 { static constexpr auto name = "dictGetUInt64"; };
struct NameDictGetInt8 { static constexpr auto name = "dictGetInt8"; };
struct NameDictGetInt16 { static constexpr auto name = "dictGetInt16"; };
struct NameDictGetInt32 { static constexpr auto name = "dictGetInt32"; };
struct NameDictGetInt64 { static constexpr auto name = "dictGetInt64"; };
struct NameDictGetFloat32 { static constexpr auto name = "dictGetFloat32"; };
struct NameDictGetFloat64 { static constexpr auto name = "dictGetFloat64"; };
struct NameDictGetDate { static constexpr auto name = "dictGetDate"; };
struct NameDictGetDateTime { static constexpr auto name = "dictGetDateTime"; };
struct NameDictGetUUID { static constexpr auto name = "dictGetUUID"; };
struct NameDictGetDecimal32 { static constexpr auto name = "dictGetDecimal32"; };
struct NameDictGetDecimal64 { static constexpr auto name = "dictGetDecimal64"; };
struct NameDictGetDecimal128 { static constexpr auto name = "dictGetDecimal128"; };
struct NameDictGetString { static constexpr auto name = "dictGetString"; };

using FunctionDictGetUInt8 = FunctionDictGet<DataTypeUInt8, NameDictGetUInt8>;
using FunctionDictGetUInt16 = FunctionDictGet<DataTypeUInt16, NameDictGetUInt16>;
using FunctionDictGetUInt32 = FunctionDictGet<DataTypeUInt32, NameDictGetUInt32>;
using FunctionDictGetUInt64 = FunctionDictGet<DataTypeUInt64, NameDictGetUInt64>;
using FunctionDictGetInt8 = FunctionDictGet<DataTypeInt8, NameDictGetInt8>;
using FunctionDictGetInt16 = FunctionDictGet<DataTypeInt16, NameDictGetInt16>;
using FunctionDictGetInt32 = FunctionDictGet<DataTypeInt32, NameDictGetInt32>;
using FunctionDictGetInt64 = FunctionDictGet<DataTypeInt64, NameDictGetInt64>;
using FunctionDictGetFloat32 = FunctionDictGet<DataTypeFloat32, NameDictGetFloat32>;
using FunctionDictGetFloat64 = FunctionDictGet<DataTypeFloat64, NameDictGetFloat64>;
using FunctionDictGetDate = FunctionDictGet<DataTypeDate, NameDictGetDate>;
using FunctionDictGetDateTime = FunctionDictGet<DataTypeDateTime, NameDictGetDateTime>;
using FunctionDictGetUUID = FunctionDictGet<DataTypeUUID, NameDictGetUUID>;
using FunctionDictGetDecimal32 = FunctionDictGet<DataTypeDecimal<Decimal32>, NameDictGetDecimal32>;
using FunctionDictGetDecimal64 = FunctionDictGet<DataTypeDecimal<Decimal64>, NameDictGetDecimal64>;
using FunctionDictGetDecimal128 = FunctionDictGet<DataTypeDecimal<Decimal128>, NameDictGetDecimal128>;
using FunctionDictGetString = FunctionDictGet<DataTypeString, NameDictGetString>;

template<typename DataType, typename Name>
using FunctionDictGetOrDefault = FunctionDictGetImpl<DataType, Name, DictionaryGetFunctionType::getOrDefault>;

struct NameDictGetUInt8OrDefault { static constexpr auto name = "dictGetUInt8OrDefault"; };
struct NameDictGetUInt16OrDefault { static constexpr auto name = "dictGetUInt16OrDefault"; };
struct NameDictGetUInt32OrDefault { static constexpr auto name = "dictGetUInt32OrDefault"; };
struct NameDictGetUInt64OrDefault { static constexpr auto name = "dictGetUInt64OrDefault"; };
struct NameDictGetInt8OrDefault { static constexpr auto name = "dictGetInt8OrDefault"; };
struct NameDictGetInt16OrDefault { static constexpr auto name = "dictGetInt16OrDefault"; };
struct NameDictGetInt32OrDefault { static constexpr auto name = "dictGetInt32OrDefault"; };
struct NameDictGetInt64OrDefault { static constexpr auto name = "dictGetInt64OrDefault"; };
struct NameDictGetFloat32OrDefault { static constexpr auto name = "dictGetFloat32OrDefault"; };
struct NameDictGetFloat64OrDefault { static constexpr auto name = "dictGetFloat64OrDefault"; };
struct NameDictGetDateOrDefault { static constexpr auto name = "dictGetDateOrDefault"; };
struct NameDictGetDateTimeOrDefault { static constexpr auto name = "dictGetDateTimeOrDefault"; };
struct NameDictGetUUIDOrDefault { static constexpr auto name = "dictGetUUIDOrDefault"; };
struct NameDictGetDecimal32OrDefault { static constexpr auto name = "dictGetDecimal32OrDefault"; };
struct NameDictGetDecimal64OrDefault { static constexpr auto name = "dictGetDecimal64OrDefault"; };
struct NameDictGetDecimal128OrDefault { static constexpr auto name = "dictGetDecimal128OrDefault"; };
struct NameDictGetStringOrDefault { static constexpr auto name = "dictGetStringOrDefault"; };

using FunctionDictGetUInt8OrDefault = FunctionDictGetOrDefault<DataTypeUInt8, NameDictGetUInt8OrDefault>;
using FunctionDictGetUInt16OrDefault = FunctionDictGetOrDefault<DataTypeUInt16, NameDictGetUInt16OrDefault>;
using FunctionDictGetUInt32OrDefault = FunctionDictGetOrDefault<DataTypeUInt32, NameDictGetUInt32OrDefault>;
using FunctionDictGetUInt64OrDefault = FunctionDictGetOrDefault<DataTypeUInt64, NameDictGetUInt64OrDefault>;
using FunctionDictGetInt8OrDefault = FunctionDictGetOrDefault<DataTypeInt8, NameDictGetInt8OrDefault>;
using FunctionDictGetInt16OrDefault = FunctionDictGetOrDefault<DataTypeInt16, NameDictGetInt16OrDefault>;
using FunctionDictGetInt32OrDefault = FunctionDictGetOrDefault<DataTypeInt32, NameDictGetInt32OrDefault>;
using FunctionDictGetInt64OrDefault = FunctionDictGetOrDefault<DataTypeInt64, NameDictGetInt64OrDefault>;
using FunctionDictGetFloat32OrDefault = FunctionDictGetOrDefault<DataTypeFloat32, NameDictGetFloat32OrDefault>;
using FunctionDictGetFloat64OrDefault = FunctionDictGetOrDefault<DataTypeFloat64, NameDictGetFloat64OrDefault>;
using FunctionDictGetDateOrDefault = FunctionDictGetOrDefault<DataTypeDate, NameDictGetDateOrDefault>;
using FunctionDictGetDateTimeOrDefault = FunctionDictGetOrDefault<DataTypeDateTime, NameDictGetDateTimeOrDefault>;
using FunctionDictGetUUIDOrDefault = FunctionDictGetOrDefault<DataTypeUUID, NameDictGetUUIDOrDefault>;
using FunctionDictGetDecimal32OrDefault = FunctionDictGetOrDefault<DataTypeDecimal<Decimal32>, NameDictGetDecimal32OrDefault>;
using FunctionDictGetDecimal64OrDefault = FunctionDictGetOrDefault<DataTypeDecimal<Decimal64>, NameDictGetDecimal64OrDefault>;
using FunctionDictGetDecimal128OrDefault = FunctionDictGetOrDefault<DataTypeDecimal<Decimal128>, NameDictGetDecimal128OrDefault>;
using FunctionDictGetStringOrDefault = FunctionDictGetOrDefault<DataTypeString, NameDictGetStringOrDefault>;

class FunctionDictGetOrNull final : public IFunction
{
public:
    static constexpr auto name = "dictGetOrNull";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictGetOrNull>(context);
    }

    explicit FunctionDictGetOrNull(ContextPtr context_)
        : dictionary_get_func_impl(context_)
        , dictionary_has_func_impl(context_)
    {}

    String getName() const override { return name; }

private:

    size_t getNumberOfArguments() const override { return 0; }

    bool isVariadic() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const override { return true; }

    bool useDefaultImplementationForNulls() const override { return false; }

    bool isDeterministic() const override { return false; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 1}; }

    bool isInjective(const ColumnsWithTypeAndName & sample_columns) const override
    {
        return dictionary_get_func_impl.isInjective(sample_columns);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto result_type = dictionary_get_func_impl.getReturnTypeImpl(arguments);

        WhichDataType result_data_type(result_type);
        if (result_data_type.isTuple())
        {
            const auto & data_type_tuple = static_cast<const DataTypeTuple &>(*result_type);
            auto elements_types_copy = data_type_tuple.getElements();
            for (auto & element_type : elements_types_copy)
                element_type = makeNullable(element_type);

            result_type = std::make_shared<DataTypeTuple>(elements_types_copy, data_type_tuple.getElementNames());
        }
        else
            result_type = makeNullable(result_type);

        return result_type;
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        /** We call dictHas function to get which map is key presented in dictionary.
            For key that presented in dictionary dict has result for that key index value will be 1. Otherwise 0.
            We invert result, and then for key that is not presented in dictionary value will be 1. Otherwise 0.
            This inverted result will be used as null column map.
            After that we call dict get function, by contract for key that are not presented in dictionary we
            return default value.
            We create nullable column from dict get result column and null column map.

            2 additional implementation details:
            1. Result from dict get can be tuple if client requested multiple attributes we apply such operation on each result column.
            2. If column is already nullable we merge column null map with null map that we get from dict has.
          */

        auto dict_has_arguments = filterAttributeNameArgumentForDictHas(arguments);
        auto is_key_in_dictionary_column = dictionary_has_func_impl.executeImpl(dict_has_arguments, std::make_shared<DataTypeUInt8>(), input_rows_count);
        auto is_key_in_dictionary_column_mutable = is_key_in_dictionary_column->assumeMutable();
        ColumnVector<UInt8> & is_key_in_dictionary_column_typed = assert_cast<ColumnVector<UInt8> &>(*is_key_in_dictionary_column_mutable);
        PaddedPODArray<UInt8> & is_key_in_dictionary_data = is_key_in_dictionary_column_typed.getData();
        for (auto & key : is_key_in_dictionary_data)
            key = !key;

        auto dictionary_get_result_type = dictionary_get_func_impl.getReturnTypeImpl(arguments);
        auto dictionary_get_result_column = dictionary_get_func_impl.executeImpl(arguments, dictionary_get_result_type, input_rows_count);

        ColumnPtr result;

        WhichDataType dictionary_get_result_data_type(dictionary_get_result_type);
        auto dictionary_get_result_column_mutable = dictionary_get_result_column->assumeMutable();

        if (dictionary_get_result_data_type.isTuple())
        {
            ColumnTuple & column_tuple = assert_cast<ColumnTuple &>(*dictionary_get_result_column_mutable);

            const auto & columns = column_tuple.getColumns();
            size_t tuple_size = columns.size();

            MutableColumns new_columns(tuple_size);
            for (size_t tuple_column_index = 0; tuple_column_index < tuple_size; ++tuple_column_index)
            {
                auto nullable_column_map = ColumnVector<UInt8>::create();
                auto & nullable_column_map_data = nullable_column_map->getData();
                nullable_column_map_data.assign(is_key_in_dictionary_data);

                auto mutable_column = columns[tuple_column_index]->assumeMutable();
                if (ColumnNullable * nullable_column = typeid_cast<ColumnNullable *>(mutable_column.get()))
                {
                    auto & null_map_data = nullable_column->getNullMapData();
                    addNullMap(null_map_data, is_key_in_dictionary_data);
                    new_columns[tuple_column_index] = std::move(mutable_column);
                }
                else
                    new_columns[tuple_column_index] = ColumnNullable::create(std::move(mutable_column), std::move(nullable_column_map));
            }

            result = ColumnTuple::create(std::move(new_columns));
        }
        else
        {
            if (ColumnNullable * nullable_column = typeid_cast<ColumnNullable *>(dictionary_get_result_column_mutable.get()))
            {
                auto & null_map_data = nullable_column->getNullMapData();
                addNullMap(null_map_data, is_key_in_dictionary_data);
                result = std::move(dictionary_get_result_column);
            }
            else
            {
                result = ColumnNullable::create(dictionary_get_result_column, std::move(is_key_in_dictionary_column_mutable));
            }
        }

        return result;
    }

    static void addNullMap(PaddedPODArray<UInt8> & null_map, PaddedPODArray<UInt8> & null_map_to_add)
    {
        assert(null_map.size() == null_map_to_add.size());

        for (size_t i = 0; i < null_map.size(); ++i)
            null_map[i] = null_map[i] || null_map_to_add[i];
    }

    static ColumnsWithTypeAndName filterAttributeNameArgumentForDictHas(const ColumnsWithTypeAndName & arguments)
    {
        ColumnsWithTypeAndName dict_has_arguments;
        dict_has_arguments.reserve(arguments.size() - 1);
        size_t attribute_name_argument_index = 1;

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (i == attribute_name_argument_index)
                continue;

            dict_has_arguments.emplace_back(arguments[i]);
        }

        return dict_has_arguments;
    }

    const FunctionDictGetNoType<DictionaryGetFunctionType::get> dictionary_get_func_impl;
    const FunctionDictHas dictionary_has_func_impl;
};

/// Functions to work with hierarchies.

class FunctionDictGetHierarchy final : public IFunction
{
public:
    static constexpr auto name = "dictGetHierarchy";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictGetHierarchy>(context);
    }

    explicit FunctionDictGetHierarchy(ContextPtr context_) : helper(context_) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const ColumnsWithTypeAndName & /*sample_columns*/) const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    bool useDefaultImplementationForConstants() const final { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return {0}; }
    bool isDeterministic() const override { return false; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        String dictionary_name;
        if (const auto * name_col = checkAndGetColumnConst<ColumnString>(arguments[0].column.get()))
            dictionary_name = name_col->getValue<String>();
        else
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type {} of first argument of function {}, expected a const string.",
                arguments[0].type->getName(),
                getName());

        auto dictionary = helper.getDictionary(arguments[0].column);
        const auto & hierarchical_attribute = helper.getDictionaryHierarchicalAttribute(dictionary);

        return std::make_shared<DataTypeArray>(removeNullable(hierarchical_attribute.type));
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        auto dictionary = helper.getDictionary(arguments[0].column);
        const auto & hierarchical_attribute = helper.getDictionaryHierarchicalAttribute(dictionary);

        auto key_column = ColumnWithTypeAndName{arguments[1].column, arguments[1].type, arguments[1].name};
        auto key_column_casted = castColumnAccurate(key_column, removeNullable(hierarchical_attribute.type));

        ColumnPtr result = dictionary->getHierarchy(key_column_casted, hierarchical_attribute.type);

        return result;
    }

    mutable FunctionDictHelper helper;
};


class FunctionDictIsIn final : public IFunction
{
public:
    static constexpr auto name = "dictIsIn";

    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictIsIn>(context);
    }

    explicit FunctionDictIsIn(ContextPtr context_)
        : helper(context_) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 3; }

    bool useDefaultImplementationForConstants() const final { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const final { return {0}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of first argument of function {}. Expected String. Actual type {}",
                getName(),
                arguments[0]->getName());

        return std::make_shared<DataTypeUInt8>();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        auto dictionary = helper.getDictionary(arguments[0].column);
        const auto & hierarchical_attribute = helper.getDictionaryHierarchicalAttribute(dictionary);

        auto key_column = ColumnWithTypeAndName{arguments[1].column->convertToFullColumnIfConst(), arguments[1].type, arguments[2].name};
        auto in_key_column = ColumnWithTypeAndName{arguments[2].column->convertToFullColumnIfConst(), arguments[2].type, arguments[2].name};

        auto hierarchical_attribute_non_nullable = removeNullable(hierarchical_attribute.type);
        auto key_column_casted = castColumnAccurate(key_column, hierarchical_attribute_non_nullable);
        auto in_key_column_casted = castColumnAccurate(in_key_column, hierarchical_attribute_non_nullable);

        ColumnPtr result = dictionary->isInHierarchy(key_column_casted, in_key_column_casted, hierarchical_attribute.type);

        return result;
    }

    mutable FunctionDictHelper helper;
};

class FunctionDictGetDescendantsExecutable final : public IExecutableFunction
{
public:
    FunctionDictGetDescendantsExecutable(
        String name_,
        size_t level_,
        DictionaryHierarchicalParentToChildIndexPtr hierarchical_parent_to_child_index,
        std::shared_ptr<FunctionDictHelper> dictionary_helper_)
        : name(std::move(name_))
        , level(level_)
        , hierarchical_parent_to_child_index(std::move(hierarchical_parent_to_child_index))
        , dictionary_helper(std::move(dictionary_helper_))
    {}

    String getName() const override { return name; }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (input_rows_count == 0)
            return result_type->createColumn();

        auto dictionary = dictionary_helper->getDictionary(arguments[0].column);
        const auto & hierarchical_attribute = dictionary_helper->getDictionaryHierarchicalAttribute(dictionary);

        auto key_column = ColumnWithTypeAndName{arguments[1].column->convertToFullColumnIfConst(), arguments[1].type, arguments[1].name};
        auto key_column_casted = castColumnAccurate(key_column, removeNullable(hierarchical_attribute.type));

        return dictionary->getDescendants(key_column_casted, removeNullable(hierarchical_attribute.type), level, hierarchical_parent_to_child_index);
    }

    String name;
    size_t level;
    DictionaryHierarchicalParentToChildIndexPtr hierarchical_parent_to_child_index;
    std::shared_ptr<FunctionDictHelper> dictionary_helper;
};

class FunctionDictGetDescendantsBase final : public IFunctionBase
{
public:
    FunctionDictGetDescendantsBase(
        String name_,
        const DataTypes & argument_types_,
        const DataTypePtr & result_type_,
        size_t level_,
        DictionaryHierarchicalParentToChildIndexPtr hierarchical_parent_to_child_index,
        std::shared_ptr<FunctionDictHelper> helper_)
        : name(std::move(name_))
        , argument_types(argument_types_)
        , result_type(result_type_)
        , level(level_)
        , hierarchical_parent_to_child_index(std::move(hierarchical_parent_to_child_index))
        , helper(std::move(helper_))
    {}

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }

    const DataTypePtr & getResultType() const override { return result_type; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    ExecutableFunctionPtr prepare(const ColumnsWithTypeAndName &) const override
    {
        return std::make_shared<FunctionDictGetDescendantsExecutable>(name, level, hierarchical_parent_to_child_index, helper);
    }

    String name;
    DataTypes argument_types;
    DataTypePtr result_type;
    size_t level;
    DictionaryHierarchicalParentToChildIndexPtr hierarchical_parent_to_child_index;
    std::shared_ptr<FunctionDictHelper> helper;
};

struct FunctionDictGetDescendantsStrategy
{
    static constexpr auto name = "dictGetDescendants";
    static constexpr size_t default_level = 0;
    static constexpr size_t number_of_arguments = 0;
    static constexpr bool is_variadic = true;
};

struct FunctionDictGetChildrenStrategy
{
    static constexpr auto name = "dictGetChildren";
    static constexpr size_t default_level = 1;
    static constexpr size_t number_of_arguments = 2;
    static constexpr bool is_variadic = false;
};

template <typename Strategy>
class FunctionDictGetDescendantsOverloadResolverImpl final : public IFunctionOverloadResolver
{
public:
    static constexpr auto name = Strategy::name;

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return Strategy::number_of_arguments; }

    bool isVariadic() const override { return Strategy::is_variadic; }

    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0, 2}; }

    bool isDeterministic() const override { return false; }

    explicit FunctionDictGetDescendantsOverloadResolverImpl(ContextPtr context)
        : dictionary_helper(std::make_shared<FunctionDictHelper>(std::move(context)))
    {}

    static FunctionOverloadResolverPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionDictGetDescendantsOverloadResolverImpl>(std::move(context));
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        auto dictionary = dictionary_helper->getDictionary(arguments[0].column);
        auto hierarchical_parent_to_child_index = dictionary->getHierarchicalIndex();

        size_t level = Strategy::default_level;

        if (arguments.size() == 3)
        {
            if (!arguments[2].column || !isColumnConst(*arguments[2].column))
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type of third argument of function {}. Expected const unsigned integer.",
                    getName());

            auto value = static_cast<Int64>(arguments[2].column->getInt(0));
            if (value < 0)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Illegal type of third argument of function {}. Expected const unsigned integer.",
                    getName());

            level = static_cast<size_t>(value);
        }

        DataTypes argument_types;
        argument_types.reserve(arguments.size());

        for (const auto & argument : arguments)
            argument_types.emplace_back(argument.type);

        return std::make_shared<FunctionDictGetDescendantsBase>(name, argument_types, result_type, level, hierarchical_parent_to_child_index, dictionary_helper);
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        size_t arguments_size = arguments.size();
        if (arguments_size < 2 || arguments_size > 3)
        {
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Illegal arguments size of function {}. Expects 2 or 3 arguments size. Actual size {}",
                getName(),
                arguments_size);
        }

        if (!isString(arguments[0].type))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of first argument of function {}. Expected const String. Actual type {}",
                getName(),
                arguments[0].type->getName());

        if (arguments.size() == 3 && !isInteger(arguments[2].type))
        {
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of third argument of function {}. Expected const unsigned integer. Actual type {}",
                getName(),
                arguments[2].type->getName());
        }

        auto dictionary = dictionary_helper->getDictionary(arguments[0].column);
        const auto & hierarchical_attribute = dictionary_helper->getDictionaryHierarchicalAttribute(dictionary);

        return std::make_shared<DataTypeArray>(removeNullable(hierarchical_attribute.type));
    }

    std::shared_ptr<FunctionDictHelper> dictionary_helper;
};

using FunctionDictGetDescendantsOverloadResolver = FunctionDictGetDescendantsOverloadResolverImpl<FunctionDictGetDescendantsStrategy>;
using FunctionDictGetChildrenOverloadResolver = FunctionDictGetDescendantsOverloadResolverImpl<FunctionDictGetChildrenStrategy>;

}
