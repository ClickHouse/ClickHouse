#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeTuple.h>

#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalDictionaries.h>

#include <Functions/IFunction.h>
#include <Dictionaries/FlatDictionary.h>
#include <Dictionaries/HashedDictionary.h>
#include <Dictionaries/CacheDictionary.h>
#include <Dictionaries/ComplexKeyHashedDictionary.h>
#include <Dictionaries/ComplexKeyCacheDictionary.h>
#include <Dictionaries/RangeHashedDictionary.h>
#include <Dictionaries/TrieDictionary.h>

#include <ext/range.hpp>


namespace DB
{

namespace ErrorCodes
{
    extern const int DICTIONARIES_WAS_NOT_LOADED;
    extern const int UNSUPPORTED_METHOD;
    extern const int UNKNOWN_TYPE;
}

/** Functions that use plug-ins (external) dictionaries.
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


class FunctionDictHas final : public IFunction
{
public:
    static constexpr auto name = "dictHas";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictHas>(context.getExternalDictionaries());
    }

    FunctionDictHas(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[1].get()) &&
            !typeid_cast<const DataTypeTuple *>(arguments[1].get()))
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ", must be UInt64 or tuple(...).",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatchSimple<FlatDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchSimple<HashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchSimple<CacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyHashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<TrieDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatchSimple(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        const auto id_col_untyped = block.safeGetByPosition(arguments[1]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
        {
            const auto & ids = id_col->getData();

            const auto out = std::make_shared<ColumnUInt8>(ext::size(ids));
            block.safeGetByPosition(result).column = out;

            dict->has(ids, out->getData());
        }
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
        {
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            PaddedPODArray<UInt8> out(1);

            dict->has(ids, out);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<UInt8>>(id_col->size(), out.front());
        }
        else
            throw Exception{
                "Second argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};

        return true;
    }

    template <typename DictionaryType>
    bool executeDispatchComplex(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        const auto key_col_with_type = block.safeGetByPosition(arguments[1]);
        if (typeid_cast<const ColumnTuple *>(key_col_with_type.column.get())
            || typeid_cast<const ColumnConstTuple *>(key_col_with_type.column.get()))
        {
            /// Functions in external dictionaries only support full-value (not constant) columns with keys.
            const ColumnPtr key_col_materialized = key_col_with_type.column->convertToFullColumnIfConst();

            const auto key_columns = ext::map<ConstColumnPlainPtrs>(
                static_cast<const ColumnTuple &>(*key_col_materialized.get()).getColumns(),
                [](const ColumnPtr & ptr) { return ptr.get(); });

            const auto & key_types = static_cast<const DataTypeTuple &>(*key_col_with_type.type).getElements();

            const auto out = std::make_shared<ColumnUInt8>(key_col_with_type.column->size());
            block.safeGetByPosition(result).column = out;

            dict->has(key_columns, key_types, out->getData());
        }
        else
            throw Exception{
                "Second argument of function " + getName() + " must be " + dict->getKeyDescription(),
                ErrorCodes::TYPE_MISMATCH};

        return true;
    }

    const ExternalDictionaries & dictionaries;
};


static bool isDictGetFunctionInjective(const ExternalDictionaries & dictionaries, const Block & sample_block)
{
    if (sample_block.columns() != 3 && sample_block.columns() != 4)
        throw Exception{"Function dictGet... takes 3 or 4 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

    const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(sample_block.getByPosition(0).column.get());
    if (!dict_name_col)
        throw Exception{
            "First argument of function dictGet... must be a constant string",
            ErrorCodes::ILLEGAL_COLUMN};

    const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(sample_block.getByPosition(1).column.get());
    if (!attr_name_col)
        throw Exception{
            "Second argument of function dictGet... must be a constant string",
            ErrorCodes::ILLEGAL_COLUMN};

    return dictionaries.getDictionary(dict_name_col->getData())->isInjective(attr_name_col->getData());
}


class FunctionDictGetString final : public IFunction
{
public:
    static constexpr auto name = "dictGetString";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictGetString>(context.getExternalDictionaries());
    }

    FunctionDictGetString(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block & sample_block) override
    {
        return isDictGetFunctionInjective(dictionaries, sample_block);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception{
                "Number of arguments for function " + getName() + " doesn't match: passed "
                    + toString(arguments.size()) + ", should be 3 or 4.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
        {
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[2].get()) &&
            !typeid_cast<const DataTypeTuple *>(arguments[2].get()))
        {
            throw Exception{
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName()
                    + ", must be UInt64 or tuple(...).",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (arguments.size() == 4 && !typeid_cast<const DataTypeDate *>(arguments[3].get()))
        {
            throw Exception{
                "Illegal type " + arguments[3]->getName() + " of fourth argument of function " + getName()
                    + ", must be Date.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatch<FlatDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<HashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<CacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyHashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<TrieDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchRange<RangeHashedDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatch(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (arguments.size() != 3)
            throw Exception{
                "Function " + getName() + " for dictionary of type " + dict->getTypeName() +
                " requires exactly 3 arguments",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
        {
            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;
            dict->getString(attr_name, id_col->getData(), out.get());
        }
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
        {
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            auto out = std::make_unique<ColumnString>();
            dict->getString(attr_name, ids, out.get());

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<String>>(
                id_col->size(), out->getDataAt(0).toString());
        }
        else
        {
            throw Exception{
                "Third argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    template <typename DictionaryType>
    bool executeDispatchComplex(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (arguments.size() != 3)
            throw Exception{
                "Function " + getName() + " for dictionary of type " + dict->getTypeName() +
                    " requires exactly 3 arguments",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto key_col_with_type = block.safeGetByPosition(arguments[2]);
        if (typeid_cast<const ColumnTuple *>(key_col_with_type.column.get())
            || typeid_cast<const ColumnConstTuple *>(key_col_with_type.column.get()))
        {
            const ColumnPtr key_col_materialized = key_col_with_type.column->convertToFullColumnIfConst();

            const auto key_columns = ext::map<ConstColumnPlainPtrs>(
                static_cast<const ColumnTuple &>(*key_col_materialized.get()).getColumns(),
                [](const ColumnPtr & ptr) { return ptr.get(); });

            const auto & key_types = static_cast<const DataTypeTuple &>(*key_col_with_type.type).getElements();

            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;

            dict->getString(attr_name, key_columns, key_types, out.get());
        }
        else
            throw Exception{
                "Third argument of function " + getName() + " must be " + dict->getKeyDescription(),
                ErrorCodes::TYPE_MISMATCH};

        return true;
    }

    template <typename DictionaryType>
    bool executeDispatchRange(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (arguments.size() != 4)
            throw Exception{
                "Function " + getName() + " for dictionary of type " + dict->getTypeName() +
                " requires exactly 4 arguments",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();
        const auto date_col_untyped = block.safeGetByPosition(arguments[3]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
            executeRange(block, result, dict, attr_name, id_col, date_col_untyped);
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
            executeRange(block, result, dict, attr_name, id_col, date_col_untyped);
        else
        {
            throw Exception{
                "Third argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    template <typename DictionaryType>
    void executeRange(
        Block & block, const size_t result, const DictionaryType * dictionary, const std::string & attr_name,
        const ColumnUInt64 * id_col, const IColumn * date_col_untyped)
    {
        if (const auto date_col = typeid_cast<const ColumnUInt16 *>(date_col_untyped))
        {
            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;
            dictionary->getString(attr_name, id_col->getData(), date_col->getData(), out.get());
        }
        else if (const auto date_col = typeid_cast<const ColumnConst<UInt16> *>(date_col_untyped))
        {
            auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;

            const PaddedPODArray<UInt16> dates(id_col->size(), date_col->getData());
            dictionary->getString(attr_name, id_col->getData(), dates, out.get());
        }
        else
        {
            throw Exception{
                "Fourth argument of function " + getName() + " must be Date",
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }

    template <typename DictionaryType>
    void executeRange(
        Block & block, const size_t result, const DictionaryType * dictionary, const std::string & attr_name,
        const ColumnConst<UInt64> * id_col, const IColumn * date_col_untyped)
    {
        if (const auto date_col = typeid_cast<const ColumnUInt16 *>(date_col_untyped))
        {
            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;

            const PaddedPODArray<UInt64> ids(date_col->size(), id_col->getData());
            dictionary->getString(attr_name, ids, date_col->getData(), out.get());
        }
        else if (const auto date_col = typeid_cast<const ColumnConst<UInt16> *>(date_col_untyped))
        {
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            const PaddedPODArray<UInt16> dates(1, date_col->getData());

            auto out = std::make_unique<ColumnString>();
            dictionary->getString(attr_name, ids, dates, out.get());

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<String>>(
                id_col->size(), out->getDataAt(0).toString());
        }
        else
        {
            throw Exception{
                "Fourth argument of function " + getName() + " must be Date",
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }

    const ExternalDictionaries & dictionaries;
};


class FunctionDictGetStringOrDefault final : public IFunction
{
public:
    static constexpr auto name = "dictGetStringOrDefault";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictGetStringOrDefault>(context.getExternalDictionaries());
    }

    FunctionDictGetStringOrDefault(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName() +
                    ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName() +
                    ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[2].get()) &&
            !typeid_cast<const DataTypeTuple *>(arguments[2].get()))
        {
            throw Exception{
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName()
                    + ", must be UInt64 or tuple(...).",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeString *>(arguments[3].get()))
            throw Exception{
                "Illegal type " + arguments[3]->getName() + " of fourth argument of function " + getName() +
                    ", must be String.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};

        return std::make_shared<DataTypeString>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatch<FlatDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<HashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<CacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyHashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<TrieDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatch(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                    ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
            executeDispatch(block, arguments, result, dict, attr_name, id_col);
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
            executeDispatch(block, arguments, result, dict, attr_name, id_col);
        else
            throw Exception{
                "Third argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};

        return true;
    }

    template <typename DictionaryType>
    void executeDispatch(
        Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * dictionary,
        const std::string & attr_name, const ColumnUInt64 * id_col)
    {
        const auto default_col_untyped = block.safeGetByPosition(arguments[3]).column.get();

        if (const auto default_col = typeid_cast<const ColumnString *>(default_col_untyped))
        {
            /// vector ids, vector defaults
            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;

            const auto & ids = id_col->getData();

            dictionary->getString(attr_name, ids, default_col, out.get());
        }
        else if (const auto default_col = typeid_cast<const ColumnConst<String> *>(default_col_untyped))
        {
            /// vector ids, const defaults
            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;

            const auto & ids = id_col->getData();
            const auto & def = default_col->getData();

            dictionary->getString(attr_name, ids, def, out.get());
        }
        else
            throw Exception{
                "Fourth argument of function " + getName() + " must be String",
                ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename DictionaryType>
    void executeDispatch(
        Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * dictionary,
        const std::string & attr_name, const ColumnConst<UInt64> * id_col)
    {
        const auto default_col_untyped = block.safeGetByPosition(arguments[3]).column.get();

        if (const auto default_col = typeid_cast<const ColumnString *>(default_col_untyped))
        {
            /// const ids, vector defaults
            /// @todo avoid materialization
            const PaddedPODArray<UInt64> ids(id_col->size(), id_col->getData());
            const auto out = std::make_shared<ColumnString>();
            block.safeGetByPosition(result).column = out;

            dictionary->getString(attr_name, ids, default_col, out.get());
        }
        else if (const auto default_col = typeid_cast<const ColumnConst<String> *>(default_col_untyped))
        {
            /// const ids, const defaults
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            auto out = std::make_unique<ColumnString>();

            const auto & def = default_col->getData();

            dictionary->getString(attr_name, ids, def, out.get());

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<String>>(
                id_col->size(), out->getDataAt(0).toString());
        }
        else
            throw Exception{
                "Fourth argument of function " + getName() + " must be String",
                ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename DictionaryType>
    bool executeDispatchComplex(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                    ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto key_col_with_type = block.safeGetByPosition(arguments[2]);
        const auto & key_col = typeid_cast<const ColumnTuple &>(*key_col_with_type.column);

        const ColumnPtr key_col_materialized = key_col.convertToFullColumnIfConst();

        const auto key_columns = ext::map<ConstColumnPlainPtrs>(
            static_cast<const ColumnTuple &>(*key_col_materialized.get()).getColumns(), [](const ColumnPtr & ptr) { return ptr.get(); });

        const auto & key_types = static_cast<const DataTypeTuple &>(*key_col_with_type.type).getElements();

        const auto out = std::make_shared<ColumnString>();
        block.safeGetByPosition(result).column = out;

        const auto default_col_untyped = block.safeGetByPosition(arguments[3]).column.get();
        if (const auto default_col = typeid_cast<const ColumnString *>(default_col_untyped))
        {
            dict->getString(attr_name, key_columns, key_types, default_col, out.get());
        }
        else if (const auto default_col = typeid_cast<const ColumnConst<String> *>(default_col_untyped))
        {
            const auto & def = default_col->getData();
            dict->getString(attr_name, key_columns, key_types, def, out.get());
        }
        else
            throw Exception{
                "Fourth argument of function " + getName() + " must be String",
                ErrorCodes::ILLEGAL_COLUMN};

        return true;
    }

    const ExternalDictionaries & dictionaries;
};


template <typename DataType> struct DictGetTraits;
#define DECLARE_DICT_GET_TRAITS(TYPE, DATA_TYPE) \
template <> struct DictGetTraits<DATA_TYPE>\
{\
    template <typename DictionaryType>\
    static void get(\
        const DictionaryType * dict, const std::string & name, const PaddedPODArray<UInt64> & ids,\
        PaddedPODArray<TYPE> & out)\
    {\
        dict->get##TYPE(name, ids, out);\
    }\
    template <typename DictionaryType>\
    static void get(\
        const DictionaryType * dict, const std::string & name, const ConstColumnPlainPtrs & key_columns,\
        const DataTypes & key_types, PaddedPODArray<TYPE> & out)\
    {\
        dict->get##TYPE(name, key_columns, key_types, out);\
    }\
    template <typename DictionaryType>\
    static void get(\
        const DictionaryType * dict, const std::string & name, const PaddedPODArray<UInt64> & ids,\
        const PaddedPODArray<UInt16> & dates, PaddedPODArray<TYPE> & out)\
    {\
        dict->get##TYPE(name, ids, dates, out);\
    }\
    template <typename DictionaryType, typename DefaultsType>\
    static void getOrDefault(\
        const DictionaryType * dict, const std::string & name, const PaddedPODArray<UInt64> & ids,\
        const DefaultsType & def, PaddedPODArray<TYPE> & out)\
    {\
        dict->get##TYPE(name, ids, def, out);\
    }\
    template <typename DictionaryType, typename DefaultsType>\
    static void getOrDefault(\
        const DictionaryType * dict, const std::string & name, const ConstColumnPlainPtrs & key_columns,\
        const DataTypes & key_types, const DefaultsType & def, PaddedPODArray<TYPE> & out)\
    {\
        dict->get##TYPE(name, key_columns, key_types, def, out);\
    }\
};
DECLARE_DICT_GET_TRAITS(UInt8, DataTypeUInt8)
DECLARE_DICT_GET_TRAITS(UInt16, DataTypeUInt16)
DECLARE_DICT_GET_TRAITS(UInt32, DataTypeUInt32)
DECLARE_DICT_GET_TRAITS(UInt64, DataTypeUInt64)
DECLARE_DICT_GET_TRAITS(Int8, DataTypeInt8)
DECLARE_DICT_GET_TRAITS(Int16, DataTypeInt16)
DECLARE_DICT_GET_TRAITS(Int32, DataTypeInt32)
DECLARE_DICT_GET_TRAITS(Int64, DataTypeInt64)
DECLARE_DICT_GET_TRAITS(Float32, DataTypeFloat32)
DECLARE_DICT_GET_TRAITS(Float64, DataTypeFloat64)
DECLARE_DICT_GET_TRAITS(UInt16, DataTypeDate)
DECLARE_DICT_GET_TRAITS(UInt32, DataTypeDateTime)
#undef DECLARE_DICT_GET_TRAITS

template <typename DataType>
class FunctionDictGet final : public IFunction
{
    using Type = typename DataType::FieldType;

public:
    static const std::string name;

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictGet>(context.getExternalDictionaries());
    }

    FunctionDictGet(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    bool isInjective(const Block & sample_block) override
    {
        return isDictGetFunctionInjective(dictionaries, sample_block);
    }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 3 && arguments.size() != 4)
            throw Exception{"Function " + getName() + " takes 3 or 4 arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
        {
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[2].get()) &&
            !typeid_cast<const DataTypeTuple *>(arguments[2].get()))
        {
            throw Exception{
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName()
                    + ", must be UInt64 or tuple(...).",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (arguments.size() == 4 && !typeid_cast<const DataTypeDate *>(arguments[3].get()))
        {
            throw Exception{
                "Illegal type " + arguments[3]->getName() + " of fourth argument of function " + getName()
                + ", must be Date.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataType>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatch<FlatDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<HashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<CacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyHashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<TrieDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchRange<RangeHashedDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (arguments.size() != 3)
            throw Exception{
                "Function " + getName() + " for dictionary of type " + dict->getTypeName() +
                " requires exactly 3 arguments.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
        {
            const auto out = std::make_shared<ColumnVector<Type>>(id_col->size());
            block.safeGetByPosition(result).column = out;

            const auto & ids = id_col->getData();
            auto & data = out->getData();

            DictGetTraits<DataType>::get(dict, attr_name, ids, data);
        }
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
        {
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            PaddedPODArray<Type> data(1);
            DictGetTraits<DataType>::get(dict, attr_name, ids, data);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<Type>>(id_col->size(), data.front());
        }
        else
        {
            throw Exception{
                "Third argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    template <typename DictionaryType>
    bool executeDispatchComplex(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (arguments.size() != 3)
            throw Exception{
                "Function " + getName() + " for dictionary of type " + dict->getTypeName() +
                    " requires exactly 3 arguments",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto key_col_with_type = block.safeGetByPosition(arguments[2]);
        if (typeid_cast<const ColumnTuple *>(key_col_with_type.column.get())
            || typeid_cast<const ColumnConstTuple *>(key_col_with_type.column.get()))
        {
            const ColumnPtr key_col_materialized = key_col_with_type.column->convertToFullColumnIfConst();

            const auto key_columns = ext::map<ConstColumnPlainPtrs>(
                static_cast<const ColumnTuple &>(*key_col_materialized.get()).getColumns(),
                [](const ColumnPtr & ptr) { return ptr.get(); });

            const auto & key_types = static_cast<const DataTypeTuple &>(*key_col_with_type.type).getElements();

            const auto out = std::make_shared<ColumnVector<Type>>(key_columns.front()->size());
            block.safeGetByPosition(result).column = out;

            auto & data = out->getData();

            DictGetTraits<DataType>::get(dict, attr_name, key_columns, key_types, data);
        }
        else
            throw Exception{
                "Third argument of function " + getName() + " must be " + dict->getKeyDescription(),
                ErrorCodes::TYPE_MISMATCH};

        return true;
    }

    template <typename DictionaryType>
    bool executeDispatchRange(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (arguments.size() != 4)
            throw Exception{
                "Function " + getName() + " for dictionary of type " + dict->getTypeName() +
                " requires exactly 4 arguments",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();
        const auto date_col_untyped = block.safeGetByPosition(arguments[3]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
            executeRange(block, result, dict, attr_name, id_col, date_col_untyped);
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
            executeRange(block, result, dict, attr_name, id_col, date_col_untyped);
        else
        {
            throw Exception{
                "Third argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    template <typename DictionaryType>
    void executeRange(
        Block & block, const size_t result, const DictionaryType * dictionary, const std::string & attr_name,
        const ColumnUInt64 * id_col, const IColumn * date_col_untyped)
    {
        if (const auto date_col = typeid_cast<const ColumnUInt16 *>(date_col_untyped))
        {
            const auto size = id_col->size();
            const auto & ids = id_col->getData();
            const auto & dates = date_col->getData();

            const auto out = std::make_shared<ColumnVector<Type>>(size);
            block.safeGetByPosition(result).column = out;

            auto & data = out->getData();
            DictGetTraits<DataType>::get(dictionary, attr_name, ids, dates, data);
        }
        else if (const auto date_col = typeid_cast<const ColumnConst<UInt16> *>(date_col_untyped))
        {
            const auto size = id_col->size();
            const auto & ids = id_col->getData();
            const PaddedPODArray<UInt16> dates(size, date_col->getData());

            const auto out = std::make_shared<ColumnVector<Type>>(size);
            block.safeGetByPosition(result).column = out;

            auto & data = out->getData();
            DictGetTraits<DataType>::get(dictionary, attr_name, ids, dates, data);
        }
        else
        {
            throw Exception{
                "Fourth argument of function " + getName() + " must be Date",
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }

    template <typename DictionaryType>
    void executeRange(
        Block & block, const size_t result, const DictionaryType * dictionary, const std::string & attr_name,
        const ColumnConst<UInt64> * id_col, const IColumn * date_col_untyped)
    {
        if (const auto date_col = typeid_cast<const ColumnUInt16 *>(date_col_untyped))
        {
            const auto size = date_col->size();
            const PaddedPODArray<UInt64> ids(size, id_col->getData());
            const auto & dates = date_col->getData();

            const auto out = std::make_shared<ColumnVector<Type>>(size);
            block.safeGetByPosition(result).column = out;

            auto & data = out->getData();
            DictGetTraits<DataType>::get(dictionary, attr_name, ids, dates, data);
        }
        else if (const auto date_col = typeid_cast<const ColumnConst<UInt16> *>(date_col_untyped))
        {
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            const PaddedPODArray<UInt16> dates(1, date_col->getData());
            PaddedPODArray<Type> data(1);
            DictGetTraits<DataType>::get(dictionary, attr_name, ids, dates, data);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<Type>>(id_col->size(), data.front());
        }
        else
        {
            throw Exception{
                "Fourth argument of function " + getName() + " must be Date",
                ErrorCodes::ILLEGAL_COLUMN};
        }
    }

    const ExternalDictionaries & dictionaries;
};

template <typename DataType>
const std::string FunctionDictGet<DataType>::name = "dictGet" + DataType{}.getName();


using FunctionDictGetUInt8 = FunctionDictGet<DataTypeUInt8>;
using FunctionDictGetUInt16 = FunctionDictGet<DataTypeUInt16>;
using FunctionDictGetUInt32 = FunctionDictGet<DataTypeUInt32>;
using FunctionDictGetUInt64 = FunctionDictGet<DataTypeUInt64>;
using FunctionDictGetInt8 = FunctionDictGet<DataTypeInt8>;
using FunctionDictGetInt16 = FunctionDictGet<DataTypeInt16>;
using FunctionDictGetInt32 = FunctionDictGet<DataTypeInt32>;
using FunctionDictGetInt64 = FunctionDictGet<DataTypeInt64>;
using FunctionDictGetFloat32 = FunctionDictGet<DataTypeFloat32>;
using FunctionDictGetFloat64 = FunctionDictGet<DataTypeFloat64>;
using FunctionDictGetDate = FunctionDictGet<DataTypeDate>;
using FunctionDictGetDateTime = FunctionDictGet<DataTypeDateTime>;


template <typename DataType>
class FunctionDictGetOrDefault final : public IFunction
{
    using Type = typename DataType::FieldType;

public:
    static const std::string name;

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictGetOrDefault>(context.getExternalDictionaries());
    }

    FunctionDictGetOrDefault(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 4; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
        {
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeString *>(arguments[1].get()))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[2].get()) &&
            !typeid_cast<const DataTypeTuple *>(arguments[2].get()))
        {
            throw Exception{
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName()
                    + ", must be UInt64 or tuple(...).",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataType *>(arguments[3].get()))
        {
            throw Exception{
                "Illegal type " + arguments[3]->getName() + " of fourth argument of function " + getName()
                    + ", must be " + DataType{}.getName() + ".",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataType>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatch<FlatDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<HashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<CacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyHashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatchComplex<TrieDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
            executeDispatch(block, arguments, result, dict, attr_name, id_col);
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
            executeDispatch(block, arguments, result, dict, attr_name, id_col);
        else
            throw Exception{
                "Third argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};

        return true;
    }

    template <typename DictionaryType>
    void executeDispatch(
        Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * dictionary,
        const std::string & attr_name, const ColumnUInt64 * id_col)
    {
        const auto default_col_untyped = block.safeGetByPosition(arguments[3]).column.get();

        if (const auto default_col = typeid_cast<const ColumnVector<Type> *>(default_col_untyped))
        {
            /// vector ids, vector defaults
            const auto out = std::make_shared<ColumnVector<Type>>(id_col->size());
            block.safeGetByPosition(result).column = out;

            const auto & ids = id_col->getData();
            auto & data = out->getData();
            const auto & defs = default_col->getData();

            DictGetTraits<DataType>::getOrDefault(dictionary, attr_name, ids, defs, data);
        }
        else if (const auto default_col =  typeid_cast<const ColumnConst<Type> *>(default_col_untyped))
        {
            /// vector ids, const defaults
            const auto out = std::make_shared<ColumnVector<Type>>(id_col->size());
            block.safeGetByPosition(result).column = out;

            const auto & ids = id_col->getData();
            auto & data = out->getData();
            const auto def = default_col->getData();

            DictGetTraits<DataType>::getOrDefault(dictionary, attr_name, ids, def, data);
        }
        else
            throw Exception{
                "Fourth argument of function " + getName() + " must be " + DataType{}.getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename DictionaryType>
    void executeDispatch(
        Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * dictionary,
        const std::string & attr_name, const ColumnConst<UInt64> * id_col)
    {
        const auto default_col_untyped = block.safeGetByPosition(arguments[3]).column.get();

        if (const auto default_col = typeid_cast<const ColumnVector<Type> *>(default_col_untyped))
        {
            /// const ids, vector defaults
            /// @todo avoid materialization
            const PaddedPODArray<UInt64> ids(id_col->size(), id_col->getData());

            const auto out = std::make_shared<ColumnVector<Type>>(id_col->size());
            block.safeGetByPosition(result).column = out;

            auto & data = out->getData();
            const auto & defs = default_col->getData();

            DictGetTraits<DataType>::getOrDefault(dictionary, attr_name, ids, defs, data);
        }
        else if (const auto default_col = typeid_cast<const ColumnConst<Type> *>(default_col_untyped))
        {
            /// const ids, const defaults
            const PaddedPODArray<UInt64> ids(1, id_col->getData());
            PaddedPODArray<Type> data(1);
            const auto & def = default_col->getData();

            DictGetTraits<DataType>::getOrDefault(dictionary, attr_name, ids, def, data);

            block.safeGetByPosition(result).column = std::make_shared<ColumnConst<Type>>(id_col->size(), data.front());
        }
        else
            throw Exception{
                "Fourth argument of function " + getName() + " must be " + DataType{}.getName(),
                ErrorCodes::ILLEGAL_COLUMN};
    }

    template <typename DictionaryType>
    bool executeDispatchComplex(
        Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        const auto attr_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[1]).column.get());
        if (!attr_name_col)
            throw Exception{
                "Second argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        const auto & attr_name = attr_name_col->getData();

        const auto key_col_with_type = block.safeGetByPosition(arguments[2]);
        const auto & key_col = typeid_cast<const ColumnTuple &>(*key_col_with_type.column);

        const ColumnPtr key_col_materialized = key_col.convertToFullColumnIfConst();

        const auto key_columns = ext::map<ConstColumnPlainPtrs>(
            static_cast<const ColumnTuple &>(*key_col_materialized.get()).getColumns(), [](const ColumnPtr & ptr) { return ptr.get(); });

        const auto & key_types = static_cast<const DataTypeTuple &>(*key_col_with_type.type).getElements();

        /// @todo detect when all key columns are constant
        const auto rows = key_col.size();
        const auto out = std::make_shared<ColumnVector<Type>>(rows);
        block.safeGetByPosition(result).column = out;
        auto & data = out->getData();

        const auto default_col_untyped = block.safeGetByPosition(arguments[3]).column.get();
        if (const auto default_col = typeid_cast<const ColumnVector<Type> *>(default_col_untyped))
        {
            /// const defaults
            const auto & defs = default_col->getData();

            DictGetTraits<DataType>::getOrDefault(dict, attr_name, key_columns, key_types, defs, data);
        }
        else if (const auto default_col = typeid_cast<const ColumnConst<Type> *>(default_col_untyped))
        {
            const auto def = default_col->getData();

            DictGetTraits<DataType>::getOrDefault(dict, attr_name, key_columns, key_types, def, data);
        }
        else
            throw Exception{
                "Fourth argument of function " + getName() + " must be " + DataType{}.getName(),
                ErrorCodes::ILLEGAL_COLUMN};

        return true;
    }

    const ExternalDictionaries & dictionaries;
};

template <typename DataType>
const std::string FunctionDictGetOrDefault<DataType>::name = "dictGet" + DataType{}.getName() + "OrDefault";


using FunctionDictGetUInt8OrDefault = FunctionDictGetOrDefault<DataTypeUInt8>;
using FunctionDictGetUInt16OrDefault = FunctionDictGetOrDefault<DataTypeUInt16>;
using FunctionDictGetUInt32OrDefault = FunctionDictGetOrDefault<DataTypeUInt32>;
using FunctionDictGetUInt64OrDefault = FunctionDictGetOrDefault<DataTypeUInt64>;
using FunctionDictGetInt8OrDefault = FunctionDictGetOrDefault<DataTypeInt8>;
using FunctionDictGetInt16OrDefault = FunctionDictGetOrDefault<DataTypeInt16>;
using FunctionDictGetInt32OrDefault = FunctionDictGetOrDefault<DataTypeInt32>;
using FunctionDictGetInt64OrDefault = FunctionDictGetOrDefault<DataTypeInt64>;
using FunctionDictGetFloat32OrDefault = FunctionDictGetOrDefault<DataTypeFloat32>;
using FunctionDictGetFloat64OrDefault = FunctionDictGetOrDefault<DataTypeFloat64>;
using FunctionDictGetDateOrDefault = FunctionDictGetOrDefault<DataTypeDate>;
using FunctionDictGetDateTimeOrDefault = FunctionDictGetOrDefault<DataTypeDateTime>;


/// Functions to work with hierarchies.

class FunctionDictGetHierarchy final : public IFunction
{
public:
    static constexpr auto name = "dictGetHierarchy";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictGetHierarchy>(context.getExternalDictionaries());
    }

    FunctionDictGetHierarchy(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 2; }
    bool isInjective(const Block & sample_block) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
        {
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[1].get()))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ", must be UInt64.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatch<FlatDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<HashedDictionary>(block, arguments, result, dict_ptr) &&
            !executeDispatch<CacheDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (!dict->hasHierarchy())
            throw Exception{
                "Dictionary does not have a hierarchy",
                ErrorCodes::UNSUPPORTED_METHOD};

        const auto get_hierarchies = [&] (const PaddedPODArray<UInt64> & in, PaddedPODArray<UInt64> & out, PaddedPODArray<UInt64> & offsets)
        {
            const auto size = in.size();

            /// copy of `in` array
            auto in_array = std::make_unique<PaddedPODArray<UInt64>>(std::begin(in), std::end(in));
            /// used for storing and handling result of ::toParent call
            auto out_array = std::make_unique<PaddedPODArray<UInt64>>(size);
            /// resulting hierarchies
            std::vector<std::vector<IDictionary::Key>> hierarchies(size);    /// TODO Bad code, poor performance.

            /// total number of non-zero elements, used for allocating all the required memory upfront
            std::size_t total_count = 0;

            while (true)
            {
                auto all_zeroes = true;

                /// erase zeroed identifiers, store non-zeroed ones
                for (const auto i : ext::range(0, size))
                {
                    const auto id = (*in_array)[i];
                    if (0 == id)
                        continue;

                    all_zeroes = false;
                    /// place id at it's corresponding place
                    hierarchies[i].push_back(id);

                    ++total_count;
                }

                if (all_zeroes)
                    break;

                /// translate all non-zero identifiers at once
                dict->toParent(*in_array, *out_array);

                /// we're going to use the `in_array` from this iteration as `out_array` on the next one
                std::swap(in_array, out_array);
            }

            out.reserve(total_count);
            offsets.resize(size);

            for (const auto i : ext::range(0, size))
            {
                const auto & ids = hierarchies[i];
                out.insert_assume_reserved(std::begin(ids), std::end(ids));
                offsets[i] = out.size();
            }
        };

        const auto id_col_untyped = block.safeGetByPosition(arguments[1]).column.get();
        if (const auto id_col = typeid_cast<const ColumnUInt64 *>(id_col_untyped))
        {
            const auto & in = id_col->getData();
            const auto backend = std::make_shared<ColumnUInt64>();
            const auto array = std::make_shared<ColumnArray>(backend);
            block.safeGetByPosition(result).column = array;

            get_hierarchies(in, backend->getData(), array->getOffsets());
        }
        else if (const auto id_col = typeid_cast<const ColumnConst<UInt64> *>(id_col_untyped))
        {
            const PaddedPODArray<UInt64> in(1, id_col->getData());
            const auto backend = std::make_shared<ColumnUInt64>();
            const auto array = std::make_shared<ColumnArray>(backend);

            get_hierarchies(in, backend->getData(), array->getOffsets());

            block.safeGetByPosition(result).column = std::make_shared<ColumnConstArray>(
                id_col->size(),
                (*array)[0].get<Array>(),
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()));
        }
        else
        {
            throw Exception{
                "Second argument of function " + getName() + " must be UInt64",
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    const ExternalDictionaries & dictionaries;
};


class FunctionDictIsIn final : public IFunction
{
public:
    static constexpr auto name = "dictIsIn";

    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionDictIsIn>(context.getExternalDictionaries());
    }

    FunctionDictIsIn(const ExternalDictionaries & dictionaries) : dictionaries(dictionaries) {}

    String getName() const override { return name; }

private:
    size_t getNumberOfArguments() const override { return 3; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!typeid_cast<const DataTypeString *>(arguments[0].get()))
        {
            throw Exception{
                "Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                    + ", expected a string.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[1].get()))
        {
            throw Exception{
                "Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                    + ", must be UInt64.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        if (!typeid_cast<const DataTypeUInt64 *>(arguments[2].get()))
        {
            throw Exception{
                "Illegal type " + arguments[2]->getName() + " of third argument of function " + getName()
                    + ", must be UInt64.",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT};
        }

        return std::make_shared<DataTypeUInt8>();
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, const size_t result) override
    {
        const auto dict_name_col = typeid_cast<const ColumnConst<String> *>(block.safeGetByPosition(arguments[0]).column.get());
        if (!dict_name_col)
            throw Exception{
                "First argument of function " + getName() + " must be a constant string",
                ErrorCodes::ILLEGAL_COLUMN};

        auto dict = dictionaries.getDictionary(dict_name_col->getData());
        const auto dict_ptr = dict.get();

        if (!executeDispatch<FlatDictionary>(block, arguments, result, dict_ptr)
            && !executeDispatch<HashedDictionary>(block, arguments, result, dict_ptr)
            && !executeDispatch<CacheDictionary>(block, arguments, result, dict_ptr))
            throw Exception{
                "Unsupported dictionary type " + dict_ptr->getTypeName(),
                ErrorCodes::UNKNOWN_TYPE};
    }

    template <typename DictionaryType>
    bool executeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
        const IDictionaryBase * dictionary)
    {
        const auto dict = typeid_cast<const DictionaryType *>(dictionary);
        if (!dict)
            return false;

        if (!dict->hasHierarchy())
            throw Exception{
                "Dictionary does not have a hierarchy",
                ErrorCodes::UNSUPPORTED_METHOD};

        const auto child_id_col_untyped = block.safeGetByPosition(arguments[1]).column.get();
        const auto ancestor_id_col_untyped = block.safeGetByPosition(arguments[2]).column.get();

        if (const auto child_id_col = typeid_cast<const ColumnUInt64 *>(child_id_col_untyped))
            execute(block, result, dict, child_id_col, ancestor_id_col_untyped);
        else if (const auto child_id_col = typeid_cast<const ColumnConst<UInt64> *>(child_id_col_untyped))
            execute(block, result, dict, child_id_col, ancestor_id_col_untyped);
        else
            throw Exception{
                "Illegal column " + child_id_col_untyped->getName()
                    + " of second argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};

        return true;
    }

    template <typename DictionaryType>
    bool execute(Block & block, const size_t result, const DictionaryType * dictionary,
        const ColumnUInt64 * child_id_col, const IColumn * ancestor_id_col_untyped)
    {
        if (const auto ancestor_id_col = typeid_cast<const ColumnUInt64 *>(ancestor_id_col_untyped))
        {
            const auto out = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = out;

            const auto & child_ids = child_id_col->getData();
            const auto & ancestor_ids = ancestor_id_col->getData();
            auto & data = out->getData();
            const auto size = child_id_col->size();
            data.resize(size);

            dictionary->isInVectorVector(child_ids, ancestor_ids, data);
        }
        else if (const auto ancestor_id_col = typeid_cast<const ColumnConst<UInt64> *>(ancestor_id_col_untyped))
        {
            const auto out = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = out;

            const auto & child_ids = child_id_col->getData();
            const auto ancestor_id = ancestor_id_col->getData();
            auto & data = out->getData();
            const auto size = child_id_col->size();
            data.resize(size);

            dictionary->isInVectorConstant(child_ids, ancestor_id, data);
        }
        else
        {
            throw Exception{
                "Illegal column " + ancestor_id_col_untyped->getName()
                    + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    template <typename DictionaryType>
    bool execute(Block & block, const size_t result, const DictionaryType * dictionary,
        const ColumnConst<UInt64> * child_id_col, const IColumn * ancestor_id_col_untyped)
    {
        if (const auto ancestor_id_col = typeid_cast<const ColumnUInt64 *>(ancestor_id_col_untyped))
        {
            const auto out = std::make_shared<ColumnUInt8>();
            block.safeGetByPosition(result).column = out;

            const auto child_id = child_id_col->getData();
            const auto & ancestor_ids = ancestor_id_col->getData();
            auto & data = out->getData();
            const auto size = child_id_col->size();
            data.resize(size);

            dictionary->isInConstantVector(child_id, ancestor_ids, data);
        }
        else if (const auto ancestor_id_col = typeid_cast<const ColumnConst<UInt64> *>(ancestor_id_col_untyped))
        {
            const auto child_id = child_id_col->getData();
            const auto ancestor_id = ancestor_id_col->getData();
            UInt8 res = 0;

            dictionary->isInConstantConstant(child_id, ancestor_id, res);

            block.getByPosition(result).column = std::make_shared<ColumnConst<UInt8>>(
                child_id_col->size(), res);
        }
        else
        {
            throw Exception{
                "Illegal column " + ancestor_id_col_untyped->getName()
                    + " of third argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN};
        }

        return true;
    }

    const ExternalDictionaries & dictionaries;
};


};
