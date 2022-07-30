#pragma once

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Dictionaries/Embedded/RegionsHierarchy.h>
#include <Dictionaries/Embedded/RegionsHierarchies.h>
#include <Dictionaries/Embedded/RegionsNames.h>
#include <IO/WriteHelpers.h>
#include <Common/typeid_cast.h>
#include <Core/Defines.h>

#include <Common/config.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int DICTIONARIES_WAS_NOT_LOADED;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Functions using deprecated dictionaries
  * - dictionaries of regions, operating systems, search engines.
  *
  * Climb up the tree to a certain level.
  *  regionToCity, regionToArea, regionToCountry, ...
  *
  * Convert values of a column
  *  regionToName
  *
  * Whether the first identifier is a descendant of the second.
  *  regionIn
  *
  * Get an array of region identifiers, consisting of the source and the parents chain. Order implementation defined.
  *  regionHierarchy
  */


struct RegionToCityImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toCity(x); }
};

struct RegionToAreaImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toArea(x); }
};

struct RegionToDistrictImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toDistrict(x); }
};

struct RegionToCountryImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toCountry(x); }
};

struct RegionToContinentImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toContinent(x); }
};

struct RegionToTopContinentImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toTopContinent(x); }
};

struct RegionToPopulationImpl
{
    static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.getPopulation(x); }
};

struct RegionInImpl
{
    static bool apply(UInt32 x, UInt32 y, const RegionsHierarchy & hierarchy) { return hierarchy.in(x, y); }
};

struct RegionHierarchyImpl
{
    static UInt32 toParent(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toParent(x); }
};


/** Auxiliary thing, allowing to get from the dictionary a specific dictionary, corresponding to the point of view
  *  (the dictionary key passed as function argument).
  * Example: when calling regionToCountry(x, 'ua'), a dictionary can be used, in which Crimea refers to Ukraine.
  */
struct RegionsHierarchyGetter
{
    using Src = const RegionsHierarchies;
    using Dst = const RegionsHierarchy;

    static Dst & get(Src & src, const std::string & key)
    {
        return src.get(key);
    }
};

/** For dictionaries without key support. Doing nothing.
  */
template <typename Dict>
struct IdentityDictionaryGetter
{
    using Src = const Dict;
    using Dst = const Dict;

    static Dst & get(Src & src, const std::string & key)
    {
        if (key.empty())
            return src;
        else
            throw Exception("Dictionary doesn't support 'point of view' keys.", ErrorCodes::BAD_ARGUMENTS);
    }
};


/// Converts an identifier using a dictionary.
template <typename T, typename Transform, typename DictGetter, typename Name>
class FunctionTransformWithDictionary : public IFunction
{
public:
    static constexpr auto name = Name::name;
    using base_type = FunctionTransformWithDictionary;

private:
    const std::shared_ptr<typename DictGetter::Src> owned_dict;

public:
    FunctionTransformWithDictionary(const std::shared_ptr<typename DictGetter::Src> & owned_dict_)
        : owned_dict(owned_dict_)
    {
        if (!owned_dict)
            throw Exception("Embedded dictionaries were not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<T>)
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                + " (must be " + String(TypeName<T>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>)
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second ('point of view') argument of function " + getName()
                + " (must be " + String(TypeName<T>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        /// The dictionary key that defines the "point of view".
        std::string dict_key;

        if (arguments.size() == 2)
        {
            const ColumnConst * key_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

            if (!key_col)
                throw Exception("Illegal column " + arguments[1].column->getName()
                    + " of second ('point of view') argument of function " + name
                    + ". Must be constant string.",
                    ErrorCodes::ILLEGAL_COLUMN);

            dict_key = key_col->getValue<String>();
        }

        const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get()))
        {
            auto col_to = ColumnVector<T>::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            typename ColumnVector<T>::Container & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Transform::apply(vec_from[i], dict);

            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of first argument of function " + name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Checks belonging using a dictionary.
template <typename T, typename Transform, typename DictGetter, typename Name>
class FunctionIsInWithDictionary : public IFunction
{
public:
    static constexpr auto name = Name::name;
    using base_type = FunctionIsInWithDictionary;

private:
    const std::shared_ptr<typename DictGetter::Src> owned_dict;

public:
    FunctionIsInWithDictionary(const std::shared_ptr<typename DictGetter::Src> & owned_dict_)
        : owned_dict(owned_dict_)
    {
        if (!owned_dict)
            throw Exception("Embedded dictionaries were not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<T>)
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                + " (must be " + String(TypeName<T>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments[1]->getName() != TypeName<T>)
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                + " (must be " + String(TypeName<T>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3 && arguments[2]->getName() != TypeName<String>)
            throw Exception("Illegal type " + arguments[2]->getName() + " of the third ('point of view') argument of function " + getName()
                + " (must be " + String(TypeName<String>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        /// The dictionary key that defines the "point of view".
        std::string dict_key;

        if (arguments.size() == 3)
        {
            const ColumnConst * key_col = checkAndGetColumnConst<ColumnString>(arguments[2].column.get());

            if (!key_col)
                throw Exception("Illegal column " + arguments[2].column->getName()
                + " of third ('point of view') argument of function " + name
                + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

            dict_key = key_col->getValue<String>();
        }

        const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

        const ColumnVector<T> * col_vec1 = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get());
        const ColumnVector<T> * col_vec2 = checkAndGetColumn<ColumnVector<T>>(arguments[1].column.get());
        const ColumnConst * col_const1 = checkAndGetColumnConst<ColumnVector<T>>(arguments[0].column.get());
        const ColumnConst * col_const2 = checkAndGetColumnConst<ColumnVector<T>>(arguments[1].column.get());

        if (col_vec1 && col_vec2)
        {
            auto col_to = ColumnUInt8::create();

            const typename ColumnVector<T>::Container & vec_from1 = col_vec1->getData();
            const typename ColumnVector<T>::Container & vec_from2 = col_vec2->getData();
            typename ColumnUInt8::Container & vec_to = col_to->getData();
            size_t size = vec_from1.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Transform::apply(vec_from1[i], vec_from2[i], dict);

            return col_to;
        }
        else if (col_vec1 && col_const2)
        {
            auto col_to = ColumnUInt8::create();

            const typename ColumnVector<T>::Container & vec_from1 = col_vec1->getData();
            const T const_from2 = col_const2->template getValue<T>();
            typename ColumnUInt8::Container & vec_to = col_to->getData();
            size_t size = vec_from1.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Transform::apply(vec_from1[i], const_from2, dict);

            return col_to;
        }
        else if (col_const1 && col_vec2)
        {
            auto col_to = ColumnUInt8::create();

            const T const_from1 = col_const1->template getValue<T>();
            const typename ColumnVector<T>::Container & vec_from2 = col_vec2->getData();
            typename ColumnUInt8::Container & vec_to = col_to->getData();
            size_t size = vec_from2.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Transform::apply(const_from1, vec_from2[i], dict);

            return col_to;
        }
        else if (col_const1 && col_const2)
        {
            return DataTypeUInt8().createColumnConst(col_const1->size(),
                toField(Transform::apply(col_const1->template getValue<T>(), col_const2->template getValue<T>(), dict)));
        }
        else
            throw Exception("Illegal columns " + arguments[0].column->getName()
                    + " and " + arguments[1].column->getName()
                    + " of arguments of function " + name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


/// Gets an array of identifiers consisting of the source and the parents chain.
template <typename T, typename Transform, typename DictGetter, typename Name>
class FunctionHierarchyWithDictionary : public IFunction
{
public:
    static constexpr auto name = Name::name;
    using base_type = FunctionHierarchyWithDictionary;

private:
    const std::shared_ptr<typename DictGetter::Src> owned_dict;

public:
    FunctionHierarchyWithDictionary(const std::shared_ptr<typename DictGetter::Src> & owned_dict_)
    : owned_dict(owned_dict_)
    {
        if (!owned_dict)
            throw Exception("Embedded dictionaries were not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<T>)
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
            + " (must be " + String(TypeName<T>) + ")",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>)
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second ('point of view') argument of function " + getName()
                + " (must be " + String(TypeName<String>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        /// The dictionary key that defines the "point of view".
        std::string dict_key;

        if (arguments.size() == 2)
        {
            const ColumnConst * key_col = checkAndGetColumnConst<ColumnString>(arguments[1].column.get());

            if (!key_col)
                throw Exception("Illegal column " + arguments[1].column->getName()
                + " of second ('point of view') argument of function " + name
                + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

            dict_key = key_col->getValue<String>();
        }

        const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(arguments[0].column.get()))
        {
            auto col_values = ColumnVector<T>::create();
            auto col_offsets = ColumnArray::ColumnOffsets::create();

            auto & res_offsets = col_offsets->getData();
            auto & res_values = col_values->getData();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            size_t size = vec_from.size();
            res_offsets.resize(size);
            res_values.reserve(size * 4);

            for (size_t i = 0; i < size; ++i)
            {
                T cur = vec_from[i];
                for (size_t depth = 0; cur && depth < DBMS_HIERARCHICAL_DICTIONARY_MAX_DEPTH; ++depth)
                {
                    res_values.push_back(cur);
                    cur = Transform::toParent(cur, dict);
                }
                res_offsets[i] = res_values.size();
            }

            return ColumnArray::create(std::move(col_values), std::move(col_offsets));
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                + " of first argument of function " + name,
                ErrorCodes::ILLEGAL_COLUMN);
    }
};


struct NameRegionToCity                { static constexpr auto name = "regionToCity"; };
struct NameRegionToArea                { static constexpr auto name = "regionToArea"; };
struct NameRegionToDistrict            { static constexpr auto name = "regionToDistrict"; };
struct NameRegionToCountry             { static constexpr auto name = "regionToCountry"; };
struct NameRegionToContinent           { static constexpr auto name = "regionToContinent"; };
struct NameRegionToTopContinent        { static constexpr auto name = "regionToTopContinent"; };
struct NameRegionToPopulation          { static constexpr auto name = "regionToPopulation"; };
struct NameRegionHierarchy             { static constexpr auto name = "regionHierarchy"; };
struct NameRegionIn                    { static constexpr auto name = "regionIn"; };


struct FunctionRegionToCity :
    public FunctionTransformWithDictionary<UInt32, RegionToCityImpl,    RegionsHierarchyGetter,    NameRegionToCity>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToArea :
    public FunctionTransformWithDictionary<UInt32, RegionToAreaImpl,    RegionsHierarchyGetter,    NameRegionToArea>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToDistrict :
    public FunctionTransformWithDictionary<UInt32, RegionToDistrictImpl, RegionsHierarchyGetter, NameRegionToDistrict>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToCountry :
    public FunctionTransformWithDictionary<UInt32, RegionToCountryImpl, RegionsHierarchyGetter, NameRegionToCountry>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToContinent :
    public FunctionTransformWithDictionary<UInt32, RegionToContinentImpl, RegionsHierarchyGetter, NameRegionToContinent>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToTopContinent :
    public FunctionTransformWithDictionary<UInt32, RegionToTopContinentImpl, RegionsHierarchyGetter, NameRegionToTopContinent>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToPopulation :
    public FunctionTransformWithDictionary<UInt32, RegionToPopulationImpl, RegionsHierarchyGetter, NameRegionToPopulation>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionIn :
    public FunctionIsInWithDictionary<UInt32, RegionInImpl, RegionsHierarchyGetter,    NameRegionIn>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionHierarchy :
    public FunctionHierarchyWithDictionary<UInt32, RegionHierarchyImpl, RegionsHierarchyGetter, NameRegionHierarchy>
{
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<base_type>(context->getEmbeddedDictionaries().getRegionsHierarchies());
    }
};


/// Converts a region's numeric identifier to a name in the specified language using a dictionary.
class FunctionRegionToName : public IFunction
{
public:
    static constexpr auto name = "regionToName";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionRegionToName>(context->getEmbeddedDictionaries().getRegionsNames());
    }

private:
    const MultiVersion<RegionsNames>::Version owned_dict;

public:
    FunctionRegionToName(const MultiVersion<RegionsNames>::Version & owned_dict_)
        : owned_dict(owned_dict_)
    {
        if (!owned_dict)
            throw Exception("Embedded dictionaries were not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// For the purpose of query optimization, we assume this function to be injective
    ///  even in face of fact that there are many different cities named Paris.
    bool isInjective(const ColumnsWithTypeAndName &) const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<UInt32>)
            throw Exception("Illegal type " + arguments[0]->getName() + " of the first argument of function " + getName()
                + " (must be " + String(TypeName<UInt32>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>)
            throw Exception("Illegal type " + arguments[0]->getName() + " of the second argument of function " + getName()
                + " (must be " + String(TypeName<String>) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isDeterministic() const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t /*input_rows_count*/) const override
    {
        RegionsNames::Language language = RegionsNames::Language::ru;

        /// If the result language is specified
        if (arguments.size() == 2)
        {
            if (const ColumnConst * col_language = checkAndGetColumnConst<ColumnString>(arguments[1].column.get()))
                language = RegionsNames::getLanguageEnum(col_language->getValue<String>());
            else
                throw Exception("Illegal column " + arguments[1].column->getName()
                        + " of the second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        const RegionsNames & dict = *owned_dict;

        if (const ColumnUInt32 * col_from = typeid_cast<const ColumnUInt32 *>(arguments[0].column.get()))
        {
            auto col_to = ColumnString::create();

            const ColumnUInt32::Container & region_ids = col_from->getData();

            for (unsigned int region_id : region_ids)
            {
                const StringRef & name_ref = dict.getRegionName(region_id, language);
                col_to->insertDataWithTerminatingZero(name_ref.data, name_ref.size + 1);
            }

            return col_to;
        }
        else
            throw Exception("Illegal column " + arguments[0].column->getName()
                    + " of the first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

}
