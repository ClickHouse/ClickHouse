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

#include <Common/config.h>
#include <Common/typeid_cast.h>

#if USE_MYSQL
#include <Dictionaries/Embedded/TechDataHierarchy.h>
#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int DICTIONARIES_WAS_NOT_LOADED;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

/** Functions using Yandex.Metrica dictionaries
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


#if USE_MYSQL

struct OSToRootImpl
{
    static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToMostAncestor(x); }
};

struct SEToRootImpl
{
    static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToMostAncestor(x); }
};

struct OSInImpl
{
    static bool apply(UInt32 x, UInt32 y, const TechDataHierarchy & hierarchy) { return hierarchy.isOSIn(x, y); }
};

struct SEInImpl
{
    static bool apply(UInt32 x, UInt32 y, const TechDataHierarchy & hierarchy) { return hierarchy.isSEIn(x, y); }
};

struct OSHierarchyImpl
{
    static UInt8 toParent(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToParent(x); }
};

struct SEHierarchyImpl
{
    static UInt8 toParent(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToParent(x); }
};

#endif


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
            throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<T>::get())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
                + " (must be " + String(TypeName<T>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>::get())
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second ('point of view') argument of function " + getName()
                + " (must be " + String(TypeName<T>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return arguments[0];
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// The dictionary key that defines the "point of view".
        std::string dict_key;

        if (arguments.size() == 2)
        {
            const ColumnConst * key_col = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[1]).column.get());

            if (!key_col)
                throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                    + " of second ('point of view') argument of function " + name
                    + ". Must be constant string.",
                    ErrorCodes::ILLEGAL_COLUMN);

            dict_key = key_col->getValue<String>();
        }

        const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnVector<T>::create();

            const typename ColumnVector<T>::Container & vec_from = col_from->getData();
            typename ColumnVector<T>::Container & vec_to = col_to->getData();
            size_t size = vec_from.size();
            vec_to.resize(size);

            for (size_t i = 0; i < size; ++i)
                vec_to[i] = Transform::apply(vec_from[i], dict);

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
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
            throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 2 && arguments.size() != 3)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 2 or 3.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<T>::get())
            throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                + " (must be " + String(TypeName<T>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments[1]->getName() != TypeName<T>::get())
            throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
                + " (must be " + String(TypeName<T>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 3 && arguments[2]->getName() != TypeName<String>::get())
            throw Exception("Illegal type " + arguments[2]->getName() + " of the third ('point of view') argument of function " + getName()
                + " (must be " + String(TypeName<String>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeUInt8>();
    }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// The dictionary key that defines the "point of view".
        std::string dict_key;

        if (arguments.size() == 3)
        {
            const ColumnConst * key_col = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[2]).column.get());

            if (!key_col)
                throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
                + " of third ('point of view') argument of function " + name
                + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

            dict_key = key_col->getValue<String>();
        }

        const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

        const ColumnVector<T> * col_vec1 = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get());
        const ColumnVector<T> * col_vec2 = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[1]).column.get());
        const ColumnConst * col_const1 = checkAndGetColumnConst<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get());
        const ColumnConst * col_const2 = checkAndGetColumnConst<ColumnVector<T>>(block.getByPosition(arguments[1]).column.get());

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

            block.getByPosition(result).column = std::move(col_to);
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

            block.getByPosition(result).column = std::move(col_to);
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

            block.getByPosition(result).column = std::move(col_to);
        }
        else if (col_const1 && col_const2)
        {
            block.getByPosition(result).column = DataTypeUInt8().createColumnConst(col_const1->size(),
                toField(Transform::apply(col_const1->template getValue<T>(), col_const2->template getValue<T>(), dict)));
        }
        else
            throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
                    + " and " + block.getByPosition(arguments[1]).column->getName()
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
            throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<T>::get())
            throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
            + " (must be " + String(TypeName<T>::get()) + ")",
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>::get())
            throw Exception("Illegal type " + arguments[1]->getName() + " of the second ('point of view') argument of function " + getName()
                + " (must be " + String(TypeName<String>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeArray>(arguments[0]);
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        /// The dictionary key that defines the "point of view".
        std::string dict_key;

        if (arguments.size() == 2)
        {
            const ColumnConst * key_col = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[1]).column.get());

            if (!key_col)
                throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                + " of second ('point of view') argument of function " + name
                + ". Must be constant string.",
                ErrorCodes::ILLEGAL_COLUMN);

            dict_key = key_col->getValue<String>();
        }

        const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

        if (const ColumnVector<T> * col_from = checkAndGetColumn<ColumnVector<T>>(block.getByPosition(arguments[0]).column.get()))
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
                while (cur)
                {
                    res_values.push_back(cur);
                    cur = Transform::toParent(cur, dict);
                }
                res_offsets[i] = res_values.size();
            }

            block.getByPosition(result).column = ColumnArray::create(std::move(col_values), std::move(col_offsets));
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
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


#if USE_MYSQL

struct NameOSToRoot                    { static constexpr auto name = "OSToRoot"; };
struct NameSEToRoot                    { static constexpr auto name = "SEToRoot"; };
struct NameOSIn                        { static constexpr auto name = "OSIn"; };
struct NameSEIn                        { static constexpr auto name = "SEIn"; };
struct NameOSHierarchy                 { static constexpr auto name = "OSHierarchy"; };
struct NameSEHierarchy                 { static constexpr auto name = "SEHierarchy"; };

#endif


struct FunctionRegionToCity :
    public FunctionTransformWithDictionary<UInt32, RegionToCityImpl,    RegionsHierarchyGetter,    NameRegionToCity>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToArea :
    public FunctionTransformWithDictionary<UInt32, RegionToAreaImpl,    RegionsHierarchyGetter,    NameRegionToArea>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToDistrict :
    public FunctionTransformWithDictionary<UInt32, RegionToDistrictImpl, RegionsHierarchyGetter, NameRegionToDistrict>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToCountry :
    public FunctionTransformWithDictionary<UInt32, RegionToCountryImpl, RegionsHierarchyGetter, NameRegionToCountry>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToContinent :
    public FunctionTransformWithDictionary<UInt32, RegionToContinentImpl, RegionsHierarchyGetter, NameRegionToContinent>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToTopContinent :
    public FunctionTransformWithDictionary<UInt32, RegionToTopContinentImpl, RegionsHierarchyGetter, NameRegionToTopContinent>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionToPopulation :
    public FunctionTransformWithDictionary<UInt32, RegionToPopulationImpl, RegionsHierarchyGetter, NameRegionToPopulation>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionIn :
    public FunctionIsInWithDictionary<UInt32, RegionInImpl, RegionsHierarchyGetter,    NameRegionIn>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};

struct FunctionRegionHierarchy :
    public FunctionHierarchyWithDictionary<UInt32, RegionHierarchyImpl, RegionsHierarchyGetter, NameRegionHierarchy>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
    }
};


#if USE_MYSQL

struct FunctionOSToRoot :
    public FunctionTransformWithDictionary<UInt8, OSToRootImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameOSToRoot>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
    }
};

struct FunctionSEToRoot :
    public FunctionTransformWithDictionary<UInt8, SEToRootImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameSEToRoot>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
    }
};

struct FunctionOSIn :
    public FunctionIsInWithDictionary<UInt8,    OSInImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameOSIn>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
    }
};

struct FunctionSEIn :
    public FunctionIsInWithDictionary<UInt8,    SEInImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameSEIn>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
    }
};

struct FunctionOSHierarchy :
    public FunctionHierarchyWithDictionary<UInt8, OSHierarchyImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameOSHierarchy>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
    }
};

struct FunctionSEHierarchy :
    public FunctionHierarchyWithDictionary<UInt8, SEHierarchyImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameSEHierarchy>
{
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
    }
};

#endif


/// Converts a region's numeric identifier to a name in the specified language using a dictionary.
class FunctionRegionToName : public IFunction
{
public:
    static constexpr auto name = "regionToName";
    static FunctionPtr create(const Context & context)
    {
        return std::make_shared<FunctionRegionToName>(context.getEmbeddedDictionaries().getRegionsNames());
    }

private:
    const MultiVersion<RegionsNames>::Version owned_dict;

public:
    FunctionRegionToName(const MultiVersion<RegionsNames>::Version & owned_dict_)
        : owned_dict(owned_dict_)
    {
        if (!owned_dict)
            throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
    }

    String getName() const override
    {
        return name;
    }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    /// For the purpose of query optimization, we assume this function to be injective
    ///  even in face of fact that there are many different cities named Moscow.
    bool isInjective(const Block &) override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != 1 && arguments.size() != 2)
            throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
                + toString(arguments.size()) + ", should be 1 or 2.",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

        if (arguments[0]->getName() != TypeName<UInt32>::get())
            throw Exception("Illegal type " + arguments[0]->getName() + " of the first argument of function " + getName()
                + " (must be " + String(TypeName<UInt32>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>::get())
            throw Exception("Illegal type " + arguments[0]->getName() + " of the second argument of function " + getName()
                + " (must be " + String(TypeName<String>::get()) + ")",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

        return std::make_shared<DataTypeString>();
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    bool isDeterministic() override { return false; }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
    {
        RegionsNames::Language language = RegionsNames::Language::RU;

        /// If the result language is specified
        if (arguments.size() == 2)
        {
            if (const ColumnConst * col_language = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[1]).column.get()))
                language = RegionsNames::getLanguageEnum(col_language->getValue<String>());
            else
                throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
                        + " of the second argument of function " + getName(),
                    ErrorCodes::ILLEGAL_COLUMN);
        }

        const RegionsNames & dict = *owned_dict;

        if (const ColumnUInt32 * col_from = typeid_cast<const ColumnUInt32 *>(block.getByPosition(arguments[0]).column.get()))
        {
            auto col_to = ColumnString::create();

            const ColumnUInt32::Container & region_ids = col_from->getData();

            for (size_t i = 0; i < region_ids.size(); ++i)
            {
                const StringRef & name_ref = dict.getRegionName(region_ids[i], language);
                col_to->insertDataWithTerminatingZero(name_ref.data, name_ref.size + 1);
            }

            block.getByPosition(result).column = std::move(col_to);
        }
        else
            throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
                    + " of the first argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN);
    }
};

};
