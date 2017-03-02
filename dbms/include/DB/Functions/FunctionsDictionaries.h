#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeDate.h>
#include <DB/DataTypes/DataTypeDateTime.h>
#include <DB/DataTypes/DataTypeTuple.h>

#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnTuple.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/EmbeddedDictionaries.h>
#include <DB/Interpreters/ExternalDictionaries.h>

#include <DB/Functions/IFunction.h>
#include <DB/Dictionaries/Embedded/RegionsHierarchy.h>
#include <DB/Dictionaries/Embedded/RegionsHierarchies.h>
#include <DB/Dictionaries/Embedded/RegionsNames.h>
#include <DB/Dictionaries/Embedded/TechDataHierarchy.h>
#include <DB/Dictionaries/FlatDictionary.h>
#include <DB/Dictionaries/HashedDictionary.h>
#include <DB/Dictionaries/CacheDictionary.h>
#include <DB/Dictionaries/ComplexKeyHashedDictionary.h>
#include <DB/Dictionaries/ComplexKeyCacheDictionary.h>
#include <DB/Dictionaries/RangeHashedDictionary.h>

#include <ext/range.hpp>


namespace DB
{

namespace ErrorCodes
{
	extern const int DICTIONARIES_WAS_NOT_LOADED;
	extern const int UNSUPPORTED_METHOD;
	extern const int UNKNOWN_TYPE;
}

/** Функции, использующие словари Яндекс.Метрики
  * - словари регионов, операционных систем, поисковых систем.
  *
  * Подняться по дереву до определенного уровня.
  *  regionToCity, regionToArea, regionToCountry, ...
  *  OSToRoot,
  *  SEToRoot,
  *
  * Преобразовать значения в столбце
  *  regionToName
  *
  * Является ли первый идентификатор потомком второго.
  *  regionIn, SEIn, OSIn.
  *
  * Получить массив идентификаторов регионов, состоящий из исходного и цепочки родителей. Порядок implementation defined.
  *  regionHierarchy, OSHierarchy, SEHierarchy.
  *
  * Функции, использующие подключаемые (внешние) словари.
  *
  * Получить значение аттрибута заданного типа.
  *	 dictGetType(dictionary, attribute, id),
  *	 	Type - placeholder для имени типа, в данный момент поддерживаются любые числовые и строковой типы.
  *		Тип должен соответствовать реальному типу аттрибута, с которым он был объявлен в структуре словаря.
  *
  * Получить массив идентификаторов, состоящий из исходного и цепочки родителей.
  *  dictGetHierarchy(dictionary, id).
  *
  * Является ли первы йидентификатор потомком второго.
  *  dictIsIn(dictionary, child_id, parent_id).
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

struct OSToRootImpl
{
	static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToMostAncestor(x); }
};

struct SEToRootImpl
{
	static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToMostAncestor(x); }
};

struct RegionInImpl
{
	static bool apply(UInt32 x, UInt32 y, const RegionsHierarchy & hierarchy) { return hierarchy.in(x, y); }
};

struct OSInImpl
{
	static bool apply(UInt32 x, UInt32 y, const TechDataHierarchy & hierarchy) { return hierarchy.isOSIn(x, y); }
};

struct SEInImpl
{
	static bool apply(UInt32 x, UInt32 y, const TechDataHierarchy & hierarchy) { return hierarchy.isSEIn(x, y); }
};

struct RegionHierarchyImpl
{
	static UInt32 toParent(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toParent(x); }
};

struct OSHierarchyImpl
{
	static UInt8 toParent(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToParent(x); }
};

struct SEHierarchyImpl
{
	static UInt8 toParent(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToParent(x); }
};


/** Вспомогательная вещь, позволяющая достать из словаря конкретный словарь, соответствующий точке зрения
  *  (ключу словаря, передаваемому в аргументе функции).
  * Пример: при вызове regionToCountry(x, 'ua'), может быть использован словарь, в котором Крым относится к Украине.
  */
struct RegionsHierarchyGetter
{
	using Src = RegionsHierarchies;
	using Dst = RegionsHierarchy;

	static const Dst & get(const Src & src, const std::string & key)
	{
		return src.get(key);
	}
};

/** Для словарей без поддержки ключей. Ничего не делает.
  */
template <typename Dict>
struct IdentityDictionaryGetter
{
	using Src = Dict;
	using Dst = Dict;

	static const Dst & get(const Src & src, const std::string & key)
	{
		if (key.empty())
			return src;
		else
			throw Exception("Dictionary doesn't support 'point of view' keys.", ErrorCodes::BAD_ARGUMENTS);
	}
};


/// Преобразует идентификатор, используя словарь.
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

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1 && arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1 or 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments[0]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
				+ " (must be " + TypeName<T>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>::get())
			throw Exception("Illegal type " + arguments[1]->getName() + " of the second ('point of view') argument of function " + getName()
				+ " (must be " + TypeName<String>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return arguments[0];
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		/// Ключ словаря, определяющий "точку зрения".
		std::string dict_key;

		if (arguments.size() == 2)
		{
			const ColumnConstString * key_col = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[1]).column.get());

			if (!key_col)
				throw Exception("Illegal column " + block.safeGetByPosition(arguments[1]).column->getName()
					+ " of second ('point of view') argument of function " + name
					+ ". Must be constant string.",
					ErrorCodes::ILLEGAL_COLUMN);

			dict_key = key_col->getData();
		}

		const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

		if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			auto col_to = std::make_shared<ColumnVector<T>>();
			block.safeGetByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<T>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from[i], dict);
		}
		else if (const ColumnConst<T> * col_from = typeid_cast<const ColumnConst<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			block.safeGetByPosition(result).column = std::make_shared<ColumnConst<T>>(
				col_from->size(), Transform::apply(col_from->getData(), dict));
		}
		else
			throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Проверяет принадлежность, используя словарь.
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

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 2 && arguments.size() != 3)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 2 or 3.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments[0]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
				+ " (must be " + TypeName<T>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments[1]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[1]->getName() + " of second argument of function " + getName()
				+ " (must be " + TypeName<T>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments.size() == 3 && arguments[2]->getName() != TypeName<String>::get())
			throw Exception("Illegal type " + arguments[2]->getName() + " of the third ('point of view') argument of function " + getName()
				+ " (must be " + TypeName<String>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeUInt8>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		/// Ключ словаря, определяющий "точку зрения".
		std::string dict_key;

		if (arguments.size() == 3)
		{
			const ColumnConstString * key_col = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[2]).column.get());

			if (!key_col)
				throw Exception("Illegal column " + block.safeGetByPosition(arguments[2]).column->getName()
				+ " of third ('point of view') argument of function " + name
				+ ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

			dict_key = key_col->getData();
		}

		const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

		const ColumnVector<T> * col_vec1 = typeid_cast<const ColumnVector<T> *>(block.safeGetByPosition(arguments[0]).column.get());
		const ColumnVector<T> * col_vec2 = typeid_cast<const ColumnVector<T> *>(block.safeGetByPosition(arguments[1]).column.get());
		const ColumnConst<T> * col_const1 = typeid_cast<const ColumnConst<T> *>(block.safeGetByPosition(arguments[0]).column.get());
		const ColumnConst<T> * col_const2 = typeid_cast<const ColumnConst<T> *>(block.safeGetByPosition(arguments[1]).column.get());

		if (col_vec1 && col_vec2)
		{
			auto col_to = std::make_shared<ColumnUInt8>();
			block.safeGetByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from1 = col_vec1->getData();
			const typename ColumnVector<T>::Container_t & vec_from2 = col_vec2->getData();
			typename ColumnUInt8::Container_t & vec_to = col_to->getData();
			size_t size = vec_from1.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from1[i], vec_from2[i], dict);
		}
		else if (col_vec1 && col_const2)
		{
			auto col_to = std::make_shared<ColumnUInt8>();
			block.safeGetByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from1 = col_vec1->getData();
			const T const_from2 = col_const2->getData();
			typename ColumnUInt8::Container_t & vec_to = col_to->getData();
			size_t size = vec_from1.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from1[i], const_from2, dict);
		}
		else if (col_const1 && col_vec2)
		{
			auto col_to = std::make_shared<ColumnUInt8>();
			block.safeGetByPosition(result).column = col_to;

			const T const_from1 = col_const1->getData();
			const typename ColumnVector<T>::Container_t & vec_from2 = col_vec2->getData();
			typename ColumnUInt8::Container_t & vec_to = col_to->getData();
			size_t size = vec_from2.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(const_from1, vec_from2[i], dict);
		}
		else if (col_const1 && col_const2)
		{
			block.safeGetByPosition(result).column = std::make_shared<ColumnConst<UInt8>>(col_const1->size(),
				Transform::apply(col_const1->getData(), col_const2->getData(), dict));
		}
		else
			throw Exception("Illegal columns " + block.safeGetByPosition(arguments[0]).column->getName()
					+ " and " + block.safeGetByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + name,
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Получает массив идентификаторов, состоящий из исходного и цепочки родителей.
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

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1 && arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1 or 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments[0]->getName() != TypeName<T>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of argument of function " + getName()
			+ " (must be " + TypeName<T>::get() + ")",
			ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>::get())
			throw Exception("Illegal type " + arguments[1]->getName() + " of the second ('point of view') argument of function " + getName()
				+ " (must be " + TypeName<String>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeArray>(arguments[0]);
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		/// Ключ словаря, определяющий "точку зрения".
		std::string dict_key;

		if (arguments.size() == 2)
		{
			const ColumnConstString * key_col = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[1]).column.get());

			if (!key_col)
				throw Exception("Illegal column " + block.safeGetByPosition(arguments[1]).column->getName()
				+ " of second ('point of view') argument of function " + name
				+ ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

			dict_key = key_col->getData();
		}

		const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

		if (const ColumnVector<T> * col_from = typeid_cast<const ColumnVector<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			auto col_values = std::make_shared<ColumnVector<T>>();
			auto col_array = std::make_shared<ColumnArray>(col_values);
			block.safeGetByPosition(result).column = col_array;

			ColumnArray::Offsets_t & res_offsets = col_array->getOffsets();
			typename ColumnVector<T>::Container_t & res_values = col_values->getData();

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
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
		}
		else if (const ColumnConst<T> * col_from = typeid_cast<const ColumnConst<T> *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			Array res;

			T cur = col_from->getData();
			while (cur)
			{
				res.push_back(static_cast<typename NearestFieldType<T>::Type>(cur));
				cur = Transform::toParent(cur, dict);
			}

			block.safeGetByPosition(result).column = std::make_shared<ColumnConstArray>(
				col_from->size(),
				res,
				std::make_shared<DataTypeArray>(std::make_shared<typename DataTypeFromFieldType<T>::Type>()));
		}
		else
			throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
			+ " of first argument of function " + name,
							ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameRegionToCity				{ static constexpr auto name = "regionToCity"; };
struct NameRegionToArea				{ static constexpr auto name = "regionToArea"; };
struct NameRegionToDistrict			{ static constexpr auto name = "regionToDistrict"; };
struct NameRegionToCountry			{ static constexpr auto name = "regionToCountry"; };
struct NameRegionToContinent		{ static constexpr auto name = "regionToContinent"; };
struct NameRegionToTopContinent		{ static constexpr auto name = "regionToTopContinent"; };
struct NameRegionToPopulation		{ static constexpr auto name = "regionToPopulation"; };
struct NameOSToRoot					{ static constexpr auto name = "OSToRoot"; };
struct NameSEToRoot					{ static constexpr auto name = "SEToRoot"; };

struct NameRegionIn					{ static constexpr auto name = "regionIn"; };
struct NameOSIn						{ static constexpr auto name = "OSIn"; };
struct NameSEIn						{ static constexpr auto name = "SEIn"; };

struct NameRegionHierarchy			{ static constexpr auto name = "regionHierarchy"; };
struct NameOSHierarchy				{ static constexpr auto name = "OSHierarchy"; };
struct NameSEHierarchy				{ static constexpr auto name = "SEHierarchy"; };


struct FunctionRegionToCity :
	public FunctionTransformWithDictionary<UInt32, RegionToCityImpl,	RegionsHierarchyGetter,	NameRegionToCity>
{
	static FunctionPtr create(const Context & context)
	{
		return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
	}
};

struct FunctionRegionToArea :
	public FunctionTransformWithDictionary<UInt32, RegionToAreaImpl,	RegionsHierarchyGetter,	NameRegionToArea>
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

struct FunctionRegionIn :
	public FunctionIsInWithDictionary<UInt32, RegionInImpl, RegionsHierarchyGetter,	NameRegionIn>
{
	static FunctionPtr create(const Context & context)
	{
		return std::make_shared<base_type>(context.getEmbeddedDictionaries().getRegionsHierarchies());
	}
};

struct FunctionOSIn :
	public FunctionIsInWithDictionary<UInt8,	OSInImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameOSIn>
{
	static FunctionPtr create(const Context & context)
	{
		return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
	}
};

struct FunctionSEIn :
	public FunctionIsInWithDictionary<UInt8,	SEInImpl, IdentityDictionaryGetter<TechDataHierarchy>, NameSEIn>
{
	static FunctionPtr create(const Context & context)
	{
		return std::make_shared<base_type>(context.getEmbeddedDictionaries().getTechDataHierarchy());
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


/// Преобразует числовой идентификатор региона в имя на заданном языке, используя словарь.
class FunctionRegionToName : public IFunction
{
public:
	static constexpr auto name = "regionToName";
	static FunctionPtr create(const Context & context)
	{
		return std::make_shared<FunctionRegionToName>(context.getEmbeddedDictionaries().getRegionsNames());
	}

private:
	const std::shared_ptr<RegionsNames> owned_dict;

public:
	FunctionRegionToName(const std::shared_ptr<RegionsNames> & owned_dict_)
		: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}

	/// Получить имя функции.
	String getName() const override
	{
		return name;
	}

	bool isVariadic() const override { return true; }
	size_t getNumberOfArguments() const override { return 0; }

	/// For the purpose of query optimization, we assume this function to be injective
	///  even in face of fact that there are many different cities named Moscow.
	bool isInjective(const Block &) override { return true; }

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
	{
		if (arguments.size() != 1 && arguments.size() != 2)
			throw Exception("Number of arguments for function " + getName() + " doesn't match: passed "
				+ toString(arguments.size()) + ", should be 1 or 2.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		if (arguments[0]->getName() != TypeName<UInt32>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of the first argument of function " + getName()
				+ " (must be " + TypeName<UInt32>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		if (arguments.size() == 2 && arguments[1]->getName() != TypeName<String>::get())
			throw Exception("Illegal type " + arguments[0]->getName() + " of the second argument of function " + getName()
				+ " (must be " + TypeName<String>::get() + ")",
				ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

		return std::make_shared<DataTypeString>();
	}

	/// Выполнить функцию над блоком.
	void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) override
	{
		RegionsNames::Language language = RegionsNames::Language::RU;

		/// Если указан язык результата
		if (arguments.size() == 2)
		{
			if (const ColumnConstString * col_language = typeid_cast<const ColumnConstString *>(block.safeGetByPosition(arguments[1]).column.get()))
				language = RegionsNames::getLanguageEnum(col_language->getData());
			else
				throw Exception("Illegal column " + block.safeGetByPosition(arguments[1]).column->getName()
						+ " of the second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		const RegionsNames & dict = *owned_dict;

		if (const ColumnUInt32 * col_from = typeid_cast<const ColumnUInt32 *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			auto col_to = std::make_shared<ColumnString>();
			block.safeGetByPosition(result).column = col_to;

			const ColumnUInt32::Container_t & region_ids = col_from->getData();

			for (size_t i = 0; i < region_ids.size(); ++i)
			{
				const StringRef & name_ref = dict.getRegionName(region_ids[i], language);
				col_to->insertDataWithTerminatingZero(name_ref.data, name_ref.size + 1);
			}
		}
		else if (const ColumnConst<UInt32> * col_from = typeid_cast<const ColumnConst<UInt32> *>(block.safeGetByPosition(arguments[0]).column.get()))
		{
			UInt32 region_id = col_from->getData();
			const StringRef & name_ref = dict.getRegionName(region_id, language);

			block.safeGetByPosition(result).column = std::make_shared<ColumnConstString>(col_from->size(), name_ref.toString());
		}
		else
			throw Exception("Illegal column " + block.safeGetByPosition(arguments[0]).column->getName()
					+ " of the first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


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
			!executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr))
			throw Exception{
				"Unsupported dictionary type " + dict_ptr->getTypeName(),
				ErrorCodes::UNKNOWN_TYPE};
	}

	template <typename DictionaryType>
	bool executeDispatchSimple(
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
	{
		const auto dict = typeid_cast<const DictionaryType *>(dictionary);
		if (!dict)
			return false;

		const auto key_col_with_type = block.safeGetByPosition(arguments[1]);
		if (typeid_cast<const ColumnTuple *>(key_col_with_type.column.get())
			|| typeid_cast<const ColumnConstTuple *>(key_col_with_type.column.get()))
		{
			/// Функции у внешних словарей поддерживают только полноценные (не константные) столбцы с ключами.
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
			!executeDispatchRange<RangeHashedDictionary>(block, arguments, result, dict_ptr))
			throw Exception{
				"Unsupported dictionary type " + dict_ptr->getTypeName(),
				ErrorCodes::UNKNOWN_TYPE};
	}

	template <typename DictionaryType>
	bool executeDispatch(
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const size_t result, const DictionaryType * const dictionary, const std::string & attr_name,
		const ColumnUInt64 * const id_col, const IColumn * const date_col_untyped)
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
		Block & block, const size_t result, const DictionaryType * const dictionary, const std::string & attr_name,
		const ColumnConst<UInt64> * const id_col, const IColumn * const date_col_untyped)
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
			!executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr))
			throw Exception{
				"Unsupported dictionary type " + dict_ptr->getTypeName(),
				ErrorCodes::UNKNOWN_TYPE};
	}

	template <typename DictionaryType>
	bool executeDispatch(
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * const dictionary,
		const std::string & attr_name, const ColumnUInt64 * const id_col)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * const dictionary,
		const std::string & attr_name, const ColumnConst<UInt64> * const id_col)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		const DictionaryType * const dict, const std::string & name, const PaddedPODArray<UInt64> & ids,\
		PaddedPODArray<TYPE> & out)\
	{\
		dict->get##TYPE(name, ids, out);\
	}\
	template <typename DictionaryType>\
	static void get(\
		const DictionaryType * const dict, const std::string & name, const ConstColumnPlainPtrs & key_columns,\
		const DataTypes & key_types, PaddedPODArray<TYPE> & out)\
	{\
		dict->get##TYPE(name, key_columns, key_types, out);\
	}\
	template <typename DictionaryType>\
	static void get(\
		const DictionaryType * const dict, const std::string & name, const PaddedPODArray<UInt64> & ids,\
		const PaddedPODArray<UInt16> & dates, PaddedPODArray<TYPE> & out)\
	{\
		dict->get##TYPE(name, ids, dates, out);\
	}\
	template <typename DictionaryType, typename DefaultsType>\
	static void getOrDefault(\
		const DictionaryType * const dict, const std::string & name, const PaddedPODArray<UInt64> & ids,\
		const DefaultsType & def, PaddedPODArray<TYPE> & out)\
	{\
		dict->get##TYPE(name, ids, def, out);\
	}\
	template <typename DictionaryType, typename DefaultsType>\
	static void getOrDefault(\
		const DictionaryType * const dict, const std::string & name, const ConstColumnPlainPtrs & key_columns,\
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
			!executeDispatchRange<RangeHashedDictionary>(block, arguments, result, dict_ptr))
			throw Exception{
				"Unsupported dictionary type " + dict_ptr->getTypeName(),
				ErrorCodes::UNKNOWN_TYPE};
	}

	template <typename DictionaryType>
	bool executeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
		const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		Block & block, const size_t result, const DictionaryType * const dictionary, const std::string & attr_name,
		const ColumnUInt64 * const id_col, const IColumn * const date_col_untyped)
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
		Block & block, const size_t result, const DictionaryType * const dictionary, const std::string & attr_name,
		const ColumnConst<UInt64> * const id_col, const IColumn * const date_col_untyped)
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
			!executeDispatchComplex<ComplexKeyCacheDictionary>(block, arguments, result, dict_ptr))
			throw Exception{
				"Unsupported dictionary type " + dict_ptr->getTypeName(),
				ErrorCodes::UNKNOWN_TYPE};
	}

	template <typename DictionaryType>
	bool executeDispatch(Block & block, const ColumnNumbers & arguments, const size_t result,
		const IDictionaryBase * const dictionary)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * const dictionary,
		const std::string & attr_name, const ColumnUInt64 * const id_col)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const DictionaryType * const dictionary,
		const std::string & attr_name, const ColumnConst<UInt64> * const id_col)
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
		Block & block, const ColumnNumbers & arguments, const size_t result, const IDictionaryBase * const dictionary)
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
		const IDictionaryBase * const dictionary)
	{
		const auto dict = typeid_cast<const DictionaryType *>(dictionary);
		if (!dict)
			return false;

		if (!dict->hasHierarchy())
			throw Exception{
				"Dictionary does not have a hierarchy",
				ErrorCodes::UNSUPPORTED_METHOD};

		const auto get_hierarchies = [&] (const PaddedPODArray<UInt64> & in, PaddedPODArray<UInt64> & out, PaddedPODArray<UInt64> & offsets) {
			const auto size = in.size();

			/// copy of `in` array
			auto in_array = std::make_unique<PaddedPODArray<UInt64>>(std::begin(in), std::end(in));
			/// used for storing and handling result of ::toParent call
			auto out_array = std::make_unique<PaddedPODArray<UInt64>>(size);
			/// resulting hierarchies
			std::vector<std::vector<IDictionary::Key>> hierarchies(size);

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
		const IDictionaryBase * const dictionary)
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
	bool execute(Block & block, const size_t result, const DictionaryType * const dictionary,
		const ColumnUInt64 * const child_id_col, const IColumn * const ancestor_id_col_untyped)
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

			for (const auto idx : ext::range(0, size))
				data[idx] = dictionary->in(child_ids[idx], ancestor_ids[idx]);
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

			for (const auto idx : ext::range(0, size))
				data[idx] = dictionary->in(child_ids[idx], ancestor_id);
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
	bool execute(Block & block, const size_t result, const DictionaryType * const dictionary,
		const ColumnConst<UInt64> * const child_id_col, const IColumn * const ancestor_id_col_untyped)
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

			for (const auto idx : ext::range(0, size))
				data[idx] = dictionary->in(child_id, ancestor_ids[idx]);
		}
		else if (const auto ancestor_id_col = typeid_cast<const ColumnConst<UInt64> *>(ancestor_id_col_untyped))
		{
			block.safeGetByPosition(result).column = std::make_shared<ColumnConst<UInt8>>(
				child_id_col->size(),
				dictionary->in(child_id_col->getData(), ancestor_id_col->getData()));
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
