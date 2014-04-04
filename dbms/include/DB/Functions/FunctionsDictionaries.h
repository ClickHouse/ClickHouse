#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeString.h>

#include <DB/Columns/ColumnVector.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnString.h>

#include <DB/Interpreters/Context.h>

#include <DB/Functions/IFunction.h>
#include <statdaemons/CategoriesHierarchy.h>


namespace DB
{

/** Функции, использующие словари Яндекс.Метрики
  * - словари регионов, операционных систем, поисковых систем.
  *
  * Подняться по дереву до определенного уровня.
  *  regionToCity, regionToArea, regionToCountry,
  *  OSToRoot,
  *  SEToRoot,
  *  categoryToRoot,
  *  categoryToSecondLevel
  * 
  * Преобразовать значения в столбце
  *  regionToName
  *
  * Является ли первый идентификатор потомком второго.
  *  regionIn, SEIn, OSIn, categoryIn.
  * 
  * Получить массив идентификаторов регионов, состоящий из исходного и цепочки родителей. Порядок implementation defined.
  *  regionHierarchy, OSHierarchy, SEHierarchy, categoryHierarchy.
  */


struct RegionToCityImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toCity(x); }
};

struct RegionToAreaImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toArea(x); }
};

struct RegionToCountryImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toCountry(x); }
};

struct RegionToContinentImpl
{
	static UInt32 apply(UInt32 x, const RegionsHierarchy & hierarchy) { return hierarchy.toContinent(x); }
};

struct OSToRootImpl
{
	static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.OSToMostAncestor(x); }
};

struct SEToRootImpl
{
	static UInt8 apply(UInt8 x, const TechDataHierarchy & hierarchy) { return hierarchy.SEToMostAncestor(x); }
};

struct CategoryToRootImpl
{
	static UInt16 apply(UInt16 x, const CategoriesHierarchy & hierarchy) { return hierarchy.toMostAncestor(x); }
};

struct CategoryToSecondLevelImpl
{
	static UInt16 apply(UInt16 x, const CategoriesHierarchy & hierarchy) { return hierarchy.toSecondLevel(x); }
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

struct CategoryInImpl
{
	static bool apply(UInt16 x, UInt16 y, const CategoriesHierarchy & hierarchy) { return hierarchy.in(x, y); }
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

struct CategoryHierarchyImpl
{
	static UInt16 toParent(UInt16 x, const CategoriesHierarchy & hierarchy) { return hierarchy.toParent(x); }
};


/** Вспомогательная вещь, позволяющая достать из словаря конкретный словарь, соответствующий точке зрения
  *  (ключу словаря, передаваемому в аргументе функции).
  * Пример: при вызове regionToCountry(x, 'ua'), может быть использован словарь, в котором Крым относится к Украине.
  */
struct RegionsHierarchyGetter
{
	typedef RegionsHierarchies Src;
	typedef RegionsHierarchy Dst;

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
	typedef Dict Src;
	typedef Dict Dst;

	static const Dst & get(const Src & src, const std::string & key)
	{
		if (key.empty())
			return src;
		else
			throw Exception("Dictionary doesn't support 'point of view' keys.", ErrorCodes::BAD_ARGUMENTS);
	}
};


/// Преобразует идентификатор, используя словарь.
template <typename T, typename Transform, typename Dict, typename DictGetter, typename Name>
class FunctionTransformWithDictionary : public IFunction
{
private:
	const SharedPtr<Dict> owned_dict;
	
public:
	FunctionTransformWithDictionary(const SharedPtr<Dict> & owned_dict_)
		: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}
	
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
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
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Ключ словаря, определяющий "точку зрения".
		std::string dict_key;

		if (arguments.size() == 2)
		{
			const ColumnConstString * key_col = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[1]).column);

			if (!key_col)
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
					+ " of second ('point of view') argument of function " + Name::get()
					+ ". Must be constant string.",
					ErrorCodes::ILLEGAL_COLUMN);

			dict_key = key_col->getData();
		}

		const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

		if (const ColumnVector<T> * col_from = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<T> * col_to = new ColumnVector<T>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from = col_from->getData();
			typename ColumnVector<T>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from[i], dict);
		}
		else if (const ColumnConst<T> * col_from = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			block.getByPosition(result).column = new ColumnConst<T>(col_from->size(), Transform::apply(col_from->getData(), dict));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of first argument of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Проверяет принадлежность, используя словарь.
template <typename T, typename Transform, typename Dict, typename DictGetter, typename Name>
class FunctionIsInWithDictionary : public IFunction
{
private:
	const SharedPtr<Dict> owned_dict;

public:
	FunctionIsInWithDictionary(const SharedPtr<Dict> & owned_dict_)
		: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}

	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
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

		return new DataTypeUInt8;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Ключ словаря, определяющий "точку зрения".
		std::string dict_key;

		if (arguments.size() == 3)
		{
			const ColumnConstString * key_col = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[2]).column);

			if (!key_col)
				throw Exception("Illegal column " + block.getByPosition(arguments[2]).column->getName()
				+ " of third ('point of view') argument of function " + Name::get()
				+ ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

			dict_key = key_col->getData();
		}

		const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

		const ColumnVector<T> * col_vec1 = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnVector<T> * col_vec2 = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[1]).column);
		const ColumnConst<T> * col_const1 = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column);
		const ColumnConst<T> * col_const2 = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[1]).column);
		
		if (col_vec1 && col_vec2)
		{
			ColumnVector<UInt8> * col_to = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from1 = col_vec1->getData();
			const typename ColumnVector<T>::Container_t & vec_from2 = col_vec2->getData();
			typename ColumnVector<UInt8>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from1.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from1[i], vec_from2[i], dict);
		}
		else if (col_vec1 && col_const2)
		{
			ColumnVector<UInt8> * col_to = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_to;

			const typename ColumnVector<T>::Container_t & vec_from1 = col_vec1->getData();
			const T const_from2 = col_const2->getData();
			typename ColumnVector<UInt8>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from1.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(vec_from1[i], const_from2, dict);
		}
		else if (col_const1 && col_vec2)
		{
			ColumnVector<UInt8> * col_to = new ColumnVector<UInt8>;
			block.getByPosition(result).column = col_to;

			const T const_from1 = col_const1->getData();
			const typename ColumnVector<T>::Container_t & vec_from2 = col_vec2->getData();
			typename ColumnVector<UInt8>::Container_t & vec_to = col_to->getData();
			size_t size = vec_from2.size();
			vec_to.resize(size);

			for (size_t i = 0; i < size; ++i)
				vec_to[i] = Transform::apply(const_from1, vec_from2[i], dict);
		}
		else if (col_const1 && col_const2)
		{
			block.getByPosition(result).column = new ColumnConst<UInt8>(col_const1->size(),
				Transform::apply(col_const1->getData(), col_const2->getData(), dict));
		}
		else
			throw Exception("Illegal columns " + block.getByPosition(arguments[0]).column->getName()
					+ " and " + block.getByPosition(arguments[1]).column->getName()
					+ " of arguments of function " + Name::get(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};


/// Получает массив идентификаторов, состоящий из исходного и цепочки родителей.
template <typename T, typename Transform, typename Dict, typename DictGetter, typename Name>
class FunctionHierarchyWithDictionary : public IFunction
{
private:
	const SharedPtr<Dict> owned_dict;
	
public:
	FunctionHierarchyWithDictionary(const SharedPtr<Dict> & owned_dict_)
	: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}
	
	/// Получить имя функции.
	String getName() const
	{
		return Name::get();
	}
	
	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
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
		
		return new DataTypeArray(arguments[0]);
	}
	
	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		/// Ключ словаря, определяющий "точку зрения".
		std::string dict_key;

		if (arguments.size() == 2)
		{
			const ColumnConstString * key_col = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[1]).column);

			if (!key_col)
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
				+ " of second ('point of view') argument of function " + Name::get()
				+ ". Must be constant string.",
				ErrorCodes::ILLEGAL_COLUMN);

			dict_key = key_col->getData();
		}

		const typename DictGetter::Dst & dict = DictGetter::get(*owned_dict, dict_key);

		if (const ColumnVector<T> * col_from = dynamic_cast<const ColumnVector<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnVector<T> * col_values = new ColumnVector<T>;
			ColumnArray * col_array = new ColumnArray(col_values);
			block.getByPosition(result).column = col_array;
			
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
		else if (const ColumnConst<T> * col_from = dynamic_cast<const ColumnConst<T> *>(&*block.getByPosition(arguments[0]).column))
		{
			Array res;
			
			T cur = col_from->getData();
			while (cur)
			{
				res.push_back(static_cast<typename NearestFieldType<T>::Type>(cur));
				cur = Transform::toParent(cur, dict);
			}
			
			block.getByPosition(result).column = new ColumnConstArray(col_from->size(), res, new DataTypeArray(new typename DataTypeFromFieldType<T>::Type));
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
			+ " of first argument of function " + Name::get(),
							ErrorCodes::ILLEGAL_COLUMN);
	}
};


struct NameRegionToCity 	{ static const char * get() { return "regionToCity"; } };
struct NameRegionToArea 	{ static const char * get() { return "regionToArea"; } };
struct NameRegionToCountry 	{ static const char * get() { return "regionToCountry"; } };
struct NameRegionToContinent{ static const char * get() { return "regionToContient"; } };
struct NameOSToRoot 		{ static const char * get() { return "OSToRoot"; } };
struct NameSEToRoot 		{ static const char * get() { return "SEToRoot"; } };
struct NameCategoryToRoot 	{ static const char * get() { return "categoryToRoot"; } };
struct NameCategoryToSecondLevel { static const char * get() { return "categoryToSecondLevel"; } };

struct NameRegionIn 		{ static const char * get() { return "regionIn"; } };
struct NameOSIn 			{ static const char * get() { return "OSIn"; } };
struct NameSEIn 			{ static const char * get() { return "SEIn"; } };
struct NameCategoryIn 		{ static const char * get() { return "categoryIn"; } };

struct NameRegionHierarchy	{ static const char * get() { return "regionHierarchy"; } };
struct NameOSHierarchy		{ static const char * get() { return "OSHierarchy"; } };
struct NameSEHierarchy		{ static const char * get() { return "SEHierarchy"; } };
struct NameCategoryHierarchy{ static const char * get() { return "categoryHierarchy"; } };


typedef FunctionTransformWithDictionary
	<UInt32, RegionToCityImpl,	RegionsHierarchies, RegionsHierarchyGetter,	NameRegionToCity> FunctionRegionToCity;

typedef FunctionTransformWithDictionary
	<UInt32, RegionToAreaImpl,	RegionsHierarchies, RegionsHierarchyGetter,	NameRegionToArea> FunctionRegionToArea;

typedef FunctionTransformWithDictionary
	<UInt32, RegionToCountryImpl,RegionsHierarchies, RegionsHierarchyGetter,	NameRegionToCountry> FunctionRegionToCountry;

typedef FunctionTransformWithDictionary
	<UInt32, RegionToContinentImpl, RegionsHierarchies, RegionsHierarchyGetter, NameRegionToContinent> FunctionRegionToContinent;

typedef FunctionTransformWithDictionary
	<UInt8, OSToRootImpl,		TechDataHierarchy, IdentityDictionaryGetter<TechDataHierarchy>,	NameOSToRoot> FunctionOSToRoot;

typedef FunctionTransformWithDictionary
	<UInt8, SEToRootImpl,		TechDataHierarchy, IdentityDictionaryGetter<TechDataHierarchy>,	NameSEToRoot> FunctionSEToRoot;

typedef FunctionTransformWithDictionary
	<UInt16, CategoryToRootImpl,	CategoriesHierarchy, IdentityDictionaryGetter<CategoriesHierarchy>, NameCategoryToRoot>	FunctionCategoryToRoot;

typedef FunctionTransformWithDictionary
	<UInt16, CategoryToSecondLevelImpl, CategoriesHierarchy, IdentityDictionaryGetter<CategoriesHierarchy>, NameCategoryToSecondLevel>
	FunctionCategoryToSecondLevel;

typedef FunctionIsInWithDictionary
	<UInt32, RegionInImpl, RegionsHierarchies, RegionsHierarchyGetter,	NameRegionIn> FunctionRegionIn;

typedef FunctionIsInWithDictionary
	<UInt8,	OSInImpl, TechDataHierarchy, IdentityDictionaryGetter<TechDataHierarchy>, NameOSIn> FunctionOSIn;

typedef FunctionIsInWithDictionary
	<UInt8,	SEInImpl, TechDataHierarchy, IdentityDictionaryGetter<TechDataHierarchy>, NameSEIn> FunctionSEIn;

typedef FunctionIsInWithDictionary
	<UInt16, CategoryInImpl, CategoriesHierarchy, IdentityDictionaryGetter<CategoriesHierarchy>, NameCategoryIn>	FunctionCategoryIn;

typedef FunctionHierarchyWithDictionary
	<UInt32, RegionHierarchyImpl, RegionsHierarchies, RegionsHierarchyGetter, NameRegionHierarchy> FunctionRegionHierarchy;

typedef FunctionHierarchyWithDictionary
	<UInt8, OSHierarchyImpl, TechDataHierarchy, IdentityDictionaryGetter<TechDataHierarchy>, NameOSHierarchy> FunctionOSHierarchy;

typedef FunctionHierarchyWithDictionary
	<UInt8, SEHierarchyImpl, TechDataHierarchy, IdentityDictionaryGetter<TechDataHierarchy>, NameSEHierarchy> FunctionSEHierarchy;

typedef FunctionHierarchyWithDictionary
	<UInt16, CategoryHierarchyImpl,	CategoriesHierarchy, IdentityDictionaryGetter<CategoriesHierarchy>, NameCategoryHierarchy> FunctionCategoryHierarchy;


/// Преобразует числовой идентификатор региона в имя на заданном языке, используя словарь.
class FunctionRegionToName : public IFunction
{
private:
	const SharedPtr<RegionsNames> owned_dict;
	
public:
	FunctionRegionToName(const SharedPtr<RegionsNames> & owned_dict_)
		: owned_dict(owned_dict_)
	{
		if (!owned_dict)
			throw Exception("Dictionaries was not loaded. You need to check configuration file.", ErrorCodes::DICTIONARIES_WAS_NOT_LOADED);
	}
	
	/// Получить имя функции.
	String getName() const
	{
		return "regionToName";
	}

	/// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
	DataTypePtr getReturnType(const DataTypes & arguments) const
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

		return new DataTypeString;
	}

	/// Выполнить функцию над блоком.
	void execute(Block & block, const ColumnNumbers & arguments, size_t result)
	{
		RegionsNames::SupportedLanguages::Enum language = RegionsNames::SupportedLanguages::RU;
		
		/// Если указан язык результата
		if (arguments.size() == 2)
		{
			if (const ColumnConstString * col_language = dynamic_cast<const ColumnConstString *>(&*block.getByPosition(arguments[1]).column))
				language = RegionsNames::getLanguageEnum(col_language->getData());
			else
				throw Exception("Illegal column " + block.getByPosition(arguments[1]).column->getName()
						+ " of the second argument of function " + getName(),
					ErrorCodes::ILLEGAL_COLUMN);
		}

		const RegionsNames & dict = *owned_dict;

		if (const ColumnVector<UInt32> * col_from = dynamic_cast<const ColumnVector<UInt32> *>(&*block.getByPosition(arguments[0]).column))
		{
			ColumnString * col_to = new ColumnString;
			block.getByPosition(result).column = col_to;

			const ColumnVector<UInt32>::Container_t & region_ids = col_from->getData();

			for (size_t i = 0; i < region_ids.size(); ++i)
			{
				const DB::StringRef & name_ref = dict.getRegionName(region_ids[i], language);
				col_to->insertDataWithTerminatingZero(name_ref.data, name_ref.size + 1);
			}
		}
		else if (const ColumnConst<UInt32> * col_from = dynamic_cast<const ColumnConst<UInt32> *>(&*block.getByPosition(arguments[0]).column))
		{
			UInt32 region_id = col_from->getData();
			const DB::StringRef & name_ref = dict.getRegionName(region_id, language);
			
			block.getByPosition(result).column = new ColumnConstString(col_from->size(), name_ref.toString());
		}
		else
			throw Exception("Illegal column " + block.getByPosition(arguments[0]).column->getName()
					+ " of the first argument of function " + getName(),
				ErrorCodes::ILLEGAL_COLUMN);
	}
};

}
