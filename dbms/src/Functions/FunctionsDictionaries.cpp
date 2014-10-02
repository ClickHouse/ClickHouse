#include <DB/Functions/FunctionFactory.h>
#include <DB/Functions/FunctionsDictionaries.h>

namespace DB
{

void registerFunctionsDictionaries(FunctionFactory & factory)
{
	#define F [](const Context & context) -> IFunction*

	factory.registerFunction("regionToCity",
		F { return new FunctionRegionToCity(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("regionToArea",
		F { return new FunctionRegionToArea(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("regionToDistrict",
		F { return new FunctionRegionToDistrict(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("regionToCountry",
		F { return new FunctionRegionToCountry(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("regionToContinent",
		F { return new FunctionRegionToContinent(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("OSToRoot",
		F { return new FunctionOSToRoot(context.getDictionaries().getTechDataHierarchy()); });
	factory.registerFunction("SEToRoot",
		F { return new FunctionSEToRoot(context.getDictionaries().getTechDataHierarchy()); });
	factory.registerFunction("categoryToRoot",
		F { return new FunctionCategoryToRoot(context.getDictionaries().getCategoriesHierarchy()); });
	factory.registerFunction("categoryToSecondLevel",
		F { return new FunctionCategoryToSecondLevel(context.getDictionaries().getCategoriesHierarchy()); });
	factory.registerFunction("regionIn",
		F { return new FunctionRegionIn(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("OSIn",
		F { return new FunctionOSIn(context.getDictionaries().getTechDataHierarchy()); });
	factory.registerFunction("SEIn",
		F { return new FunctionSEIn(context.getDictionaries().getTechDataHierarchy()); });
	factory.registerFunction("categoryIn",
		F { return new FunctionCategoryIn(context.getDictionaries().getCategoriesHierarchy()); });
	factory.registerFunction("regionHierarchy",
		F { return new FunctionRegionHierarchy(context.getDictionaries().getRegionsHierarchies()); });
	factory.registerFunction("OSHierarchy",
		F { return new FunctionOSHierarchy(context.getDictionaries().getTechDataHierarchy()); });
	factory.registerFunction("SEHierarchy",
		F { return new FunctionSEHierarchy(context.getDictionaries().getTechDataHierarchy()); });
	factory.registerFunction("categoryHierarchy",
		F { return new FunctionCategoryHierarchy(context.getDictionaries().getCategoriesHierarchy()); });
	factory.registerFunction("regionToName",
		F { return new FunctionRegionToName(context.getDictionaries().getRegionsNames()); });

	#undef F
}

}
