#pragma once

#include <DB/Functions/IFunction.h>
#include <common/singleton.h>


namespace DB
{

class Context;


/** Creates function by name.
  * Function could use for initialization (take ownership of shared_ptr, for example)
  *  some dictionaries from Context.
  */
class FunctionFactory : public Singleton<FunctionFactory>
{
	friend class StorageSystemFunctions;

private:
	typedef FunctionPtr (*Creator)(const Context & context);	/// Not std::function, for lower object size and less indirection.
	std::unordered_map<String, Creator> functions;

public:
	FunctionFactory();

	FunctionPtr get(const String & name, const Context & context) const;	/// Throws an exception if not found.
	FunctionPtr tryGet(const String & name, const Context & context) const;	/// Returns nullptr if not found.

	/// No locking, you must register all functions before usage of get, tryGet.
	template <typename F> void registerFunction()
	{
		static_assert(std::is_same<decltype(&F::create), Creator>::value, "F::create has incorrect type");
		functions[F::name] = &F::create;
	}
};

}
