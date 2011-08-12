#pragma once

#include <map>

#include <Poco/SharedPtr.h>

#include <DB/Core/NamesAndTypes.h>
#include <DB/Functions/IFunction.h>


namespace DB
{

using Poco::SharedPtr;

typedef std::map<String, FunctionPtr> Functions;

/** Набор известных объектов, которые могут быть использованы в запросе.
  */
struct Context
{
	Functions functions;
	NamesAndTypes columns;
};


}
