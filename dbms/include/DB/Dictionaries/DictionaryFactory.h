#pragma once

#include <DB/Dictionaries/FlatDictionary.h>
#include <DB/Dictionaries/IDictionary.h>
#include <Yandex/singleton.h>
#include <statdaemons/ext/memory.hpp>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class DictionaryFactory : public Singleton<DictionaryFactory>
{
public:
	DictionaryPtr create() const
	{
		return ext::make_unique<FlatDictionary>();
	}
};

}
