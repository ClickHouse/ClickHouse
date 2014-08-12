#pragma once

#include <DB/Core/Types.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <map>

namespace DB
{

/** Раскрывает в строке макросы из конфига.
  */
class Macros
{
public:
	Macros();
	Macros(const Poco::Util::AbstractConfiguration & config, const String & key);

	/// Заменить в строке подстроки вида {macro_name} на значение для macro_name, полученное из конфига.
	String expand(const String & s) const;

private:
	typedef std::map<String, String> MacroMap;

	MacroMap macros;
};

}
