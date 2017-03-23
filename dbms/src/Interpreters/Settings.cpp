#include <DB/Interpreters/Settings.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_SETTING;
	extern const int THERE_IS_NO_PROFILE;
	extern const int NO_ELEMENTS_IN_CONFIG;
}


/// Установить настройку по имени.
void Settings::set(const String & name, const Field & value)
{
#define TRY_SET(TYPE, NAME, DEFAULT) \
	else if (name == #NAME) NAME.set(value);

	if (false) {}
	APPLY_FOR_SETTINGS(TRY_SET)
	else if (!limits.trySet(name, value))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

/// Установить настройку по имени. Прочитать сериализованное в бинарном виде значение из буфера (для межсерверного взаимодействия).
void Settings::set(const String & name, ReadBuffer & buf)
{
#define TRY_SET(TYPE, NAME, DEFAULT) \
	else if (name == #NAME) NAME.set(buf);

	if (false) {}
	APPLY_FOR_SETTINGS(TRY_SET)
	else if (!limits.trySet(name, buf))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

/// Пропустить сериализованное в бинарном виде значение из буфера.
void Settings::ignore(const String & name, ReadBuffer & buf)
{
#define TRY_IGNORE(TYPE, NAME, DEFAULT) \
	else if (name == #NAME) decltype(NAME)(DEFAULT).set(buf);

	if (false) {}
	APPLY_FOR_SETTINGS(TRY_IGNORE)
	else if (!limits.tryIgnore(name, buf))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

	#undef TRY_IGNORE
}

/** Установить настройку по имени. Прочитать значение в текстовом виде из строки (например, из конфига, или из параметра URL).
	*/
void Settings::set(const String & name, const String & value)
{
#define TRY_SET(TYPE, NAME, DEFAULT) \
	else if (name == #NAME) NAME.set(value);

	if (false) {}
	APPLY_FOR_SETTINGS(TRY_SET)
	else if (!limits.trySet(name, value))
		throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

/** Установить настройки из профиля (в конфиге сервера, в одном профиле может быть перечислено много настроек).
	* Профиль также может быть установлен с помощью функций set, как настройка profile.
	*/
void Settings::setProfile(const String & profile_name, Poco::Util::AbstractConfiguration & config)
{
	String elem = "profiles." + profile_name;

	if (!config.has(elem))
		throw Exception("There is no profile '" + profile_name + "' in configuration file.", ErrorCodes::THERE_IS_NO_PROFILE);

	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(elem, config_keys);

	for (const std::string & key : config_keys)
	{
		if (key == "profile")	/// Наследование одного профиля от другого.
			setProfile(config.getString(elem + "." + key), config);
		else
			set(key, config.getString(elem + "." + key));
	}
}

void Settings::loadSettingsFromConfig(const String & path, const Poco::Util::AbstractConfiguration & config)
{
	if (!config.has(path))
		throw Exception("There is no path '" + path + "' in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

	Poco::Util::AbstractConfiguration::Keys config_keys;
	config.keys(path, config_keys);

	for (const std::string & key : config_keys)
	{
		set(key, config.getString(path + "." + key));
	}
}

/// Прочитать настройки из буфера. Они записаны как набор name-value пар, идущих подряд, заканчивающихся пустым name.
/// Если выставлен флаг check_readonly, в настройках выставлено readonly, но пришли какие-то изменения кинуть исключение.
void Settings::deserialize(ReadBuffer & buf)
{
	auto before_readonly = limits.readonly;

	while (true)
	{
		String name;
		readBinary(name, buf);

		/// Пустая строка - это маркер конца настроек.
		if (name.empty())
			break;

		/// Если readonly = 2, то можно менять настройки, кроме настройки readonly.
		if (before_readonly == 0 || (before_readonly == 2 && name != "readonly"))
			set(name, buf);
		else
			ignore(name, buf);
	}
}

/// Записать изменённые настройки в буфер. (Например, для отправки на удалённый сервер.)
void Settings::serialize(WriteBuffer & buf) const
{
#define WRITE(TYPE, NAME, DEFAULT) \
	if (NAME.changed) \
	{ \
		writeStringBinary(#NAME, buf); \
		NAME.write(buf); \
	}

	APPLY_FOR_SETTINGS(WRITE)

	limits.serialize(buf);

	/// Пустая строка - это маркер конца настроек.
	writeStringBinary("", buf);

#undef WRITE
}

}
