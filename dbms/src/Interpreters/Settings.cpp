#include <DB/Interpreters/Settings.h>


namespace DB
{

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

	for (Poco::Util::AbstractConfiguration::Keys::const_iterator it = config_keys.begin(); it != config_keys.end(); ++it)
	{
		if (*it == "profile")	/// Наследование одного профиля от другого.
			setProfile(config.getString(elem + "." + *it), config);
		else
			set(*it, config.getString(elem + "." + *it));
	}
}

/// Прочитать настройки из буфера. Они записаны как набор name-value пар, идущих подряд, заканчивающихся пустым name.
/// Если выставлен флаг check_readonly, в настройках выставлено readonly, но пришли какие-то изменения кинуть исключение.
void Settings::deserialize(ReadBuffer & buf)
{
	bool readonly = limits.readonly == 1;	/// Если readonly = 2, то можно менять настройки.

	while (true)
	{
		String name;
		readBinary(name, buf);

		/// Пустая строка - это маркер конца настроек.
		if (name.empty())
			break;

		if (!readonly)
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
