#include <Interpreters/Settings.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_SETTING;
    extern const int THERE_IS_NO_PROFILE;
    extern const int NO_ELEMENTS_IN_CONFIG;
}


/// Set the configuration by name.
void Settings::set(const String & name, const Field & value)
{
#define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) NAME.set(value);

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_SET)
    else if (!limits.trySet(name, value))
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

/// Set the configuration by name. Read the binary serialized value from the buffer (for interserver interaction).
void Settings::set(const String & name, ReadBuffer & buf)
{
#define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) NAME.set(buf);

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_SET)
    else if (!limits.trySet(name, buf))
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

/// Skip the binary-serialized value from the buffer.
void Settings::ignore(const String & name, ReadBuffer & buf)
{
#define TRY_IGNORE(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) decltype(NAME)(DEFAULT).set(buf);

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_IGNORE)
    else if (!limits.tryIgnore(name, buf))
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

    #undef TRY_IGNORE
}

/** Set the setting by name. Read the value in text form from a string (for example, from a config, or from a URL parameter).
    */
void Settings::set(const String & name, const String & value)
{
#define TRY_SET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) NAME.set(value);

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_SET)
    else if (!limits.trySet(name, value))
        throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);

#undef TRY_SET
}

String Settings::get(const String & name) const
{
#define GET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) return NAME.toString();

    if (false) {}
    APPLY_FOR_SETTINGS(GET)
    else
    {
        String value;
        if (!limits.tryGet(name, value))
            throw Exception("Unknown setting " + name, ErrorCodes::UNKNOWN_SETTING);
        return value;
    }

#undef GET
}

bool Settings::tryGet(const String & name, String & value) const
{
#define TRY_GET(TYPE, NAME, DEFAULT, DESCRIPTION) \
    else if (name == #NAME) { value = NAME.toString(); return true; }

    if (false) {}
    APPLY_FOR_SETTINGS(TRY_GET)
    else
        return limits.tryGet(name, value);

#undef TRY_GET
}

/** Set the settings from the profile (in the server configuration, many settings can be listed in one profile).
    * The profile can also be set using the `set` functions, like the `profile` setting.
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
        if (key == "profile")   /// Inheritance of one profile from another.
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

/// Read the settings from the buffer. They are written as a set of name-value pairs that go successively, ending with an empty `name`.
/// If the `check_readonly` flag is set, `readonly` is set in the preferences, but some changes have occurred - throw an exception.
void Settings::deserialize(ReadBuffer & buf)
{
    auto before_readonly = limits.readonly;

    while (true)
    {
        String name;
        readBinary(name, buf);

        /// An empty string is the marker for the end of the settings.
        if (name.empty())
            break;

        /// If readonly = 2, then you can change the settings, except for the readonly setting.
        if (before_readonly == 0 || (before_readonly == 2 && name != "readonly"))
            set(name, buf);
        else
            ignore(name, buf);
    }
}

/// Record the changed settings to the buffer. (For example, to send to a remote server.)
void Settings::serialize(WriteBuffer & buf) const
{
#define WRITE(TYPE, NAME, DEFAULT, DESCRIPTION) \
    if (NAME.changed) \
    { \
        writeStringBinary(#NAME, buf); \
        NAME.write(buf); \
    }

    APPLY_FOR_SETTINGS(WRITE)

    limits.serialize(buf);

    /// An empty string is a marker for the end of the settings.
    writeStringBinary("", buf);

#undef WRITE
}

}
