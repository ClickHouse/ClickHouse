#include <Common/GuardedPoolAllocatorOptions.h>
#include <Common/GuardedPoolAllocatorOptions.inc>
#include <Common/IO.h>

#include <unistd.h>

#include <cassert>
#include <cstring>
#include <cstdint>
#include <cinttypes>
#include <cstdlib>


#include <cstdio>

namespace
{

enum class OptionType : uint8_t
{
    OT_bool,
    OT_int,
};

class OptionParser
{
public:
    explicit OptionParser() = default;
    void registerOption(const char * name, const char * desc, OptionType type, void * var);
    bool parseString(const char * string);
    static void printOptionDescriptions();

private:
    /// Calculate at compile-time how many options are available.
#define M(...) +1
    static constexpr size_t max_options = 0
    CLICKHOUSE_GWP_ASAN_OPTIONS(M)
#undef M
        ;

    struct Option
    {
        const char * name;
        const char * desc;
        OptionType type;
        void * var;
    } options[max_options];

    size_t options_total = 0;
    const char * buffer = nullptr;
    uintptr_t pos = 0;

    void skipWhitespace();
    bool parseOptions();
    bool parseOption();
    bool setOptionToValue(const char * name, const char * value);
};

#define ARRAY_SIZE(a) (sizeof(a)/sizeof((a)[0]))

/// Macros to avoid using strlen(), since it may fail if SSE is not supported.
#define writeError(data) do \
    { \
        static_assert(__builtin_constant_p(data)); \
        if (!writeRetry(STDERR_FILENO, data, ARRAY_SIZE(data) - 1)) \
            _Exit(1); \
    } while (false)

void OptionParser::printOptionDescriptions()
{
    writeError("GWP-ASan: Available options:\n");
#define M(TYPE, NAME, DEFAULT, DESCRIPTION) \
    writeError("\t"); \
    writeError(#NAME); \
    writeError("\n\t\t–"); \
    writeError(DESCRIPTION); \
    writeError("\n");
    CLICKHOUSE_GWP_ASAN_OPTIONS(M)
#undef M
}

bool isSeparator(char ch)
{
    return ch == ' ' || ch == ',' || ch == ':' || ch == '\n' || ch == '\t' || ch == '\r';
}

bool isSeparatorOrNull(char ch)
{
    return !ch || isSeparator(ch);
}

void OptionParser::skipWhitespace()
{
    while (isSeparator(buffer[pos]))
        ++pos;
}

bool OptionParser::parseOption()
{
    const uintptr_t name_start = pos;
    while (buffer[pos] != '=' && !isSeparatorOrNull(buffer[pos]))
        ++pos;

    const char * name = buffer + name_start;
    if (buffer[pos] != '=')
        return false;
    const uintptr_t value_start = ++pos;
    const char * value;
    if (buffer[pos] == '\'' || buffer[pos] == '"')
    {
        const char quote = buffer[pos++];
        while (buffer[pos] != 0 && buffer[pos] != quote)
            ++pos;
        if (buffer[pos] == 0)
            return false;
        value = buffer + value_start + 1;
        ++pos; // consume the closing quote
    }
    else
    {
        while (!isSeparatorOrNull(buffer[pos]))
            ++pos;
        value = buffer + value_start;
    }

    return setOptionToValue(name, value);
}

bool OptionParser::parseOptions()
{
    while (true)
    {
        skipWhitespace();
        if (buffer[pos] == 0)
            break;
        if (!parseOption())
            return false;
    }
    return true;
}

bool OptionParser::parseString(const char * string)
{
    /// If !string we do not need to parse anything and defaults are applied
    if (!string)
        return true;
    buffer = string;
    pos = 0;
    return parseOptions();
}

bool parseBool(const char * value, bool * b)
{
    if (strncmp(value, "0", 1) == 0 || strncmp(value, "no", 2) == 0 || strncmp(value, "false", 5) == 0)
    {
        *b = false;
        return true;
    }
    if (strncmp(value, "1", 1) == 0 || strncmp(value, "yes", 3) == 0 || strncmp(value, "true", 4) == 0)
    {
        *b = true;
        return true;
    }
    return false;
}

bool OptionParser::setOptionToValue(const char * name, const char * value)
{
    for (size_t i = 0; i < options_total; ++i)
    {
        const uintptr_t len = strlen(options[i].name);
        if (strncmp(name, options[i].name, len) != 0 || name[len] != '=')
            continue;
        bool ok = false;
        switch (options[i].type)
        {
            case OptionType::OT_bool:
                ok = parseBool(value, reinterpret_cast<bool *>(options[i].var));
                if (!ok)
                    return ok;
                break;
            case OptionType::OT_int:
                char * value_end;
                *reinterpret_cast<int *>(options[i].var) = static_cast<int>(strtol(value, &value_end, 10));
                ok = *value_end == '"' || *value_end == '\'' || isSeparatorOrNull(*value_end);
                if (!ok)
                    return ok;
                break;
        }
        return ok;
    }

    return false;
}

void OptionParser::registerOption(const char * name, const char * desc, OptionType type, void * var)
{
    assert(options_total < max_options);
    options[options_total].name = name;
    options[options_total].desc = desc;
    options[options_total].type = type;
    options[options_total].var = var;
    ++options_total;
}

void registerGwpAsanOptions(OptionParser * parser, clickhouse_gwp_asan::Options * opts)
{
#define M(TYPE, NAME, DEFAULT, DESCRIPTION) \
    parser->registerOption(#NAME, DESCRIPTION, OptionType::OT_##TYPE, &opts->NAME);
    CLICKHOUSE_GWP_ASAN_OPTIONS(M)
#undef M
}

clickhouse_gwp_asan::Options * getOptionsInternal()
{
    static clickhouse_gwp_asan::Options gwp_asan_options;
    return &gwp_asan_options;
}

}

namespace clickhouse_gwp_asan
{

bool initOptions(const char * options_str)
{
    Options * opts = getOptionsInternal();
    opts->setDefaults();

    OptionParser parser;
    registerGwpAsanOptions(&parser, opts);

    // Override from the provided options string.
    if (!parser.parseString(options_str))
    {
        return false;
    }

    if (opts->help)
        OptionParser::printOptionDescriptions();

    if (!opts->enabled)
        return true;

    if (opts->max_simultaneous_allocations <= 0)
    {
        writeError("GWP-ASan ERROR: max_simultaneous_allocations must be > 0 when GWP-ASan "
                "is enabled.\n");
        opts->enabled = false;
    }
    if (opts->sample_rate <= 0)
    {
        writeError("GWP-ASan ERROR: sample_rate must be > 0 when GWP-ASan is enabled.\n");
        opts->enabled = false;
    }

    return true;
}

bool initOptions()
{
    return initOptions(getenv("CLICKHOUSE_GWP_ASAN_OPTIONS"));
}

const Options & getOptions() { return *getOptionsInternal(); }

#undef ARRAY_SIZE
#undef writeError

}
