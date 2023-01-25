#include <Common/LoggingFormatStringHelpers.h>

[[noreturn]] void functionThatFailsCompilationOfConstevalFunctions(const char * error)
{
    throw std::runtime_error(error);
}

template <typename... Args>
PreformattedMessage FormatStringHelperImpl<Args...>::format(Args && ...args) const
{
    return PreformattedMessage{fmt::format(fmt_str, std::forward<Args...>(args)...), message_format_string};
}
