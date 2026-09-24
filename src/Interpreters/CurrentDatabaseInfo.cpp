#include <Interpreters/CurrentDatabaseInfo.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>


namespace DB
{

CurrentDatabaseInfo::CurrentDatabaseInfo(String full_name_)
{
    /// a quoted first component is one literal database name, the dot search skips it
    if (!full_name_.empty() && (full_name_.front() == '\'' || full_name_.front() == '"'))
    {
        ReadBufferFromString in(full_name_);
        String database;
        const bool closed = full_name_.front() == '\'' ? tryReadQuotedString(database, in) : tryReadDoubleQuotedString(database, in);
        if (closed && !database.empty() && in.eof())
        {
            value = std::move(database);
            return;
        }
        if (closed && !database.empty() && *in.position() == '.' && in.available() > 1)
        {
            separator_idx = database.size();
            value = std::move(database);
            value += '.';
            value.append(in.position() + 1, in.buffer().end());
            return;
        }
        /// not a well-formed quoted component: a literal name, kept as is
        value = std::move(full_name_);
        return;
    }

    value = std::move(full_name_);
    const auto dot = value.find('.');
    /// a leading or trailing dot separates nothing
    if (dot != String::npos && dot != 0 && dot + 1 != value.size())
        separator_idx = dot;
}

}
