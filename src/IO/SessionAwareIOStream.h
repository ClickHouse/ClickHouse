#pragma once

#include <iosfwd>


namespace DB
{
/**
 * Wrapper of IOStream to store response stream and corresponding HTTP session.
 */
template <typename Session>
class SessionAwareIOStream : public std::iostream
{
public:
    SessionAwareIOStream(Session session_, std::streambuf * sb)
        : std::iostream(sb)
        , session(std::move(session_))
    {
    }

    Session & getSession() { return session; }

    const Session & getSession() const { return session; }

private:
    /// Poco HTTP session is holder of response stream.
    Session session;
};

}
