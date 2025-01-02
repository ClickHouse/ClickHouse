#include <Core/Settings.h>
#include <Server/ProxyV1Handler.h>
#include <Poco/Net/NetException.h>
#include <Common/NetException.h>
#include <Common/logger_useful.h>
#include <Interpreters/Context.h>


namespace DB
{
namespace Setting
{
    extern const SettingsSeconds receive_timeout;
}

namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int SOCKET_TIMEOUT;
    extern const int CANNOT_READ_FROM_SOCKET;
    extern const int CANNOT_PARSE_INPUT_ASSERTION_FAILED;
}

void ProxyV1Handler::run()
{
    const auto & settings = server.context()->getSettingsRef();
    socket().setReceiveTimeout(settings[Setting::receive_timeout]);

    std::string word;
    bool eol;

    // Read PROXYv1 protocol header
    // http://www.haproxy.org/download/1.8/doc/proxy-protocol.txt

    // read "PROXY"
    if (!readWord(5, word, eol) || word != "PROXY" || eol)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    // read "TCP4" or "TCP6" or "UNKNOWN"
    if (!readWord(7, word, eol))
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    if (word != "TCP4" && word != "TCP6" && word != "UNKNOWN")
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    if (word == "UNKNOWN" && eol)
        return;

    if (eol)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    // read address
    if (!readWord(39, word, eol) || eol)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    stack_data.forwarded_for = std::move(word);

    // read address
    if (!readWord(39, word, eol) || eol)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    // read port
    if (!readWord(5, word, eol) || eol)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    // read port and "\r\n"
    if (!readWord(5, word, eol) || !eol)
        throw Exception(ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED, "PROXY protocol violation");

    if (!stack_data.forwarded_for.empty())
        LOG_TRACE(log, "Forwarded client address from PROXY header: {}", stack_data.forwarded_for);
}

bool ProxyV1Handler::readWord(int max_len, std::string & word, bool & eol)
{
    word.clear();
    eol = false;

    char ch = 0;
    int n = 0;
    bool is_cr = false;
    try
    {
        for (++max_len; max_len > 0 || is_cr; --max_len)
        {
            n = socket().receiveBytes(&ch, 1);
            if (n == 0)
            {
                socket().shutdown();
                return false;
            }
            if (n < 0)
                break;

            if (is_cr)
                return ch == 0x0A;

            if (ch == 0x0D)
            {
                is_cr = true;
                eol = true;
                continue;
            }

            if (ch == ' ')
                return true;

            word.push_back(ch);
        }
    }
    catch (const Poco::Net::NetException & e)
    {
        throw NetException(ErrorCodes::NETWORK_ERROR, "{}, while reading from socket ({})", e.displayText(), socket().peerAddress().toString());
    }
    catch (const Poco::TimeoutException &)
    {
        throw NetException(ErrorCodes::SOCKET_TIMEOUT, "Timeout exceeded while reading from socket ({}, {} ms)",
            socket().peerAddress().toString(),
            socket().getReceiveTimeout().totalMilliseconds());
    }
    catch (const Poco::IOException & e)
    {
        throw NetException(ErrorCodes::NETWORK_ERROR, "{}, while reading from socket ({})", e.displayText(), socket().peerAddress().toString());
    }

    if (n < 0)
        throw NetException(ErrorCodes::CANNOT_READ_FROM_SOCKET, "Cannot read from socket ({})", socket().peerAddress().toString());

    return false;
}

}
