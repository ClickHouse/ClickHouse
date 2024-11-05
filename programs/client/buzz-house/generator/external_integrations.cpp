#include "external_integrations.h"

#include <array>
#include <cctype>
#include <cinttypes>
#include <cstring>
#include <ctime>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

namespace buzzhouse
{

static bool ExecuteCommand(const char * cmd, std::string & result)
{
    char buffer[1024];
    FILE * pp = popen(cmd, "r");

    if (!pp)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        std::cerr << "popen error: " << buffer << std::endl;
        return false;
    }
    std::unique_ptr<FILE, decltype(&pclose)> pipe(pp, pclose);
    if (!pipe)
    {
        return false;
    }
    while (fgets(buffer, static_cast<int>(sizeof(buffer)), pipe.get()))
    {
        result += buffer;
    }
    return true;
}

bool MinIOIntegration::SendRequest(const std::string & resource)
{
    struct tm ttm;
    std::string sign;
    ssize_t nbytes = 0;
    bool created = false;
    int sock = -1, error = 0;
    char buffer[1024], found_ip[1024];
    const std::time_t time = std::time({});
    std::stringstream http_request, sign_cmd;
    struct addrinfo hints = {}, *result = nullptr;

    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = IPPROTO_TCP;

    (void)std::sprintf(buffer, "%" PRIu32 "", sc.port);
    if ((error = getaddrinfo(sc.hostname.c_str(), buffer, &hints, &result)) != 0)
    {
        if (error == EAI_SYSTEM)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            std::cerr << "getaddrinfo: " << buffer << std::endl;
        }
        else
        {
            std::cerr << "getaddrinfo: " << gai_strerror(error) << std::endl;
        }
        return false;
    }
    /* Loop through results */
    for (const struct addrinfo * p = result; p; p = p->ai_next)
    {
        if ((sock = socket(p->ai_family, p->ai_socktype, p->ai_protocol)) == -1)
        {
            strerror_r(errno, buffer, sizeof(buffer));
            std::cerr << "Could not connect: " << buffer << std::endl;
            return false;
        }
        if (connect(sock, p->ai_addr, p->ai_addrlen) == 0)
        {
            if ((error = getnameinfo(p->ai_addr, p->ai_addrlen, found_ip, sizeof(found_ip), nullptr, 0, NI_NUMERICHOST)) != 0)
            {
                if (error == EAI_SYSTEM)
                {
                    strerror_r(errno, buffer, sizeof(buffer));
                    std::cerr << "getnameinfo: " << buffer << std::endl;
                }
                else
                {
                    std::cerr << "getnameinfo: " << gai_strerror(error) << std::endl;
                }
                return false;
            }
            break;
        }
        error = errno;
        close(sock);
        sock = -1;
    }
    freeaddrinfo(result);
    if (sock == -1)
    {
        strerror_r(errno, buffer, sizeof(buffer));
        std::cerr << "Could not connect: " << buffer << std::endl;
        return false;
    }
    (void)gmtime_r(&time, &ttm);
    (void)std::strftime(buffer, sizeof(buffer), "%a, %d %b %Y %H:%M:%S %z", &ttm);
    sign_cmd << R"(printf "PUT\n\napplication/octet-stream\n)" << buffer << "\\n"
             << resource << "\""
             << " | openssl sha1 -hmac " << sc.password << " -binary | base64";
    if (!ExecuteCommand(sign_cmd.str().c_str(), sign))
    {
        close(sock);
        return false;
    }

    http_request << "PUT " << resource << " HTTP/1.1" << std::endl
                 << "Host: " << found_ip << ":" << std::to_string(sc.port) << std::endl
                 << "Accept: */*" << std::endl
                 << "Date: " << buffer << std::endl
                 << "Content-Type: application/octet-stream" << std::endl
                 << "Authorization: AWS " << sc.user << ":" << sign << "Content-Length: 0" << std::endl
                 << std::endl
                 << std::endl;

    if (send(sock, http_request.str().c_str(), http_request.str().length(), 0) != static_cast<int>(http_request.str().length()))
    {
        strerror_r(errno, buffer, sizeof(buffer));
        close(sock);
        std::cerr << "Error sending request: " << http_request.str() << std::endl << buffer << std::endl;
        return false;
    }
    if ((nbytes = read(sock, buffer, sizeof(buffer))) > 0 && nbytes < static_cast<ssize_t>(sizeof(buffer)) && nbytes > 12
        && !(created = (std::strncmp(buffer + 9, "200", 3) == 0)))
    {
        std::cerr << "Request not successful: " << http_request.str() << std::endl << buffer << std::endl;
    }
    close(sock);
    return created;
}

}
