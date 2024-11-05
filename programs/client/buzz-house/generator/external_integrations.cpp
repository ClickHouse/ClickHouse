#include "external_integrations.h"

#include <array>
#include <cctype>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <netdb.h>
#include <unistd.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>

static void ExecuteCommand(const char * cmd, std::string & result)
{
    std::array<char, 256> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);

    if (!pipe)
    {
        throw std::runtime_error("popen() failed!");
    }
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe.get()) != nullptr)
    {
        result += buffer.data();
    }
}

namespace buzzhouse
{
bool MinIOIntegration::SendRequest(const std::string & resource)
{
    int sock;
    char buffer[1024];
    struct sockaddr_in client = {};
    std::stringstream ss, sign_cmd;
    std::string sign;
    const std::time_t time = std::time({});
    const struct hostent * host = gethostbyname(sc.hostname.c_str());

    if (!host || !host->h_addr)
    {
        std::cerr << "Error retrieving DNS information." << std::endl;
        return false;
    }

    client.sin_family = AF_INET;
    client.sin_port = htonl(sc.port);
    std::memcpy(&client.sin_addr, host->h_addr, host->h_length);

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0)
    {
        std::cerr << "Error creating socket: " << strerror(errno) << std::endl;
        return false;
    }
    if (connect(sock, reinterpret_cast<const struct sockaddr *>(&client), sizeof(client)) < 0)
    {
        close(sock);
        std::cerr << "Could not connect: " << strerror(errno) << std::endl;
        return false;
    }

    (void)std::strftime(buffer, sizeof(buffer), "%a, %b %d %H:%M:%S %Y %Z", std::gmtime(&time));
    sign_cmd << "echo -en \""
             << "PUT\n\napplication/octet-stream\n"
             << buffer << "\n"
             << resource << "\" | openssl sha1 -hmac " << sc.password << " -binary | base64";
    ExecuteCommand(sign_cmd.str().c_str(), sign);

    ss << "PUT " << resource << "HTTP/1.1\r\n"
       << "Host: " << sc.hostname << ":" << std::to_string(sc.port) << "\r\n"
       << "Accept: */*\r\n"
       << "Date: " << buffer << "\r\n"
       << "Content-Type: application/octet-stream\r\n"
       << "Authorization: AWS " << sc.user << ":/" << sign << "\r\n"
       << "Content-Length: 0\r\n"
       << "\r\n\r\n";
    const std::string & request = ss.str();

    if (send(sock, request.c_str(), request.length(), 0) != static_cast<int>(request.length()))
    {
        close(sock);
        std::cerr << "Error sending request: " << strerror(errno) << std::endl;
        return false;
    }

    while (read(sock, buffer, sizeof(buffer)) > 0)
    {
        std::cout << buffer;
    }
    close(sock);
    return true;
}

}
