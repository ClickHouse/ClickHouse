#pragma once

#include <Server/HTTP/HTTPRequestHandler.h>


namespace DB
{

class IServer;

/// Response with file to user.
class FileRequestHandler : public HTTPRequestHandler
{
private:
    IServer & server;
    const std::string & base_directory_path;

public:
    FileRequestHandler(IServer & server_, const std::string & base_directory_path_);
    void handleRequest(HTTPServerRequest & request, HTTPServerResponse & response) override;
};

}
