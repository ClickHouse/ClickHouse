/* Copyright (c) 2018 BlackBerry Limited

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. */
#pragma once

#include "IServer.h"

#include <common/logger_useful.h>
#include <Poco/Net/HTTPRequestHandler.h>

namespace DB
{

class LiveRequestHandler : public Poco::Net::HTTPRequestHandler
{
private:
    IServer & server;
    Poco::Logger * log;

    std::vector<std::string> splitURI(std::string str, std::string token);

public:
    explicit LiveRequestHandler(IServer & server_);

    void handleRequest(
        Poco::Net::HTTPServerRequest & request,
        Poco::Net::HTTPServerResponse & response) override;
};

}
