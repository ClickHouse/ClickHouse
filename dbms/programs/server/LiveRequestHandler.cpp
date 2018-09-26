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
#include "LiveRequestHandler.h"

#include <IO/HTTPCommon.h>

#include <Common/Exception.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>

namespace DB
{

//@app.route('/live/<cid>', methods=['GET'])  open live channel
//@app.route('/live/<cid>', methods=['DELETE']) delete live channel
//@app.route('/live/<cid>/save', methods=['GET']) temp save channel
//@app.route('/live/<cid>/multi', methods=['DELETE']) delete multiple queries from channel
//@app.route('/live/<cid>/multi/suspend', methods=['GET'])
//@app.route('/live/<cid>/multi/resume', methods=['GET'])
//@app.route('/live/<cid>/multi/update', methods=['GET'])
//@app.route('/live/<cid>/<qid>?query=...', methods=['GET']) add new live query to the channel
//@app.route('/live/<cid>/<qid>', methods=['GET']) read current query result
//@app.route('/live/<cid>/<qid>', methods=['DELETE']) delete query from channel
//@app.route('/live/<cid>/<qid>/suspend', methods=['GET']) suspend query
//@app.route('/live/<cid>/<qid>/resume', methods=['GET']) resume query
//@app.route('/live/<cid>/<qid>/update', methods=['GET']) update query

std::vector<std::string> LiveRequestHandler::splitURI(std::string str, std::string token){
    std::vector<std::string> result;

    while(str.size()){
        int index = str.find(token);

        if(index != std::string::npos)
        {
            result.push_back(str.substr(0,index));
            str = str.substr(index + token.size());
            if(str.size() == 0) result.push_back(str);
        }
        else
        {
            result.push_back(str);
            str = "";
        }
    }
    return result;
}

LiveRequestHandler::LiveRequestHandler(IServer & server_)
    : server(server_)
    , log(&Poco::Logger::get("LiveRequestHandler"))
{
}

void LiveRequestHandler::handleRequest(
    Poco::Net::HTTPServerRequest & request,
    Poco::Net::HTTPServerResponse & response)
{
    try
    {
        const auto & config = server.config();
        const auto & uri = request.getURI();

        setResponseDefaultHeaders(response, config.getUInt("keep_alive_timeout", 10));

        response.setContentType("text/html; charset=UTF-8");

        std::vector<std::string> uri_parts = splitURI(uri, "/");

        // route('/live', methods=['GET'])
        if (uri_parts.size() == 2 && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET) {
            response.send() << "Ok." << "\n";
        }
        // route('/live/<cid>', methods=['GET'])
        else if (uri_parts.size() == 3 && request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET) {
            response.setContentLength(Poco::Net::HTTPMessage::UNKNOWN_CONTENT_LENGTH);

            /// For keep-alive to work we need HTTP 1.1
            if (request.getVersion() == Poco::Net::HTTPServerRequest::HTTP_1_1) {
                response.setChunkedTransferEncoding(true);
            }
            else {
                response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_VERSION_NOT_SUPPORTED);
                response.send();
                return;
            }

            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_OK);
            std::ostream & out = response.send();

            while (true) {
                Poco::Timestamp timestamp = Poco::Timestamp();
                // timestamp in seconds
                double d_timestamp = (double)timestamp.epochMicroseconds()/1000000;

                LOG_TRACE(log, "Sending heartbeat..." << std::setprecision(16) << d_timestamp);

                out << "{\"heartbeat\":{\"time\":\"" << std::setprecision(16) << d_timestamp << "\"}}" << "\n";
                out.flush();

                std::this_thread::sleep_for(std::chrono::milliseconds(5000));
                if ( out.bad() == true ) {
                    LOG_TRACE(log, "Bad output stream exiting...");
                    break;
                }
            }
        }
        // route('/live/<cid>', methods=['DELETE'])
        else if (uri_parts.size() == 3 && request.getMethod() == Poco::Net::HTTPRequest::HTTP_DELETE) {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_METHOD_NOT_ALLOWED, Poco::Net::HTTPResponse::HTTP_REASON_NOT_IMPLEMENTED);
            response.send();
        }
        // invalid handle
        else {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_NOT_FOUND);
            response.send() << "There is no handle " << request.getURI() << "\n";
        }
    }
    catch (...)
    {
        tryLogCurrentException("LiveRequestHandler");
    }
}

}
