#pragma once

#include <Core/Types.h>

#include <Poco/Net/HTTPRequest.h>


namespace DB
{

namespace S3Helper
{
    void authenticateRequest(
        Poco::Net::HTTPRequest & request,
        const String & access_key_id,
        const String & secret_access_key);
};

}
