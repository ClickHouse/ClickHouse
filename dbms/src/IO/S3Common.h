#pragma once

#include <Core/Types.h>

#include <Poco/Net/HTTPRequest.h>


namespace DB
{

struct S3Helper
{
    static void authenticateRequest(Poco::Net::HTTPRequest & request,
        const String & access_key_id,
        const String & secret_access_key);
};

}
