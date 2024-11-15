#include <string>

#include <Common/Base64.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <IO/S3/AWSLogger.h>
#include <IO/S3/Credentials.h>
#include <IO/S3/PocoHTTPClient.h>
#include <IO/S3/PocoHTTPClientFactory.h>
#include <IO/S3Common.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>

#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <base/scope_guard.h>

#include "google/cloud/kms/v1/key_management_client.h"
#include "google/cloud/location.h"
#include "google/cloud/status.h"

using namespace DB;
using namespace S3;

int main(int argc, char * argv[])
{
    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());
    global_context->makeGlobalContext();

    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("debug");

    auto logger = getLogger("PMO");
    LOG_DEBUG(logger, "GCP KMS testing...");

    if (argc != 3)
    {
        std::cerr << "Usage: " << argv[0] << " project-id location-id\n";
        return 1;
    }

    auto const location = google::cloud::Location(argv[1], argv[2]);

    namespace kms = ::google::cloud::kms_v1;
    auto client = kms::KeyManagementServiceClient(
        kms::MakeKeyManagementServiceConnection());

    for (auto kr : client.ListKeyRings(location.FullName()))
    {
        if (!kr) throw std::runtime_error(std::move(kr).status().message());
        std::cout << kr->DebugString() << "\n";
    }

    return 0;
}
