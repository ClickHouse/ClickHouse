/// Example taken from https://github.com/googleapis/google-cloud-cpp/tree/main/google/cloud/kms#quickstart

#include <google/cloud/kms/v1/key_management_client.h>
#include <google/cloud/location.h>
#include <iostream>

int main(int argc, char* argv[]) try
{
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
        if (!kr) throw std::move(kr).status(); // NOLINT
        std::cout << kr->DebugString() << "\n";
    }

    return 0;
}
catch (google::cloud::Status const& status)
{
    std::cerr << "google::cloud::Status thrown: " << status << "\n";
    return 1;
}
