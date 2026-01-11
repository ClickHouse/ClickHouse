/// Example taken from https://github.com/googleapis/google-cloud-cpp/tree/main/google/cloud/storage#quickstart

#include <google/cloud/storage/client.h>
#include <iostream>
#include <string>

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "Missing bucket name.\n";
        std::cerr << "Usage: quickstart <bucket-name>\n";
        return 1;
    }
    std::string const bucket_name = argv[1];

    // Create a client to communicate with Google Cloud Storage. This client
    // uses the default configuration for authentication and project id.
    auto client = google::cloud::storage::Client();

    auto writer = client.WriteObject(bucket_name, "quickstart.txt");
    writer << "Hello World!";
    writer.Close();
    if (!writer.metadata())
    {
        std::cerr << "Error creating object: " << writer.metadata().status()
                            << "\n";
        return 1;
    }
    std::cout << "Successfully created object: " << *writer.metadata() << "\n";

    auto reader = client.ReadObject(bucket_name, "quickstart.txt");
    if (!reader)
    {
        std::cerr << "Error reading object: " << reader.status() << "\n";
        return 1;
    }

    std::string contents{std::istreambuf_iterator<char>{reader}, {}};
    std::cout << contents << "\n";

    return 0;
}
