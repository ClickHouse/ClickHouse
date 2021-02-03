#pragma once
#include <Interpreters/Context.h>


namespace DB
{

class PostgreSQLReplicaMetadata
{
public:
    PostgreSQLReplicaMetadata(const std::string & metadata_file_path);

    void commitVersion(const std::function<void()> & syncTableFunc);

    size_t version()
    {
        return data_version++;
    }

private:
    void readDataVersion();
    void writeDataVersion();

    const std::string metadata_file;
    const std::string tmp_metadata_file;

    size_t data_version;

};

}
