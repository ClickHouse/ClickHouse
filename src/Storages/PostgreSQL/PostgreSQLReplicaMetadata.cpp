#include "PostgreSQLReplicaMetadata.h"
#include <Poco/File.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


PostgreSQLReplicaMetadata::PostgreSQLReplicaMetadata(const std::string & metadata_file_path)
    : metadata_file(metadata_file_path)
    , tmp_metadata_file(metadata_file_path + ".tmp")
    , data_version(1)
{
}


void PostgreSQLReplicaMetadata::readDataVersion()
{
    if (Poco::File(metadata_file).exists())
    {
        ReadBufferFromFile in(metadata_file, DBMS_DEFAULT_BUFFER_SIZE);
        assertString("\nData version:\t", in);
        readIntText(data_version, in);

        LOG_DEBUG(&Poco::Logger::get("PostgreSQLReplicaMetadata"),
                "Last written version is {}. (From metadata file {})", data_version, metadata_file);
    }
}


void PostgreSQLReplicaMetadata::writeDataVersion()
{
    WriteBufferFromFile out(tmp_metadata_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_TRUNC | O_CREAT);
    writeString("\nData version:\t" + toString(data_version), out);

    out.next();
    out.sync();
    out.close();
}


/// While data is recieved, version is updated. Before table sync, write last version to tmp file.
/// Then sync data to table and rename tmp to non-tmp.
void PostgreSQLReplicaMetadata::commitVersion(const std::function<void()> & finalizeStreamFunc)
{
    writeDataVersion();

    try
    {
        /// TODO: return last actially written lsn and write it to file
        finalizeStreamFunc();
        Poco::File(tmp_metadata_file).renameTo(metadata_file);
    }
    catch (...)
    {
        Poco::File(tmp_metadata_file).remove();
        throw;
    }
}

}
