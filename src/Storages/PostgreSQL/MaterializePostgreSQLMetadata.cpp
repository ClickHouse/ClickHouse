#include "MaterializePostgreSQLMetadata.h"

#if USE_LIBPQXX
#include <Poco/File.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <common/logger_useful.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>


namespace DB
{

MaterializePostgreSQLMetadata::MaterializePostgreSQLMetadata(const std::string & metadata_file_path)
    : metadata_file(metadata_file_path)
    , tmp_metadata_file(metadata_file_path + ".tmp")
    , last_version(1)
{
}


void MaterializePostgreSQLMetadata::readMetadata()
{
    if (Poco::File(metadata_file).exists())
    {
        ReadBufferFromFile in(metadata_file, DBMS_DEFAULT_BUFFER_SIZE);

        assertString("\nLast version:\t", in);
        readIntText(last_version, in);

        assertString("\nLast LSN:\t", in);
        readString(last_lsn, in);

        if (checkString("\nActual LSN:\t", in))
        {
            std::string actual_lsn;
            readString(actual_lsn, in);

            if (!actual_lsn.empty())
                last_lsn = actual_lsn;
        }

        LOG_DEBUG(&Poco::Logger::get("MaterializePostgreSQLMetadata"),
                "Last written version is {}. (From metadata file {})", last_version, metadata_file);
    }
}


void MaterializePostgreSQLMetadata::writeMetadata(bool append_metadata)
{
    WriteBufferFromFile out(tmp_metadata_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_TRUNC | O_CREAT);

    if (append_metadata)
    {
        writeString("\nActual LSN:\t" + toString(last_lsn), out);
    }
    else
    {
        writeString("\nLast version:\t" + toString(last_version), out);
        writeString("\nLast LSN:\t" + toString(last_lsn), out);
    }

    out.next();
    out.sync();
    out.close();
}


/// While data is received, version is updated. Before table sync, write last version to tmp file.
/// Then sync data to table and rename tmp to non-tmp.
void MaterializePostgreSQLMetadata::commitMetadata(std::string & lsn, const std::function<String()> & finalizeStreamFunc)
{
    std::string actual_lsn;
    last_lsn = lsn;
    writeMetadata();

    try
    {
        actual_lsn = finalizeStreamFunc();
        /// This is not supposed to happen
        if (actual_lsn != last_lsn)
        {
            writeMetadata(true);
            LOG_WARNING(&Poco::Logger::get("MaterializePostgreSQLMetadata"),
                    "Last written LSN {} is not equal to actual LSN {}", last_lsn, actual_lsn);
        }

        Poco::File(tmp_metadata_file).renameTo(metadata_file);
    }
    catch (...)
    {
        Poco::File(tmp_metadata_file).remove();
        throw;
    }
}

}

#endif
