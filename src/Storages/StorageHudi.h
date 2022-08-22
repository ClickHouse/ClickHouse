#pragma once

#include "config_core.h"

#include <Storages/IStorage.h>
#include <Storages/StorageS3.h>

namespace Poco {
    class Logger;
}

namespace Aws::S3
{
    class S3Client;
}

namespace DB
{

class StorageHudi final : public IStorage {
public:
    StorageHudi(
        const S3::URI& uri_,
        const String& access_key_,
        const String& secret_access_key_,
        const StorageID & table_id_,
        ColumnsDescription columns_description_,
        ConstraintsDescription constraints_description_,
        const String & comment,
        ContextPtr context_
    );

    String getName() const override { return "Hudi"; }

private:
    static void updateS3Configuration(ContextPtr, StorageS3::S3Configuration &);

private:
    StorageS3::S3Configuration s3_configuration;
    Poco::Logger * log;
};

}
