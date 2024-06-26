#pragma once

#include "config.h"

#if USE_CEPH

#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <rados/librados.h>
#include <rados/librados.hpp>
#include <fcntl.h>
#include <string>
#include <memory>


namespace DB
{
class WriteBufferFromCeph final : public WriteBufferFromFileBase
{
public:
    WriteBufferFromCeph(
        const String & mon_uri_, /// ceph-monitor uri, e.g. "mon1:1234,mon2:1234,mon3:1234"
        const String & user_,
        const String & secret_,
        const String & pool_,
        const String & object_id_,
        const WriteSettings & write_settings_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        int flags_ = O_WRONLY);

    ~WriteBufferFromCeph() override;

    void nextImpl() override;

    void sync() override {} /// No need to sync, because ceph api is sync

    std::string getFileName() const override { return object_id; }

private:
    void initialize();
    size_t writeImpl(const char * begin, size_t size);
    String mon_uri;
    String pool;
    String object_id;

    std::unique_ptr<librados::Rados> rados;
    std::unique_ptr<librados::IoCtx> io_ctx;
    /// All R/W operations are done using this C-style. librados::IoCtx requires R/W operations
    /// using librados::bufferlist, which is not convenient for us.
    rados_t rados_c;
    rados_ioctx_t io_ctx_c;

    WriteSettings write_settings;
    int flags;
    bool initialized = false;
    bool first_write = true;

};

}
#endif
