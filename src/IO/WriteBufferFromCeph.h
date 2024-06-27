#pragma once

#include "Disks/WriteMode.h"
#include "config.h"

#if USE_CEPH

#include "IO/Ceph/RadosIO.h"
#include <IO/WriteBuffer.h>
#include <IO/WriteSettings.h>
#include <IO/WriteBufferFromFileBase.h>


namespace DB
{
class WriteBufferFromCeph final : public WriteBufferFromFileBase
{
public:
    WriteBufferFromCeph(
        std::shared_ptr<librados::Rados> rados_,
        const String & pool,
        const String & object_id_,
        const WriteSettings & write_settings_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode_ = WriteMode::Rewrite);

    WriteBufferFromCeph(
        std::unique_ptr<Ceph::RadosIO> impl_,
        const String & object_id_,
        const WriteSettings & write_settings_,
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode_ = WriteMode::Rewrite);

    ~WriteBufferFromCeph() override;

    void nextImpl() override;

    void sync() override {} /// No need to sync, because ceph api is sync

    std::string getFileName() const override { return object_id; }

private:
    void initialize();
    size_t writeImpl(const char * begin, size_t size);

    std::unique_ptr<Ceph::RadosIO> impl;

    String object_id;
    WriteSettings write_settings;
    WriteMode mode;
    bool initialized = false;
    bool first_write = true;

};

}
#endif
