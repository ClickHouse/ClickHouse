#pragma once

#include "config.h"

#if USE_CEPH

#include <Disks/WriteMode.h>
#include <IO/Ceph/RadosIO.h>
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
        const String & nspace,
        const String & object_id_,
        const WriteSettings & write_settings_,
        std::optional<std::map<String, String>> object_attributes_ = {},
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode_ = WriteMode::Rewrite);

    WriteBufferFromCeph(
        std::shared_ptr<Ceph::RadosIO> impl_,
        const String & object_id_,
        const WriteSettings & write_settings_,
        std::optional<std::map<String, String>> object_attributes_ = {},
        size_t buf_size_ = DBMS_DEFAULT_BUFFER_SIZE,
        WriteMode mode_ = WriteMode::Rewrite);

    ~WriteBufferFromCeph() override = default;

    void sync() override {} /// No need to sync, because ceph api is sync

    std::string getFileName() const override { return object_id; }

private:
    static String getObjectName(const String & object_id, size_t object_seq)
    {
        return fmt::format("{}.#{}", object_id, object_seq);
    }
    void initialize();
    size_t writeImpl(const char * begin, size_t len);
    void setObjectAttributes();

    void nextImpl() override;
    void finalizeImpl() override;

    std::shared_ptr<Ceph::RadosIO> impl;

    String object_id;
    WriteSettings write_settings;
    std::optional<std::map<String, String>> object_attributes;
    WriteMode mode;
    bool initialized = false;
    bool first_write = true;

};

}
#endif
