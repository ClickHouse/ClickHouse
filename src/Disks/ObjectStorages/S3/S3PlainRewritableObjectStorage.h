#pragma once

#include <Disks/ObjectStorages/S3/S3ObjectStorage.h>

namespace DB
{

class S3PlainRewritableObjectStorage : public S3ObjectStorage
{
public:
    template <class... Args>
    explicit S3PlainRewritableObjectStorage(Args &&... args) : S3ObjectStorage(std::forward<Args>(args)...)
    {
    }

    std::string getName() const override { return "S3PlainRewritableObjectStorage"; }

    bool isWriteOnce() const override { return false; }

    bool isPlain() const override { return true; }
};

}
