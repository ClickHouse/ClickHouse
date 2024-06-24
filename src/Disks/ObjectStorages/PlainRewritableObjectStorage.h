#pragma once

#include <Disks/ObjectStorages/IObjectStorage.h>

namespace DB
{

template <typename BaseObjectStorage>
class PlainRewritableObjectStorage : public BaseObjectStorage
{
public:
    template <class... Args>
    explicit PlainRewritableObjectStorage(Args &&... args) : BaseObjectStorage(std::forward<Args>(args)...)
    {
    }

    std::string getName() const override { return "PlainRewritable" + BaseObjectStorage::getName(); }

    bool isWriteOnce() const override { return false; }

    bool isPlain() const override { return true; }
};

}
