#pragma once

#include "Storages/IStorage.h"

#include <memory>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


class StorageS3ReaderFollower : public IStorage 
{
    std::string getName() const override;
    bool isRemote() const override;
    void startup() override;
private:
    class Impl;
    std::unique_ptr<Impl> pimpl;
};


}
