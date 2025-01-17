#pragma once
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include "delta_kernel_ffi.hpp"

namespace DeltaLake
{

class IKernelHelper
{
public:
    virtual ~IKernelHelper() = default;

    virtual const std::string & getTablePath() const = 0;

    virtual const std::string & getDataPath() const = 0;

    virtual ffi::EngineBuilder * createBuilder() const = 0;
};

using KernelHelperPtr = std::shared_ptr<IKernelHelper>;

}

namespace DB
{

DeltaLake::KernelHelperPtr getKernelHelper(const StorageObjectStorage::ConfigurationPtr & configuration);


}
