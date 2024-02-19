#pragma once
#include "config.h"

#if USE_HDFS
#include <Storages/ObjectStorage/StorageObjectStorageConfiguration.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/IAST_fwd.h>

namespace DB
{

class StorageHDFSConfiguration : public StorageObjectStorageConfiguration
{
public:
    StorageHDFSConfiguration() = default;
    StorageHDFSConfiguration(const StorageHDFSConfiguration & other);

    Path getPath() const override { return path; }
    void setPath(const Path & path_) override { path = path_; }

    const Paths & getPaths() const override { return paths; }
    Paths & getPaths() override { return paths; }

    String getNamespace() const override { return ""; }
    String getDataSourceDescription() override { return url; }

    void check(ContextPtr context) const override;
    ObjectStoragePtr createObjectStorage(ContextPtr context, bool is_readonly = true) override; /// NOLINT
    StorageObjectStorageConfigurationPtr clone() override { return std::make_shared<StorageHDFSConfiguration>(*this); }

    void fromNamedCollection(const NamedCollection &) override {}
    void fromAST(ASTs & args, ContextPtr, bool /* with_structure */) override;

    static void addStructureToArgs(ASTs &, const String &, ContextPtr) {}

private:
    String url;
    String path;
    std::vector<String> paths;
};

}

#endif
