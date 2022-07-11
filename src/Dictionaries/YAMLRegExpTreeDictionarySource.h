#pragma once

#include <Common/Exception.h>
#include <Common/config.h>

#if USE_YAML_CPP

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Poco/Logger.h>
#include <Poco/Timestamp.h>

#include <Dictionaries/IDictionarySource.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class YAMLRegExpTreeDictionarySource : public IDictionarySource
{
public:
    YAMLRegExpTreeDictionarySource(const std::string & filepath_, Block & sample_block_, ContextPtr context_, bool created_from_ddl);

    YAMLRegExpTreeDictionarySource(const YAMLRegExpTreeDictionarySource & other);

    QueryPipeline loadAll() override;

    QueryPipeline loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for YAMLRegExpTreeDictionarySource");
    }

    QueryPipeline loadIds(const std::vector<UInt64> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadIds is unsupported for YAMLRegExpTreeDictionarySource");
    }

    QueryPipeline loadKeys(const Columns &, const std::vector<size_t> &) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadKeys is unsupported for YAMLRegExpTreeDictionarySource");
    }

    bool supportsSelectiveLoad() const override { return false; }

    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<YAMLRegExpTreeDictionarySource>(*this); }

    bool isModified() const override;

    std::string toString() const override;

private:
    const std::string filepath;

    Block sample_block;
    ContextPtr context;

    Poco::Logger * logger;
    Poco::Timestamp last_modification;

    Poco::Timestamp getLastModification() const;
};

}

#endif
