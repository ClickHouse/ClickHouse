#pragma once

#include <Common/Exception.h>

#include "config.h"

#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <QueryPipeline/QueryPipeline.h>

#include <Poco/Logger.h>
#include <Poco/Timestamp.h>

#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class YAMLRegExpTreeDictionarySource : public IDictionarySource
{
#if USE_YAML_CPP
public:
    YAMLRegExpTreeDictionarySource(const String & filepath_, const DictionaryStructure & dict_struct_, ContextPtr context_, bool created_from_ddl);

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

    String toString() const override;

private:
    const String filepath;
    String key_name;
    const DictionaryStructure structure;

    ContextPtr context;

    LoggerPtr logger;
    Poco::Timestamp last_modification;

    Poco::Timestamp getLastModification() const;
#endif
};

}

