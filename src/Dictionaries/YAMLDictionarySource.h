#pragma once

#include <Formats/FormatFactory.h>
#include <Poco/Timestamp.h>
#include "IDictionarySource.h"
#include <Core/Block.h>
#include <Interpreters/Context.h>
#include <Processors/Pipe.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Source for the RegexpTreeDictionary.
class YAMLDictionarySource final : public IDictionarySource
{
public:
    
    YAMLDictionarySource(
            const std::string & filepath_, 
            Block & sample_block_,
            ContextPtr context_,
            bool created_from_ddl);
 
    YAMLDictionarySource(const YAMLDictionarySource & other);

    Pipe loadAll() override;

    Pipe loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdateAll is unsupported for YAMLDictionarySource");
    }

    Pipe loadIds(const std::vector<UInt64> & /*ids*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadIds  is unsupported for YAMLDictionarySource");
    }
    
    Pipe loadKeys(const Columns & /*key_columns*/,const std::vector<size_t> & /*requested_rows*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadKeys is unsupported for YAMLDictionarySource");
    }

    bool isModified() const override
    {
        return getLastModification() != last_modification;
    }

    bool supportsSelectiveLoad() const override { return false; }

    //Not supported for YAMLDictionarySource
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_unique<YAMLDictionarySource>(*this); }

    std::string toString() const override;

private:
    Poco::Timestamp getLastModification() const;

    const std::string filepath;
    Block sample_block;
    ContextPtr context;
    Poco::Timestamp last_modification;
};

}

