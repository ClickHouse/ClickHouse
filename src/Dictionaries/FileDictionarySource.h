#pragma once

#include <Poco/Timestamp.h>
#include "IDictionarySource.h"
#include <Core/Block.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Allows loading dictionaries from a file with given format, does not support "random access"
class FileDictionarySource final : public IDictionarySource
{
public:
    FileDictionarySource(const std::string & filepath_, const std::string & format_,
        Block & sample_block_, ContextPtr context_, bool created_from_ddl);

    FileDictionarySource(const FileDictionarySource & other);

    Pipe loadAll() override;

    Pipe loadUpdatedAll() override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for FileDictionarySource");
    }

    Pipe loadIds(const std::vector<UInt64> & /*ids*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadIds is unsupported for FileDictionarySource");
    }

    Pipe loadKeys(const Columns & /*key_columns*/, const std::vector<size_t> & /*requested_rows*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadKeys is unsupported for FileDictionarySource");
    }

    bool isModified() const override
    {
        // We can't count on that the mtime increases or that it has
        // a particular relation to system time, so just check for strict
        // equality.
        return getLastModification() != last_modification;
    }

    bool supportsSelectiveLoad() const override { return false; }

    ///Not supported for FileDictionarySource
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<FileDictionarySource>(*this); }

    std::string toString() const override;

private:
    Poco::Timestamp getLastModification() const;

    const std::string filepath;
    const std::string format;
    Block sample_block;
    ContextPtr context;
    Poco::Timestamp last_modification;
};

}
