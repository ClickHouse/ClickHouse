#pragma once

#include <Core/Block_fwd.h>
#include <Dictionaries/IDictionarySource.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

/// Allows creating empty dictionary
class NullDictionarySource final : public IDictionarySource
{
public:
    explicit NullDictionarySource(SharedHeader sample_block_);

    NullDictionarySource(const NullDictionarySource & other);

    BlockIO loadAll(ContextMutablePtr) override;

    BlockIO loadUpdatedAll(ContextMutablePtr) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadUpdatedAll is unsupported for NullDictionarySource");
    }

    BlockIO loadIds(ContextMutablePtr, const std::vector<UInt64> & /*ids*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadIds is unsupported for NullDictionarySource");
    }

    BlockIO loadKeys(ContextMutablePtr, const Columns & /*key_columns*/, const std::vector<size_t> & /*requested_rows*/) override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method loadKeys is unsupported for NullDictionarySource");
    }

    bool isModified() const override { return false; }

    bool supportsSelectiveLoad() const override { return false; }

    ///Not supported for NullDictionarySource
    bool hasUpdateField() const override { return false; }

    DictionarySourcePtr clone() const override { return std::make_shared<NullDictionarySource>(*this); }

    std::string toString() const override;

private:
    SharedHeader sample_block;
};

}
