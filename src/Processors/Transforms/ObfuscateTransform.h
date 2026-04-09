#pragma once
#include <optional>
#include <Core/Block.h>
#include <Processors/IAccumulatingTransform.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Common/Obfuscator/Obfuscator.h>

namespace DB
{

class ObfuscateTransform : public IAccumulatingTransform
{
public:
    ObfuscateTransform(
        const Block & header_,
        TemporaryDataOnDiskScopePtr tmp_data_scope_,
        const MarkovModelParameters & params_,
        UInt64 seed_,
        bool keep_original_data_
    );

    String getName() const override { return "Obfuscate"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    Obfuscator obfuscator;

    TemporaryBlockStreamHolder stream_holder;
    std::optional<TemporaryBlockStreamReaderHolder> reader;

    bool keep_original_data;

    bool first_generate = true;
};

}
