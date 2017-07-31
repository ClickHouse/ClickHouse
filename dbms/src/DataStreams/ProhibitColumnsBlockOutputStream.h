#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Core/NamesAndTypes.h>


namespace DB
{

/// Throws exception on encountering prohibited column in block
class ProhibitColumnsBlockOutputStream : public IBlockOutputStream
{
public:
    ProhibitColumnsBlockOutputStream(const BlockOutputStreamPtr & output, const NamesAndTypesList & columns)
        : output{output}, columns{columns}
    {
    }

private:
    void write(const Block & block) override;

    void flush() override { output->flush(); }

    void writePrefix() override { output->writePrefix(); }
    void writeSuffix() override { output->writeSuffix(); }

    BlockOutputStreamPtr output;
    NamesAndTypesList columns;
};

}
