#pragma once
#include <DataStreams/IProfilingBlockInputStream.h>

namespace DB 
{

class DictionaryBlockInputStreamBase : public IProfilingBlockInputStream
{
protected:
    Block block;

    DictionaryBlockInputStreamBase() : was_read(false) {}

    String getID() const override;

private:
    bool was_read;

    Block readImpl() override;
    void readPrefixImpl() override { was_read = false; }
    void readSuffixImpl() override { was_read = false; }
};

}